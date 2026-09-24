# pg-outbox

Transactional outbox for Postgres. Zero dependencies, and no driver of its own —
you bring a `query` function.

```
6 concurrent workers, 500 messages across 25 keys

delivered              500 / 500
duplicate deliveries   0
concurrency violations 0  (two workers holding the same key at once)
ordering violations    0 / 25 keys
elapsed                43ms
```

That's `node examples/concurrency-demo.ts` — six dispatchers hammering the same
table concurrently, and the claim query (`FOR UPDATE SKIP LOCKED` plus a
per-key exclusivity check) making sure nothing is delivered twice and nothing
for the same key jumps the queue.

## The problem

Writing to your database and publishing to a broker cannot be made atomic.
Publish first, and a rolled-back transaction means you announced something
that never happened. Write first, and a publish failure means the rest of the
system never hears about it — there's no way to `COMMIT` a Kafka topic and a
Postgres row together.

The outbox pattern sidesteps this: write the message as a normal row *in the
same transaction* as your business data, so it either commits with that data
or not at all. A separate dispatcher then reads unpublished rows and delivers
them afterwards, retrying until it succeeds. You trade the (impossible)
atomic-publish guarantee for **the message will be published at least once,
eventually** — which is what's actually achievable, and usually what you want.

```ts
import { createOutbox } from 'pg-outbox';

const outbox = createOutbox({ query }); // `query` here is for dispatch, not enqueue — see below

const client = await pool.connect();
await client.query('BEGIN');
await createOrder(client, order); // your own business-data write
await outbox.enqueue(client.query.bind(client), { topic: 'order.created', payload: { id: order.id } });
await client.query('COMMIT');
client.release();

// separately, a dispatcher process
await outbox.dispatch(async (msg) => { await publish(msg); });
```

## Why not just publish after commit?

Because "after commit" is a window where your process can die, the broker can
be unreachable, or the publish call can throw for a reason that has nothing to
do with whether the write succeeded — and when it does, there is no way back
into the transaction to undo the write, and no way to make the publish retry
transactional either. The row *is* the durable record of intent to publish;
as long as it's in the same transaction as the write, "did we publish" reduces
to "is there still a row," which you can always answer, and retry against,
after a crash.

## Why not logical replication (`pg-transactional-outbox` et al.)?

[`pg-transactional-outbox`](https://github.com/Zehelein/pg-transactional-outbox)
and similar libraries capture outbox rows via Postgres's logical replication
(`pgoutput`, a replication slot) instead of polling. That's a legitimate, more
efficient design — no polling latency, no wasted queries against an empty
table — and if you can set up a replication slot and run a `WAL`-consuming
process, **it's a reasonable choice, arguably a better one for high-throughput
systems.**

Use `pg-outbox` instead when:

- You don't control the database configuration enough to create a replication
  slot (managed Postgres with logical replication disabled, a shared instance,
  a security policy that says no) or don't want the operational surface area
  of a WAL consumer.
- You want the dispatcher to be "just a process that polls a table," debuggable
  with `SELECT * FROM outbox_messages` and killable with `Ctrl-C`.
  You want zero dependencies and no coupling to a specific database driver.
- Millisecond-scale delivery latency doesn't matter as much as operational
  simplicity — `pg-outbox` uses `LISTEN`/`NOTIFY` as a fast path (see below),
  but it's still fundamentally a poller with a low-latency nudge, not a WAL
  stream.

## Install

```sh
npm install pg-outbox
```

Node >= 20.6, Postgres >= 9.5 (`SKIP LOCKED` requires it). No runtime
dependencies.

## Use

`createOutbox` takes one required thing: a `query` function.

```ts
export type Query = (text: string, params?: unknown[]) => Promise<{ rows: any[] }>;
```

`pg`'s `client.query` already has this shape. So does a transaction object
from most ORMs' raw-query escape hatch. `postgres.js` uses tagged templates
instead, so wrap it:

```ts
import postgres from 'postgres';
const sql = postgres(process.env.DATABASE_URL);
const query: Query = (text, params = []) => sql.unsafe(text, params);
```

**The `query` you pass to `createOutbox` must be bound to one stable
connection/session — not a `pg.Pool`, whose `.query` hands a different
physical connection to every call.** `processBatch` issues a literal
`BEGIN`, the claim, every claimed message's resolution, and `COMMIT` as
several separate calls to this function, and that only forms one real
transaction if every call lands on the same session — see "How claiming
works" below for why that transaction exists at all. Pass `pool.connect()`'s
checked-out `client.query` (or an equivalent single-connection wrapper), not
`pool.query`.

**The failure mode if you get this wrong is silent, not an error.** A pool
will happily run `BEGIN` on connection A and the claim on connection B — no
exception, no warning, just two unrelated autocommitted statements and a row
lock that never actually spans the batch. Per-key exclusivity quietly stops
holding under load; nothing in the code path will tell you, which is exactly
the shape of bug this library's own claim query shipped with once already
(see "How claiming works"). If you're not sure whether your `query` is
pool-backed, it probably is — check.

### Enqueue — inside your transaction

```ts
import { createOutbox } from 'pg-outbox';

const outbox = createOutbox({ query }); // this `query` is for dispatch/admin, not enqueue

const client = await pool.connect();
try {
  await client.query('BEGIN');
  await client.query('INSERT INTO orders (id, total) VALUES ($1, $2)', [order.id, order.total]);
  await outbox.enqueue(client.query.bind(client), {
    topic: 'order.created',
    payload: { id: order.id, total: order.total },
    key: String(order.id), // same-order events stay ordered; defaults to `topic`
  });
  await client.query('COMMIT');
} catch (err) {
  await client.query('ROLLBACK');
  throw err;
} finally {
  client.release();
}
```

**`enqueue`'s first argument must be the same `query` your business-data write
goes through.** There's no way to make this happen for you — a `Pool`'s
`query` runs on whatever connection is free, not the one holding your open
transaction — so the API asks for it explicitly every call instead of hiding
a pool reference that would open a second, unrelated connection and quietly
break the whole guarantee. If `outbox.enqueue(pool.query, ...)` compiles and
runs, that's the bug: it happened outside the transaction, and a rollback
after it won't undo it.

### Dispatch — a separate process

```ts
await outbox.dispatch(async (msg) => {
  await publish(msg.topic, msg.payload); // msg.id is here too, for dedup — see below
});
```

`dispatch` runs until you stop it (`signal: AbortController#signal`), claiming
and delivering messages as they become available. For environments where a
long-lived process isn't an option (a cron job, a serverless function), call
`outbox.processBatch(handler)` directly — it claims and processes one batch,
then returns.

## How claiming works

This is the load-bearing SQL (`src/sql.ts`'s `claimSQL`, abbreviated):

```sql
WITH candidates AS (
  SELECT DISTINCT ON (key) id
  FROM outbox_messages
  WHERE status = 'pending' AND available_at <= now()
  ORDER BY key, id
),
locked AS (
  SELECT m.id
  FROM outbox_messages m
  JOIN candidates c ON c.id = m.id
  ORDER BY m.id
  LIMIT $1
  FOR UPDATE OF m SKIP LOCKED
)
UPDATE outbox_messages m SET status = 'processing', attempts = m.attempts + 1, ...
FROM locked WHERE m.id = locked.id
RETURNING m.*;
```

`processBatch` runs this as the first statement of an explicit transaction
(`BEGIN` ... claim ... handle each message ... complete/retry/dead-letter
each one ... `COMMIT`) that stays open until every claimed message in the
batch is resolved. That's not incidental — it's the fix for a real
correctness bug an earlier version of this query had, caught only because
this package's own regression test (`test/concurrency-regression.test.ts`)
runs against a real Postgres under real concurrent load rather than relying
on the in-memory fake or small-scale checks alone. Worth explaining both
halves:

**What `candidates` does.** For each key, it picks exactly one row — the
oldest `pending` one — using a normal MVCC-snapshot read. This is where a
key's "next message" gets decided, and critically, it's decided *before*
anything tries to lock anything.

**What `locked` does.** It tries to lock *only those chosen rows*, live, via
`FOR UPDATE OF m SKIP LOCKED`. If a key's chosen row is currently locked —
another transaction claimed it and hasn't resolved it yet — that row is
dropped. Because `candidates` already committed to exactly one row per key
before any locking happened, there's no second row for `locked` to fall back
to: a key whose head message is in flight contributes *nothing* to this
batch. And because holding the whole claim-through-resolution cycle inside
one transaction keeps that row's lock held for the message's entire
in-flight duration (not just the instant it's claimed), this exclusion holds
for as long as the message takes to process, however long that is.

**The bug this replaced.** An earlier version used `NOT EXISTS (SELECT 1
... WHERE status = 'processing')` to detect in-flight siblings, protected by
an advisory lock (`pg_try_advisory_xact_lock`) that was released the moment
the *claim* transaction committed — which, since claiming was its own short
autocommit statement, was almost immediately, long before the handler
actually finished. That combination is subtly broken: Postgres's
`READ COMMITTED` isolation takes one snapshot per *statement*, at the
statement's start, not per row as it scans. If worker B's claim statement
started before worker A committed row 1 as `processing`, B's `NOT EXISTS`
check — evaluated later, possibly *after* A had already committed and
released its advisory lock — still saw the pre-commit state and happily
claimed row 2 of the same key. The window only opens when a scan takes long
enough for another transaction to fully claim-and-commit inside it, which is
exactly why it never showed up in quick, few-row tests and only appeared
under real concurrent load against real Postgres: a regression test with 6
workers, 25 keys and 500 messages (`test/concurrency-regression.test.ts`)
reproduced the old query failing with **8 same-key concurrency violations**
on one run and **2 on a second run** (violations are counted across the
whole 500-message run, not stopped at the first one) — the fixed query
(above) passed the identical test **five consecutive times with zero
violations of either kind (same-key overlap or ordering)**. A
`FOR UPDATE ... SKIP LOCKED` check is never snapshot-stale like that, which
is why the fix routes through it instead of a status column.

**What this means for you:** the `query` this library is constructed with
must be bound to one stable connection/session for dispatch — see the
prominent warning under "Use" above. A connection pool that hands a
different physical connection to each call will run `BEGIN` and `COMMIT` on
different sessions and silently lose this guarantee.

## Ordering

Messages with the same `key` (default: `topic`) are delivered one at a time,
in the order they were enqueued, for as long as their `available_at` (i.e.
including the delay between retries) puts them in claim order. Messages with
**different** keys have no ordering relationship at all — they can be claimed
by different workers and delivered in parallel, which `processBatch` does
deliberately (see "concurrent dispatch" in the tests): a batch never contains
two messages of the same key, so processing the whole batch concurrently can
never reorder anything.

If you don't need cross-message ordering, give unrelated messages distinct
keys (e.g. per-entity) so they don't serialize behind each other for no
reason. If you need every message in a topic ordered, leave `key` unset.

## At-least-once, never exactly-once

A crash between your handler completing and `pg-outbox` deleting the row means
the same message is claimed again on restart. **Your handler must be
idempotent** — safe to run twice with the same message. `msg.id` is a stable
identifier included on every message specifically so you can deduplicate on
it (e.g. a `processed_message_ids` table, an idempotency key your broker or
downstream API already supports).

Because `processBatch` holds one open transaction across the whole batch
(see "How claiming works" above), a worker process dying mid-batch rolls the
*entire* batch back automatically — every claim in it reverts to `pending`,
with nothing stuck as `processing` forever and nothing left for a reaper to
clean up. The trade-off: if message 1 of a 5-message batch already
succeeded (published, in your handler's eyes) when the process died on
message 3, message 1 redelivers too, since its completion was never
committed either. Still at-least-once, never zero-delivery — just a
batch-sized blast radius for that specific redelivery, not a single-message
one. Smaller `batchSize` values shrink that radius.

## Retries, backoff, and dead-lettering

A handler that throws causes the message to be requeued with `available_at`
pushed out by `backoff(attempts)` (default: `500ms * 2^(attempts-1)`, capped
at 30s), up to `maxAttempts` (default 5, overridable per-message). Once
exhausted, the message is moved to `status = 'dead_letter'` and stays in the
table — it's never picked up again, and never silently deleted — so you can
inspect, fix, and manually requeue it (`UPDATE ... SET status = 'pending',
attempts = 0 WHERE id = ...`).

```ts
await outbox.dispatch(handler, {
  onError: (err, msg) => logger.warn({ err, messageId: msg.id, attempt: msg.attempts }, 'delivery failed'),
});
```

## `LISTEN`/`NOTIFY`, with polling as the fallback

`dispatch` polls (`pollInterval`, default 1000ms) by design — `NOTIFY` is not
durable. A notification sent while nobody is `LISTEN`ing (dispatcher mid-crash,
mid-restart, or simply not running yet) is gone forever; a poll loop is what
actually guarantees a message enqueued at 2am gets delivered even if nothing
was listening at 2am. `NOTIFY` is purely a latency optimization on top of that
guarantee, never a replacement for it.

There's also no driver-agnostic way to *receive* `NOTIFY` — unlike running a
query, it's an asynchronous push over a held connection, with a completely
different API in `pg` (`client.on('notification', ...)`) than in `postgres.js`
or anything else. So it's an optional hook you wire up for your own driver:

```ts
import { Client } from 'pg';

const listenClient = new Client();
await listenClient.connect();

const outbox = createOutbox({
  query,
  listen: async (channel, onNotify) => {
    await listenClient.query(`LISTEN ${channel}`);
    listenClient.on('notification', (msg) => {
      if (msg.channel === channel) onNotify();
    });
    return async () => {
      await listenClient.query(`UNLISTEN ${channel}`);
    };
  },
});
```

Without `listen`, `dispatch` still delivers correctly — it just finds out
about new messages within `pollInterval` instead of immediately.

## Schema

```ts
await outbox.createSchema(); // runs outbox.schemaSQL, idempotent (IF NOT EXISTS)
console.log(outbox.schemaSQL); // print it and put it in your own migration instead
```

`createSchema` is a convenience for prototyping; most people should copy
`outbox.schemaSQL`'s output into a real migration so schema changes go through
the same review and rollout process as the rest of the database. Nothing in
this library applies schema changes behind your back beyond what you
explicitly call.

## Options

`createOutbox(options)`:

| Option | Default | |
| --- | --- | --- |
| `query` | — | Required. Runs SQL, returns `{ rows }`. Used for dispatch/admin, not `enqueue`. |
| `table` | `outbox_messages` | Must be a bare identifier (`/^[a-zA-Z_][a-zA-Z0-9_]*$/`). |
| `channel` | `pg_outbox` | `NOTIFY`/`LISTEN` channel. Same identifier rule. |
| `maxAttempts` | `5` | Default attempt budget; overridable per message. |
| `backoff` | capped exponential | `(attempt: number) => number`, delay in ms before the next try. |
| `listen` | — | Optional `LISTEN` fast path — see above. |

`outbox.enqueue(tx, message)`:

| Field | Default | |
| --- | --- | --- |
| `topic` | — | Required. |
| `payload` | — | Required. JSON-serializable; stored as `jsonb`. |
| `key` | `topic` | Ordering key — see "Ordering" above. |
| `maxAttempts` | outbox-level default | Per-message override. |

`outbox.dispatch(handler, options)` / `outbox.processBatch(handler, options)`:

| Option | Default | |
| --- | --- | --- |
| `batchSize` | `10` | Messages claimed per pass; at most one per distinct key. |
| `workerId` | hostname + pid + random | Recorded on claimed rows (`locked_by`), for logs and manual inspection. |
| `onError` | — | `(error, message) => void`, called on every handler failure. |
| `pollInterval` | `1000` (`dispatch` only) | Fallback poll interval in ms. |
| `signal` | — | (`dispatch` only) `AbortSignal` that stops the loop. |

## What it does not do

- **Not exactly-once delivery.** See "At-least-once" above — this is a
  property of the outbox pattern generally, not something a smarter query
  could fix.
- **Not logical-replication-based capture.** See "Why not logical
  replication" above — this is a polling design with a `NOTIFY` fast path,
  not a WAL consumer.
- **No automatic cleanup of dead-lettered messages.** They stay until you deal
  with them; add your own retention job if you want them purged.
- **No cross-database or cross-shard delivery ordering.** Ordering is a
  property of one table, claimed against by workers pointed at that table.
- **No built-in metrics or tracing.** `onError` and the `ProcessResult` counts
  `processBatch` returns are the hooks; wire them into whatever you already
  use.

## Develop

Tests are TypeScript run directly by Node's test runner — no build, no install:

```sh
node --test "test/*.test.ts"   # full suite, needs node 24+ for type stripping
node examples/concurrency-demo.ts

npm run build && npm run test:dist   # what CI runs against node 20 and 22
```

`test/pg-outbox.test.ts` runs entirely against `test/helpers/memory-db.ts`, an
in-memory implementation of the `Query` contract that enforces the same claim
invariants the real SQL does (see that file's doc comment) — no database
needed for full coverage of dispatch, retries, backoff, dead-lettering, and
concurrent-worker ordering.

`test/integration.test.ts` and `test/concurrency-regression.test.ts` run
against a real Postgres when one is reachable (`DATABASE_URL`, or
`postgres://postgres@127.0.0.1:5432/postgres` by default) and **skip
explicitly, with the reason printed**, when it isn't:

```
[integration] SKIPPING: No reachable Postgres at postgres://postgres@127.0.0.1:5432/postgres
(connect ECONNREFUSED 127.0.0.1:5432). Set DATABASE_URL to run the integration suite for real.
```

`test/concurrency-regression.test.ts` is the large-scale regression test for
the claim-query bug described in "How claiming works": 6 concurrent workers,
25 keys, 500 messages, asserting zero same-key overlaps and zero ordering
violations. It genuinely needs a real Postgres and real concurrent
connections — the bug it guards against never reproduced against the
in-memory fake or at small scale.

It connects with a small hand-rolled wire-protocol client
(`test/helpers/pg-wire.ts`) rather than a driver, since devDependencies are
limited to `typescript` and `@types/node`; it supports trust/cleartext/MD5
auth, not SCRAM, and says so if it hits it.

## License

MIT
