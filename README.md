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

`pg`'s `pool.query` and `client.query` already have this shape. So does a
transaction object from most ORMs' raw-query escape hatch. `postgres.js` uses
tagged templates instead, so wrap it:

```ts
import postgres from 'postgres';
const sql = postgres(process.env.DATABASE_URL);
const query: Query = (text, params = []) => sql.unsafe(text, params);
```

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
WITH locked AS (
  SELECT t.id, t.key
  FROM outbox_messages t
  WHERE t.status = 'pending'
    AND t.available_at <= now()
    AND NOT EXISTS (
      SELECT 1 FROM outbox_messages p WHERE p.key = t.key AND p.status = 'processing'
    )
    AND pg_try_advisory_xact_lock(hashtext($3), hashtext(t.key))
  ORDER BY t.id
  FOR UPDATE SKIP LOCKED
),
head AS (
  SELECT DISTINCT ON (key) id FROM locked ORDER BY key, id LIMIT $1
)
UPDATE outbox_messages m SET status = 'processing', attempts = m.attempts + 1, ...
FROM head WHERE m.id = head.id
RETURNING m.*;
```

Three mechanisms, doing three different jobs:

- **`FOR UPDATE SKIP LOCKED`** is what lets several dispatchers run against the
  same table concurrently without double-delivering or blocking each other. If
  another transaction already has a candidate row locked, this query skips it
  instead of waiting behind it — so N dispatchers polling the same table
  degrade to "each gets a share of the work," not "each waits for the others."
- **`NOT EXISTS (... status = 'processing')`** is what makes per-key ordering
  hold for the *entire* time a message is in flight, not just the instant it's
  claimed: once a message is marked `processing`, every other message with the
  same `key` is invisible to every claim query — including this one, later —
  until that message resolves (succeeds, is retried, or is dead-lettered).
- **`pg_try_advisory_xact_lock(hashtext($3), hashtext(t.key))`** closes a
  narrower race: two dispatchers running this query at the *same instant*
  against a key whose first message isn't committed as `processing` yet. The
  advisory lock is per-key, held for the statement's duration, and excludes
  every row of that key (not just the specific one being raced over) from the
  loser's candidate set — so the loser's query can't fall through to a
  *later* message of that key and hand it out of order.

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

`test/integration.test.ts` runs against a real Postgres when one is reachable
(`DATABASE_URL`, or `postgres://postgres@127.0.0.1:5432/postgres` by default)
and **skips explicitly, with the reason printed**, when it isn't:

```
[integration] SKIPPING: No reachable Postgres at postgres://postgres@127.0.0.1:5432/postgres
(connect ECONNREFUSED 127.0.0.1:5432). Set DATABASE_URL to run the integration suite for real.
```

It connects with a small hand-rolled wire-protocol client
(`test/helpers/pg-wire.ts`) rather than a driver, since devDependencies are
limited to `typescript` and `@types/node`; it supports trust/cleartext/MD5
auth, not SCRAM, and says so if it hits it.

## License

MIT
