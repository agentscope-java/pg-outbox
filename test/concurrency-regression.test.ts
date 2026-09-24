/**
 * Large-scale regression test for a real correctness bug this library
 * shipped with: the original `claimSQL` used `NOT EXISTS (... status =
 * 'processing')` to keep a key's messages from overlapping, protected by
 * `pg_try_advisory_xact_lock` — a lock released the instant the *claim*
 * transaction committed, not when the claimed message finished processing.
 * Under Postgres `READ COMMITTED`, the `NOT EXISTS` snapshot is taken once
 * per *statement*; if another worker's claim scan started before a sibling
 * committed row 1 of a key as `processing`, the scan could still see the
 * pre-commit state by the time it reached row 2 of that key — even after
 * the sibling had already committed and released its advisory lock — and
 * claim it anyway. It never reproduces at small scale (a handful of rows,
 * a couple of workers): the window only opens when a scan takes long enough
 * for another worker's claim-and-commit to land inside it, which needs a
 * table big enough, and enough concurrent workers, for that overlap to be
 * likely. Hence the scale here rather than a smaller one — this mirrors the
 * sibling Python package's `test_large_scale_concurrent_dispatch_never_overlaps_a_key`,
 * which caught the same bug in `pgoutbox` before this port existed.
 *
 * Skips (loudly, never silently) when no real Postgres is reachable — see
 * test/integration.test.ts's doc comment for how to point it at one.
 */
import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';

import { createOutbox } from '../src/index.ts';
import { PgConnection, UnsupportedAuthError, parseConnectionString } from './helpers/pg-wire.ts';

const CONNECTION_STRING = process.env.DATABASE_URL ?? 'postgres://postgres@127.0.0.1:5432/postgres';
const TABLE = 'pg_outbox_concurrency_regression_test';
const NUM_KEYS = 25;
const MESSAGES_PER_KEY = 20;
const NUM_WORKERS = 6;
const TOTAL = NUM_KEYS * MESSAGES_PER_KEY;

let admin: PgConnection | undefined;
let unavailableReason: string | undefined;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

before(async () => {
  try {
    admin = await PgConnection.connect(parseConnectionString(CONNECTION_STRING), 2000);
  } catch (err) {
    if (err instanceof UnsupportedAuthError) {
      unavailableReason =
        `Postgres at ${CONNECTION_STRING} requires an auth method (likely SCRAM) this ` +
        `zero-dependency test client doesn't implement — see test/helpers/pg-wire.ts.`;
    } else {
      unavailableReason = `No reachable Postgres at ${CONNECTION_STRING} (${(err as Error).message}). ` +
        `Set DATABASE_URL to run the concurrency regression suite for real.`;
    }
    // eslint-disable-next-line no-console
    console.log(`\n  [concurrency-regression] SKIPPING: ${unavailableReason}\n`);
  }
});

after(async () => {
  if (admin) {
    await admin.query(`DROP TABLE IF EXISTS "${TABLE}"`).catch(() => {});
    await admin.close();
  }
});

test(
  '6 workers, 25 keys, 500 messages: zero same-key overlaps, zero ordering violations',
  { timeout: 120_000 },
  async (t) => {
    if (!admin) return t.skip(unavailableReason);

    const setup = createOutbox({ query: (text, params) => admin!.query(text, params), table: TABLE });
    await admin.query(`DROP TABLE IF EXISTS "${TABLE}"`);
    await setup.createSchema();

    for (let k = 0; k < NUM_KEYS; k++) {
      for (let seq = 0; seq < MESSAGES_PER_KEY; seq++) {
        await admin.query('BEGIN');
        await setup.enqueue(admin.query.bind(admin), {
          topic: 't',
          key: `k${k}`,
          payload: { key: k, seq },
        });
        await admin.query('COMMIT');
      }
    }

    const delivered: { id: string; key: string; payload: { key: number; seq: number } }[] = [];
    const active = new Map<string, string>(); // key -> name of the worker currently holding it
    const violations: { key: string; holder: string; intruder: string }[] = [];

    async function worker(name: string): Promise<void> {
      const conn = await PgConnection.connect(parseConnectionString(CONNECTION_STRING), 2000);
      try {
        const outbox = createOutbox({ query: (text, params) => conn.query(text, params), table: TABLE });
        for (let round = 0; round < 200; round++) {
          const result = await outbox.processBatch(
            async (msg) => {
              if (active.has(msg.key)) {
                violations.push({ key: msg.key, holder: active.get(msg.key)!, intruder: name });
              }
              active.set(msg.key, name);
              await sleep(1 + Math.random() * 3); // simulated publish latency — widens the race window
              if (active.get(msg.key) === name) active.delete(msg.key);
              delivered.push({ id: msg.id, key: msg.key, payload: msg.payload as { key: number; seq: number } });
            },
            { batchSize: 5, workerId: name },
          );
          if (result.claimed === 0) {
            if (delivered.length >= TOTAL) break;
            await sleep(2);
          }
        }
      } finally {
        await conn.close();
      }
    }

    await Promise.all(Array.from({ length: NUM_WORKERS }, (_, i) => worker(`W${i}`)));

    assert.deepEqual(violations, [], `same-key overlap across workers: ${JSON.stringify(violations.slice(0, 5))}`);
    assert.equal(delivered.length, TOTAL, `delivered ${delivered.length}/${TOTAL}`);
    const ids = delivered.map((m) => m.id);
    assert.equal(new Set(ids).size, ids.length, 'no duplicate deliveries');

    const byKey = new Map<number, number[]>();
    for (const m of delivered) {
      const arr = byKey.get(m.payload.key) ?? [];
      arr.push(m.payload.seq);
      byKey.set(m.payload.key, arr);
    }
    const orderingViolations: number[] = [];
    const expected = Array.from({ length: MESSAGES_PER_KEY }, (_, i) => i);
    for (const [k, seqs] of byKey) {
      if (JSON.stringify(seqs) !== JSON.stringify(expected)) orderingViolations.push(k);
    }
    assert.deepEqual(orderingViolations, [], `keys delivered out of order: ${JSON.stringify(orderingViolations)}`);
  },
);
