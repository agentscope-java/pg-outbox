/**
 * Runs against a real Postgres when one is reachable, and skips — loudly,
 * never silently — when it isn't. Point `DATABASE_URL` at a scratch database
 * to run these for real; otherwise every test below reports itself skipped
 * with the reason, which `node --test` prints in the summary.
 */
import assert from 'node:assert/strict';
import { after, before, describe, test } from 'node:test';

import { createOutbox } from '../src/index.ts';
import { PgConnection, UnsupportedAuthError, parseConnectionString } from './helpers/pg-wire.ts';

const CONNECTION_STRING = process.env.DATABASE_URL ?? 'postgres://postgres@127.0.0.1:5432/postgres';
const TABLE = 'pg_outbox_integration_test';

let admin: PgConnection | undefined;
let unavailableReason: string | undefined;

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
        `Set DATABASE_URL to run the integration suite for real.`;
    }
    // eslint-disable-next-line no-console
    console.log(`\n  [integration] SKIPPING: ${unavailableReason}\n`);
  }
});

after(async () => {
  if (admin) {
    await admin.query(`DROP TABLE IF EXISTS "${TABLE}"`).catch(() => {});
    await admin.close();
  }
});

describe('pg-outbox against real Postgres', () => {
  test('creates the schema', async (t) => {
    if (!admin) return t.skip(unavailableReason);
    const outbox = createOutbox({ query: (text, params) => admin!.query(text, params), table: TABLE });
    await outbox.createSchema();
    const { rows } = await admin.query(
      `SELECT 1 FROM information_schema.tables WHERE table_name = $1`,
      [TABLE],
    );
    assert.equal(rows.length, 1);
  });

  test('enqueue only commits if the caller\'s transaction does', async (t) => {
    if (!admin) return t.skip(unavailableReason);
    const outbox = createOutbox({ query: (text, params) => admin!.query(text, params), table: TABLE });
    await outbox.createSchema();

    await admin.query('BEGIN');
    await outbox.enqueue(admin.query.bind(admin), { topic: 'rollback.me', payload: { n: 1 } });
    await admin.query('ROLLBACK');

    const { rows } = await admin.query(`SELECT count(*)::int AS n FROM "${TABLE}" WHERE topic = $1`, [
      'rollback.me',
    ]);
    assert.equal(rows[0].n, 0);
  });

  test('a committed enqueue is delivered by dispatch, and removed on success', async (t) => {
    if (!admin) return t.skip(unavailableReason);
    const outbox = createOutbox({ query: (text, params) => admin!.query(text, params), table: TABLE });
    await outbox.createSchema();

    await admin.query('BEGIN');
    await outbox.enqueue(admin.query.bind(admin), { topic: 'order.created', payload: { id: 42 } });
    await admin.query('COMMIT');

    const delivered: unknown[] = [];
    const result = await outbox.processBatch(async (msg) => {
      delivered.push(msg.payload);
    });

    assert.equal(result.succeeded, 1);
    assert.deepEqual(delivered, [{ id: 42 }]);

    const { rows } = await admin.query(`SELECT count(*)::int AS n FROM "${TABLE}" WHERE topic = $1`, [
      'order.created',
    ]);
    assert.equal(rows[0].n, 0);
  });

  test('FOR UPDATE SKIP LOCKED: two concurrent claimers never return the same row', async (t) => {
    if (!admin) return t.skip(unavailableReason);
    const outbox = createOutbox({ query: (text, params) => admin!.query(text, params), table: TABLE });
    await outbox.createSchema();

    for (let i = 0; i < 10; i++) {
      await admin.query('BEGIN');
      await outbox.enqueue(admin.query.bind(admin), { topic: 'fanout', key: `key-${i}`, payload: { i } });
      await admin.query('COMMIT');
    }

    const second = await PgConnection.connect(parseConnectionString(CONNECTION_STRING), 2000);
    try {
      const outboxB = createOutbox({ query: (text, params) => second.query(text, params), table: TABLE });
      const [a, b] = await Promise.all([
        outbox.processBatch(async () => {}, { batchSize: 10 }),
        outboxB.processBatch(async () => {}, { batchSize: 10 }),
      ]);
      assert.equal(a.claimed + b.claimed, 10, 'every message claimed exactly once across both workers');
    } finally {
      await second.close();
    }
  });
});
