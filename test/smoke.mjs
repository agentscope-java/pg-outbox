/**
 * Plain-JS checks against the built package, so they run on every Node version
 * in `engines` — the TypeScript suite needs type stripping and only runs on 24+.
 *
 *   npm run build && node --test test/smoke.mjs
 */
import assert from 'node:assert/strict';
import test from 'node:test';

const { createOutbox, InvalidIdentifierError } = await import('../dist/index.js');

/** A tiny in-memory stand-in for the `query` contract, self-contained for smoke purposes. */
function makeDb() {
  let rows = [];
  let nextId = 1;
  const notifications = [];

  const query = async (text, params = []) => {
    const tag = text.split('\n', 1)[0].trim();
    const now = Date.now();
    if (tag === '-- pg-outbox:schema') return { rows: [] };
    if (tag === '-- pg-outbox:insert') {
      const [key, topic, payloadJson, maxAttempts] = params;
      const row = {
        id: nextId++,
        key,
        topic,
        payload: JSON.parse(payloadJson),
        status: 'pending',
        attempts: 0,
        max_attempts: Number(maxAttempts),
        available_at: now,
        last_error: null,
        created_at: now,
      };
      rows.push(row);
      return { rows: [{ id: row.id }] };
    }
    if (tag === '-- pg-outbox:notify') {
      notifications.push({ channel: params[0], payload: params[1] });
      return { rows: [] };
    }
    if (tag === '-- pg-outbox:claim') {
      const [batchSize, workerId] = params;
      const processingKeys = new Set(rows.filter((r) => r.status === 'processing').map((r) => r.key));
      const headPerKey = new Map();
      for (const row of rows) {
        if (row.status !== 'pending' || row.available_at > now || processingKeys.has(row.key)) continue;
        const current = headPerKey.get(row.key);
        if (!current || row.id < current.id) headPerKey.set(row.key, row);
      }
      const claimed = [...headPerKey.values()].sort((a, b) => a.id - b.id).slice(0, Number(batchSize));
      for (const row of claimed) {
        row.status = 'processing';
        row.attempts += 1;
        row.locked_by = workerId;
      }
      return {
        rows: claimed.map((r) => ({
          id: r.id,
          key: r.key,
          topic: r.topic,
          payload: r.payload,
          attempts: r.attempts,
          max_attempts: r.max_attempts,
          created_at: new Date(r.created_at),
        })),
      };
    }
    if (tag === '-- pg-outbox:complete') {
      rows = rows.filter((r) => String(r.id) !== String(params[0]));
      return { rows: [] };
    }
    if (tag === '-- pg-outbox:retry') {
      const row = rows.find((r) => String(r.id) === String(params[0]));
      if (row) {
        row.status = 'pending';
        row.available_at = Date.now() + Number(params[1]);
        row.last_error = params[2];
      }
      return { rows: [] };
    }
    if (tag === '-- pg-outbox:dead-letter') {
      const row = rows.find((r) => String(r.id) === String(params[0]));
      if (row) {
        row.status = 'dead_letter';
        row.last_error = params[1];
      }
      return { rows: [] };
    }
    throw new Error(`unrecognized statement: ${tag}`);
  };

  return { query, notifications, get rows() { return rows; } };
}

test('enqueue + processBatch delivers a message and removes it', async () => {
  const db = makeDb();
  const outbox = createOutbox({ query: db.query });
  await outbox.enqueue(db.query, { topic: 'order.created', payload: { id: 1 } });

  const delivered = [];
  const result = await outbox.processBatch(async (msg) => {
    delivered.push(msg.payload);
  });

  assert.equal(result.succeeded, 1);
  assert.deepEqual(delivered, [{ id: 1 }]);
  assert.equal(db.rows.length, 0);
});

test('a message that always fails is dead-lettered after maxAttempts', async () => {
  const db = makeDb();
  const outbox = createOutbox({ query: db.query, backoff: () => 0, maxAttempts: 2 });
  await outbox.enqueue(db.query, { topic: 't', payload: {} });

  const fail = async () => {
    throw new Error('nope');
  };
  await outbox.processBatch(fail);
  const second = await outbox.processBatch(fail);

  assert.equal(second.deadLettered, 1);
  assert.equal(db.rows[0].status, 'dead_letter');
});

test('rejects an unsafe table identifier', () => {
  const db = makeDb();
  assert.throws(() => createOutbox({ query: db.query, table: 'bad; drop table x' }), InvalidIdentifierError);
});

test('exposes the raw schema SQL', () => {
  const db = makeDb();
  const outbox = createOutbox({ query: db.query, table: 'my_outbox' });
  assert.match(outbox.schemaSQL, /CREATE TABLE IF NOT EXISTS "my_outbox"/);
});
