import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

import { createOutbox, InvalidIdentifierError, type Query } from '../src/index.ts';
import { MemoryDb } from './helpers/memory-db.ts';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** A deferred promise, for orchestrating handler timing precisely in tests. */
function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

describe('enqueue', () => {
  test('writes through the given tx, not the outbox\'s own query', async () => {
    const db = new MemoryDb();
    const poisoned: Query = async () => {
      throw new Error('enqueue must not use the default query — it would defeat the whole point');
    };
    const outbox = createOutbox({ query: poisoned, table: 'outbox_messages' });

    const { id } = await outbox.enqueue(db.query, { topic: 'order.created', payload: { id: 1 } });

    assert.equal(typeof id, 'string');
    assert.equal(db.rows.length, 1);
    assert.equal(db.rows[0]!.topic, 'order.created');
  });

  test('defaults the ordering key to the topic', async () => {
    const db = new MemoryDb();
    const outbox = createOutbox({ query: db.query });
    await outbox.enqueue(db.query, { topic: 'order.created', payload: {} });
    assert.equal(db.rows[0]!.key, 'order.created');
  });

  test('notifies the configured channel', async () => {
    const db = new MemoryDb();
    const outbox = createOutbox({ query: db.query, channel: 'my_channel' });
    await outbox.enqueue(db.query, { topic: 't', payload: {} });
    assert.deepEqual(db.notifications, [{ channel: 'my_channel', payload: 'outbox_messages' }]);
  });
});

describe('identifiers', () => {
  test('rejects a table name that is not a bare identifier', () => {
    const db = new MemoryDb();
    assert.throws(() => createOutbox({ query: db.query, table: 'bad name; drop table x' }), InvalidIdentifierError);
  });

  test('rejects an unsafe channel name', () => {
    const db = new MemoryDb();
    assert.throws(() => createOutbox({ query: db.query, channel: '1-not-an-identifier' }), InvalidIdentifierError);
  });

  test('exposes raw schema SQL naming the configured table', () => {
    const db = new MemoryDb();
    const outbox = createOutbox({ query: db.query, table: 'my_outbox' });
    assert.match(outbox.schemaSQL, /CREATE TABLE IF NOT EXISTS "my_outbox"/);
    assert.match(outbox.schemaSQL, /payload JSONB NOT NULL/);
  });
});

describe('processBatch', () => {
  test('delivers a message and removes it on success', async () => {
    const db = new MemoryDb();
    const outbox = createOutbox({ query: db.query });
    await outbox.enqueue(db.query, { topic: 'order.created', payload: { id: 7 } });

    const delivered: unknown[] = [];
    const result = await outbox.processBatch(async (msg) => {
      delivered.push(msg.payload);
    });

    assert.equal(result.claimed, 1);
    assert.equal(result.succeeded, 1);
    assert.deepEqual(delivered, [{ id: 7 }]);
    assert.equal(db.rows.length, 0, 'delivered messages are removed');
  });

  test('message carries a stable id, attempt count, and creation time', async () => {
    const db = new MemoryDb();
    const outbox = createOutbox({ query: db.query });
    await outbox.enqueue(db.query, { topic: 't', payload: { a: 1 } });

    let seen: any;
    await outbox.processBatch(async (msg) => {
      seen = msg;
    });

    assert.equal(typeof seen.id, 'string');
    assert.equal(seen.attempts, 1);
    assert.equal(seen.maxAttempts, 5);
    assert.ok(seen.createdAt instanceof Date);
  });

  test('claims at most one message per distinct key, oldest first', async () => {
    const db = new MemoryDb();
    const outbox = createOutbox({ query: db.query });
    await outbox.enqueue(db.query, { topic: 't', key: 'k1', payload: 'k1-a' });
    await outbox.enqueue(db.query, { topic: 't', key: 'k1', payload: 'k1-b' });
    await outbox.enqueue(db.query, { topic: 't', key: 'k2', payload: 'k2-a' });

    const delivered: unknown[] = [];
    const result = await outbox.processBatch(async (msg) => {
      delivered.push(msg.payload);
    });

    assert.equal(result.claimed, 2, 'one per key: k1-a and k2-a, not k1-b yet');
    assert.deepEqual(new Set(delivered), new Set(['k1-a', 'k2-a']));
  });

  test('does not start a key\'s next message until the current one resolves', async () => {
    const db = new MemoryDb();
    const outbox = createOutbox({ query: db.query });
    await outbox.enqueue(db.query, { topic: 't', key: 'k', payload: 'first' });
    await outbox.enqueue(db.query, { topic: 't', key: 'k', payload: 'second' });

    const hold = deferred();
    const firstBatch = outbox.processBatch(async (msg) => {
      assert.equal(msg.payload, 'first');
      await hold.promise;
    });

    // A concurrent claim attempt while "first" is still in flight must not see "second".
    await sleep(5);
    const concurrent = await outbox.processBatch(async () => {});
    assert.equal(concurrent.claimed, 0, 'second is blocked behind first, still in flight');

    hold.resolve();
    await firstBatch;

    const after = await outbox.processBatch(async (msg) => {
      assert.equal(msg.payload, 'second');
    });
    assert.equal(after.claimed, 1);
  });

  test('processes independent keys concurrently within one batch', async () => {
    const db = new MemoryDb();
    const outbox = createOutbox({ query: db.query });
    await outbox.enqueue(db.query, { topic: 't', key: 'a', payload: 'a' });
    await outbox.enqueue(db.query, { topic: 't', key: 'b', payload: 'b' });

    const aStarted = deferred();
    const bStarted = deferred();
    let aRunningWhenBStarted = false;

    await outbox.processBatch(async (msg) => {
      if (msg.key === 'a') {
        aStarted.resolve();
        await bStarted.promise;
        aRunningWhenBStarted = true;
      } else {
        await aStarted.promise;
        bStarted.resolve();
      }
    });

    assert.ok(aRunningWhenBStarted, 'both handlers overlapped instead of running strictly sequentially');
  });

  test('retries a failed message with backoff, then succeeds', async () => {
    const db = new MemoryDb();
    const outbox = createOutbox({ query: db.query, backoff: () => 40 });
    await outbox.enqueue(db.query, { topic: 't', payload: { n: 1 } });

    let attempts = 0;
    const errors: unknown[] = [];

    const first = await outbox.processBatch(
      async () => {
        attempts++;
        throw new Error('boom');
      },
      { onError: (err) => errors.push(err) },
    );
    assert.equal(first.retried, 1);
    assert.equal(db.rows[0]!.status, 'pending');
    assert.equal(db.rows[0]!.attempts, 1);

    // Not claimable yet — backoff hasn't elapsed.
    const tooSoon = await outbox.processBatch(async () => {
      attempts++;
    });
    assert.equal(tooSoon.claimed, 0);

    await sleep(60);
    const second = await outbox.processBatch(async () => {
      attempts++;
    });
    assert.equal(second.succeeded, 1);
    assert.equal(attempts, 2);
    assert.equal(errors.length, 1);
  });

  test('dead-letters a message once it exhausts maxAttempts', async () => {
    const db = new MemoryDb();
    const outbox = createOutbox({ query: db.query, backoff: () => 0, maxAttempts: 2 });
    await outbox.enqueue(db.query, { topic: 't', payload: {} });

    let attempts = 0;
    const fail = async () => {
      attempts++;
      throw new Error('always fails');
    };

    const first = await outbox.processBatch(fail);
    assert.equal(first.retried, 1);
    assert.equal(db.rows[0]!.status, 'pending');

    const second = await outbox.processBatch(fail);
    assert.equal(second.deadLettered, 1);
    assert.equal(db.rows[0]!.status, 'dead_letter');
    assert.equal(attempts, 2, `stopped after maxAttempts, not attempt ${attempts}`);

    const third = await outbox.processBatch(fail);
    assert.equal(third.claimed, 0, 'a dead-lettered message is never claimed again');
    assert.equal(attempts, 2);
  });

  test('a message can override the outbox-level maxAttempts', async () => {
    const db = new MemoryDb();
    const outbox = createOutbox({ query: db.query, backoff: () => 0, maxAttempts: 5 });
    await outbox.enqueue(db.query, { topic: 't', payload: {}, maxAttempts: 1 });

    const first = await outbox.processBatch(async () => {
      throw new Error('nope');
    });
    assert.equal(first.deadLettered, 1);
  });
});

describe('dispatch', () => {
  test('delivers messages already waiting, then stops on abort', async () => {
    const db = new MemoryDb();
    const outbox = createOutbox({ query: db.query });
    await outbox.enqueue(db.query, { topic: 't', payload: 'hello' });

    const controller = new AbortController();
    const delivered: unknown[] = [];

    const run = outbox.dispatch(
      async (msg) => {
        delivered.push(msg.payload);
        controller.abort();
      },
      { pollInterval: 10, signal: controller.signal },
    );

    await run;
    assert.deepEqual(delivered, ['hello']);
  });

  test('wakes immediately on NOTIFY instead of waiting for the poll interval', async () => {
    const db = new MemoryDb();
    const outbox = createOutbox({ query: db.query, listen: db.listen });
    const controller = new AbortController();
    const delivered = deferred<unknown>();

    const start = Date.now();
    const run = outbox.dispatch(
      async (msg) => {
        delivered.resolve(msg.payload);
        controller.abort();
      },
      { pollInterval: 5000, signal: controller.signal },
    );

    await sleep(15);
    await outbox.enqueue(db.query, { topic: 't', payload: 'fast' });

    const payload = await delivered.promise;
    await run;
    const elapsed = Date.now() - start;

    assert.equal(payload, 'fast');
    assert.ok(elapsed < 1000, `expected a NOTIFY-driven wake-up well under the 5000ms poll interval, took ${elapsed}ms`);
  });

  test('without a listen hook, still delivers by polling', async () => {
    const db = new MemoryDb();
    const outbox = createOutbox({ query: db.query });
    const controller = new AbortController();
    const delivered = deferred<unknown>();

    const run = outbox.dispatch(
      async (msg) => {
        delivered.resolve(msg.payload);
        controller.abort();
      },
      { pollInterval: 20, signal: controller.signal },
    );

    await sleep(5);
    await outbox.enqueue(db.query, { topic: 't', payload: 'polled' });

    assert.equal(await delivered.promise, 'polled');
    await run;
  });
});

describe('concurrent dispatch (SKIP LOCKED semantics)', () => {
  test('two workers polling the same table never double-deliver, and never violate per-key order', async () => {
    const db = new MemoryDb();
    const outbox = createOutbox({ query: db.query });

    const keys = ['a', 'b', 'c'];
    for (const key of keys) {
      for (let i = 0; i < 4; i++) {
        await outbox.enqueue(db.query, { topic: 't', key, payload: `${key}-${i}` });
      }
    }

    const deliveries: string[] = [];
    const activeKeys = new Set<string>();
    let concurrencyViolation = false;

    const worker = async () => {
      let idle = 0;
      while (idle < 3) {
        const result = await outbox.processBatch(async (msg) => {
          const key = msg.key;
          if (activeKeys.has(key)) concurrencyViolation = true;
          activeKeys.add(key);
          await sleep(Math.random() * 5);
          deliveries.push(String(msg.payload));
          activeKeys.delete(key);
        });
        idle = result.claimed === 0 ? idle + 1 : 0;
        await sleep(2);
      }
    };

    await Promise.all([worker(), worker(), worker()]);

    assert.equal(deliveries.length, keys.length * 4, 'every message delivered exactly once');
    assert.equal(new Set(deliveries).size, deliveries.length, 'no duplicate deliveries');
    assert.ok(!concurrencyViolation, 'no two workers processed the same key at once');

    for (const key of keys) {
      const order = deliveries.filter((d) => d.startsWith(`${key}-`));
      assert.deepEqual(order, [`${key}-0`, `${key}-1`, `${key}-2`, `${key}-3`], `key "${key}" delivered out of order`);
    }
  });
});
