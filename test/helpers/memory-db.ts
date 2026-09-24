/**
 * An in-process stand-in for Postgres that implements the `Query` contract for
 * exactly the statements this library issues (matched by the `-- pg-outbox:*`
 * tag every query is written with — see src/sql.ts). It is not a SQL engine;
 * it re-implements the *invariants* the real queries rely on so dispatcher
 * logic, retries, backoff and dead-lettering are covered without a database:
 *
 *   - claim: at most one row per distinct `key`, and never a key that already
 *     has a `processing` row — the same guarantee `claimSQL`'s
 *     `candidates`/`locked` CTEs plus `FOR UPDATE OF m SKIP LOCKED` give you
 *     against real Postgres. This fake enforces it "for free" by resolving
 *     each `query()` call synchronously (see below) rather than by locking,
 *     so it was never subject to the snapshot-staleness bug the real SQL
 *     had before `locked`'s live row-lock check replaced the old
 *     `NOT EXISTS (... status = 'processing')` read — see the README's "How
 *     claiming works" section.
 *   - `available_at` gates claiming, so backoff delays are honored.
 *   - `complete`/`retry`/`dead-letter` mutate exactly the row they're given.
 *
 * Each `query()` call resolves its work synchronously before ever `await`ing,
 * so concurrent callers (`Promise.all([db.query(...), db.query(...)])`) are
 * still serialized one-at-a-time from the fake's point of view — which is
 * exactly the atomicity a single real SQL statement gives you too.
 */
import type { ListenFn, Query } from '../../src/types.ts';

interface MemoryRow {
  id: number;
  key: string;
  topic: string;
  payload: unknown;
  status: 'pending' | 'processing' | 'dead_letter';
  attempts: number;
  max_attempts: number;
  available_at: number;
  locked_at: number | null;
  locked_by: string | null;
  last_error: string | null;
  created_at: number;
}

export class MemoryDb {
  rows: MemoryRow[] = [];
  notifications: { channel: string; payload: string }[] = [];

  #nextId = 1;
  #subscribers = new Map<string, Set<() => void>>();

  query: Query = async (text, params = []) => {
    const trimmed = text.trim();
    if (trimmed === 'BEGIN' || trimmed === 'COMMIT' || trimmed === 'ROLLBACK') {
      // MemoryDb enforces claim exclusivity synchronously within #claim
      // itself (see that method's doc comment), so it has no concept of a
      // multi-statement open transaction to simulate — these are no-ops
      // here. `processBatch` still issues them (matching what it sends
      // real Postgres), which is exactly what exercises that any real
      // `query` function passed to this library must tolerate literal
      // BEGIN/COMMIT/ROLLBACK text landing on the same session.
      return { rows: [] };
    }
    const tag = (text.split('\n', 1)[0] ?? '').trim();
    switch (tag) {
      case '-- pg-outbox:schema':
        return { rows: [] };
      case '-- pg-outbox:insert':
        return this.#insert(params ?? []);
      case '-- pg-outbox:notify':
        return this.#notify(params ?? []);
      case '-- pg-outbox:claim':
        return this.#claim(params ?? []);
      case '-- pg-outbox:complete':
        return this.#complete(params ?? []);
      case '-- pg-outbox:retry':
        return this.#retry(params ?? []);
      case '-- pg-outbox:dead-letter':
        return this.#deadLetter(params ?? []);
      default:
        throw new Error(`MemoryDb: unrecognized statement:\n${text}`);
    }
  };

  /** Lets tests wire `dispatch`'s optional `listen` hook without a real connection. */
  listen: ListenFn = async (channel, onNotify) => {
    let set = this.#subscribers.get(channel);
    if (!set) this.#subscribers.set(channel, (set = new Set()));
    set.add(onNotify);
    return async () => {
      set!.delete(onNotify);
    };
  };

  #insert(params: unknown[]) {
    const [key, topic, payloadJson, maxAttempts] = params as [string, string, string, number];
    const now = Date.now();
    const row: MemoryRow = {
      id: this.#nextId++,
      key,
      topic,
      payload: JSON.parse(payloadJson),
      status: 'pending',
      attempts: 0,
      max_attempts: Number(maxAttempts),
      available_at: now,
      locked_at: null,
      locked_by: null,
      last_error: null,
      created_at: now,
    };
    this.rows.push(row);
    return { rows: [{ id: row.id }] };
  }

  #notify(params: unknown[]) {
    const [channel, payload] = params as [string, string];
    this.notifications.push({ channel, payload });
    for (const cb of this.#subscribers.get(channel) ?? []) cb();
    return { rows: [] };
  }

  /** The load-bearing bit: mirrors claimSQL's guarantees without a database. */
  #claim(params: unknown[]) {
    const [batchSizeRaw, workerId] = params as [number, string];
    const batchSize = Number(batchSizeRaw);
    const now = Date.now();

    const processingKeys = new Set(this.rows.filter((r) => r.status === 'processing').map((r) => r.key));
    const headPerKey = new Map<string, MemoryRow>();
    for (const row of this.rows) {
      if (row.status !== 'pending') continue;
      if (row.available_at > now) continue;
      if (processingKeys.has(row.key)) continue; // a message of this key is already in flight
      const current = headPerKey.get(row.key);
      if (!current || row.id < current.id) headPerKey.set(row.key, row);
    }

    const claimed = [...headPerKey.values()]
      .sort((a, b) => a.id - b.id)
      .slice(0, batchSize);

    for (const row of claimed) {
      row.status = 'processing';
      row.attempts += 1;
      row.locked_at = now;
      row.locked_by = workerId;
    }

    return {
      rows: claimed.map((row) => ({
        id: row.id,
        key: row.key,
        topic: row.topic,
        payload: row.payload,
        attempts: row.attempts,
        max_attempts: row.max_attempts,
        created_at: new Date(row.created_at),
      })),
    };
  }

  #complete(params: unknown[]) {
    const [id] = params as [string];
    this.rows = this.rows.filter((row) => String(row.id) !== String(id));
    return { rows: [] };
  }

  #retry(params: unknown[]) {
    const [id, delayMs, lastError] = params as [string, string, string];
    const row = this.#find(id);
    if (row) {
      row.status = 'pending';
      row.available_at = Date.now() + Number(delayMs);
      row.locked_at = null;
      row.locked_by = null;
      row.last_error = lastError;
    }
    return { rows: [] };
  }

  #deadLetter(params: unknown[]) {
    const [id, lastError] = params as [string, string];
    const row = this.#find(id);
    if (row) {
      row.status = 'dead_letter';
      row.locked_at = null;
      row.locked_by = null;
      row.last_error = lastError;
    }
    return { rows: [] };
  }

  #find(id: string): MemoryRow | undefined {
    return this.rows.find((row) => String(row.id) === String(id));
  }
}
