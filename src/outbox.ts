import os from 'node:os';
import process from 'node:process';

import { validateIdentifier, schemaSQL, insertSQL, notifySQL, claimSQL, completeSQL, retrySQL, deadLetterSQL } from './sql.ts';
import type {
  CreateOutboxOptions,
  DispatchOptions,
  EnqueueMessage,
  OutboxMessage,
  ProcessBatchOptions,
  ProcessResult,
  Query,
} from './types.ts';

const DEFAULT_TABLE = 'outbox_messages';
const DEFAULT_CHANNEL = 'pg_outbox';
const DEFAULT_MAX_ATTEMPTS = 5;
const DEFAULT_BATCH_SIZE = 10;
const DEFAULT_POLL_INTERVAL = 1000;

function defaultBackoff(attempt: number): number {
  return Math.min(30_000, 500 * 2 ** (attempt - 1));
}

function defaultWorkerId(): string {
  return `${os.hostname()}-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
}

function errorMessage(err: unknown): string {
  const message = err instanceof Error ? err.message : String(err);
  // last_error is a TEXT column with no fixed cap, but a runaway stack trace
  // shouldn't end up bloating every row; keep it to a diagnostic-sized slice.
  return message.slice(0, 2000);
}

function rowToMessage(row: any): OutboxMessage {
  return {
    id: String(row.id),
    key: row.key,
    topic: row.topic,
    payload: row.payload,
    attempts: Number(row.attempts),
    maxAttempts: Number(row.max_attempts),
    createdAt: row.created_at instanceof Date ? row.created_at : new Date(row.created_at),
  };
}

/** Resolves once `ms` pass, `signal` aborts, or `wake()` is called — whichever first. */
class Waiter {
  #settle: (() => void) | null = null;

  wait(ms: number, signal?: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
      const done = () => {
        if (this.#settle !== done) return;
        clearTimeout(timer);
        signal?.removeEventListener('abort', done);
        this.#settle = null;
        resolve();
      };
      const timer = setTimeout(done, ms);
      timer.unref?.();
      signal?.addEventListener('abort', done, { once: true });
      this.#settle = done;
    });
  }

  /** Ends the current `wait()` early, if one is pending. */
  wake(): void {
    this.#settle?.();
  }
}

export interface Outbox {
  /** The (validated, quoted-on-use) table name this instance operates on. */
  table: string;
  /** The raw `CREATE TABLE` / index SQL, for your own migrations. */
  schemaSQL: string;
  /** Runs `schemaSQL` via the configured `query`. Safe to call repeatedly (`IF NOT EXISTS`). */
  createSchema(): Promise<void>;
  /**
   * Writes a message as part of `tx`'s transaction. `tx` must be the same
   * query function your business-data write goes through — a fresh connection
   * here would make the atomicity this library exists for a lie.
   */
  enqueue(tx: Query, message: EnqueueMessage): Promise<{ id: string }>;
  /**
   * Claims and processes one batch (at most one message per distinct key),
   * then returns. Building block `dispatch` is written on top of; call it
   * directly if you want to drive the loop yourself (e.g. from a cron job or
   * a serverless function where a long-lived process isn't an option).
   *
   * Runs as one database transaction, from claim through every message's
   * resolution (complete/retry/dead-letter), committing once at the end.
   * This is not just bookkeeping: it's what keeps a claimed row's lock held
   * for its *entire* time in-flight, which is what makes per-key
   * exclusivity correct — see `claimSQL`'s doc comment and the README's
   * "How claiming works" section. **This means the `query` this `Outbox`
   * was constructed with must be bound to one stable connection/session** —
   * the same requirement `enqueue` has for its `tx`, now extended to
   * dispatch. A connection pool that hands out a different physical
   * connection per call will silently run `BEGIN` and `COMMIT` on different
   * sessions and defeat this — see the README for the failure mode.
   */
  processBatch(handler: (message: OutboxMessage) => Promise<void>, opts?: ProcessBatchOptions): Promise<ProcessResult>;
  /**
   * Runs `processBatch` in a loop until `opts.signal` aborts. Drains
   * immediately while a batch comes back full; otherwise waits for
   * `pollInterval` or a `LISTEN` wake-up, whichever comes first.
   */
  dispatch(handler: (message: OutboxMessage) => Promise<void>, opts?: DispatchOptions): Promise<void>;
}

export function createOutbox(options: CreateOutboxOptions): Outbox {
  const { query } = options;
  const table = validateIdentifier('table', options.table ?? DEFAULT_TABLE);
  const channel = validateIdentifier('channel', options.channel ?? DEFAULT_CHANNEL);
  const defaultMaxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
  const backoff = options.backoff ?? defaultBackoff;
  const listen = options.listen;

  const SQL = {
    schema: schemaSQL(table),
    insert: insertSQL(table),
    notify: notifySQL(),
    claim: claimSQL(table),
    complete: completeSQL(table),
    retry: retrySQL(table),
    deadLetter: deadLetterSQL(table),
  };

  async function createSchema(): Promise<void> {
    await query(SQL.schema);
  }

  async function enqueue(tx: Query, message: EnqueueMessage): Promise<{ id: string }> {
    const key = message.key ?? message.topic;
    const maxAttempts = message.maxAttempts ?? defaultMaxAttempts;
    const { rows } = await tx(SQL.insert, [key, message.topic, JSON.stringify(message.payload), maxAttempts]);
    const id = String(rows[0].id);
    // Deferred until commit by Postgres itself: if `tx`'s transaction rolls
    // back, this notification is never sent. If it wasn't part of `tx`, that
    // guarantee is exactly what would be lost — see enqueue's own doc comment.
    await tx(SQL.notify, [channel, table]);
    return { id };
  }

  async function processBatch(
    handler: (message: OutboxMessage) => Promise<void>,
    opts: ProcessBatchOptions = {},
  ): Promise<ProcessResult> {
    const batchSize = opts.batchSize ?? DEFAULT_BATCH_SIZE;
    const workerId = opts.workerId ?? defaultWorkerId();

    // Claim-through-resolution runs as one explicit transaction so each
    // claimed row's lock (see claimSQL's `FOR UPDATE OF m SKIP LOCKED`) is
    // held for its whole in-flight duration, not just the instant of the
    // claim — see this function's doc comment for why that's load-bearing.
    // `query` must therefore land every call below on the same session.
    await query('BEGIN');
    let messages: OutboxMessage[];
    try {
      const { rows } = await query(SQL.claim, [batchSize, workerId]);
      messages = rows.map(rowToMessage);

      // Every claimed message belongs to a distinct key (that's what `claim`
      // guarantees), so running handlers concurrently can never reorder two
      // messages of the same key — it only overlaps independent keys. The
      // SQL calls each handler makes below (complete/retry/dead-letter) all
      // land on the same connection regardless of handler timing, so they
      // still serialize correctly within this one transaction; only once
      // every message has settled do we decide whether to COMMIT or
      // ROLLBACK, so no query is ever issued after that decision is made.
      const outcomes = await Promise.allSettled(
        messages.map(async (message) => {
          try {
            await handler(message);
          } catch (err) {
            opts.onError?.(err, message);
            if (message.attempts >= message.maxAttempts) {
              await query(SQL.deadLetter, [message.id, errorMessage(err)]);
              return 'deadLettered' as const;
            }
            await query(SQL.retry, [message.id, String(backoff(message.attempts)), errorMessage(err)]);
            return 'retried' as const;
          }
          await query(SQL.complete, [message.id]);
          return 'succeeded' as const;
        }),
      );

      const failed = outcomes.find((o) => o.status === 'rejected') as PromiseRejectedResult | undefined;
      if (failed) throw failed.reason;

      await query('COMMIT');

      const values = (outcomes as PromiseFulfilledResult<'succeeded' | 'retried' | 'deadLettered'>[]).map(
        (o) => o.value,
      );
      return {
        claimed: messages.length,
        succeeded: values.filter((v) => v === 'succeeded').length,
        retried: values.filter((v) => v === 'retried').length,
        deadLettered: values.filter((v) => v === 'deadLettered').length,
      };
    } catch (err) {
      try {
        await query('ROLLBACK');
      } catch {
        // the original error is what matters; a rollback failure (e.g. the
        // connection already dropped) shouldn't mask it.
      }
      throw err;
    }
  }

  async function dispatch(
    handler: (message: OutboxMessage) => Promise<void>,
    opts: DispatchOptions = {},
  ): Promise<void> {
    const pollInterval = opts.pollInterval ?? DEFAULT_POLL_INTERVAL;
    const batchSize = opts.batchSize ?? DEFAULT_BATCH_SIZE;
    const signal = opts.signal;
    const waiter = new Waiter();

    // NOTIFY is not durable — a wake-up missed while nobody is LISTENing is
    // simply lost — so it is only ever a way to poll sooner, never a
    // substitute for the poll itself.
    const unlisten = listen ? await listen(channel, () => waiter.wake()) : undefined;

    try {
      while (!signal?.aborted) {
        const result = await processBatch(handler, { batchSize, workerId: opts.workerId, onError: opts.onError });
        if (signal?.aborted) break;
        if (result.claimed >= batchSize) continue; // batch was full: more is likely waiting
        await waiter.wait(pollInterval, signal);
      }
    } finally {
      await unlisten?.();
    }
  }

  return { table, schemaSQL: SQL.schema, createSchema, enqueue, processBatch, dispatch };
}
