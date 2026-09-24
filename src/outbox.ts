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

    const { rows } = await query(SQL.claim, [batchSize, workerId, table]);
    const messages = rows.map(rowToMessage);

    // Every claimed message belongs to a distinct key (that's what `claim`
    // guarantees), so processing the batch concurrently can never reorder two
    // messages of the same key — it only overlaps independent keys.
    const outcomes = await Promise.all(
      messages.map(async (message) => {
        try {
          await handler(message);
          await query(SQL.complete, [message.id]);
          return 'succeeded' as const;
        } catch (err) {
          opts.onError?.(err, message);
          if (message.attempts >= message.maxAttempts) {
            await query(SQL.deadLetter, [message.id, errorMessage(err)]);
            return 'deadLettered' as const;
          }
          await query(SQL.retry, [message.id, String(backoff(message.attempts)), errorMessage(err)]);
          return 'retried' as const;
        }
      }),
    );

    return {
      claimed: messages.length,
      succeeded: outcomes.filter((o) => o === 'succeeded').length,
      retried: outcomes.filter((o) => o === 'retried').length,
      deadLettered: outcomes.filter((o) => o === 'deadLettered').length,
    };
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
