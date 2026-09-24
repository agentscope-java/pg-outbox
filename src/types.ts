/**
 * The one thing this library needs from your database driver: something that
 * runs parameterized SQL and hands back rows. `pg`'s `client.query`,
 * `postgres.js` wrapped in `sql.unsafe`, and most ORMs' raw-query escape
 * hatches all satisfy this shape already — see the README for adapters.
 *
 * **The `query` passed to `createOutbox` must be bound to one stable
 * connection/session — not a `pg.Pool`, whose `.query` hands out a
 * different physical connection per call.** `processBatch` issues a literal
 * `BEGIN`, the claim, every claimed message's resolution, and `COMMIT` as
 * several calls to this function; that only holds together as one
 * transaction if they all land on the same session. See the README's "How
 * claiming works" section for why, and what a pool does wrong here (it
 * won't throw — it silently runs `BEGIN`/`COMMIT` on different connections
 * and loses the per-key exclusivity guarantee this library exists for).
 */
export type Query = (text: string, params?: unknown[]) => Promise<{ rows: any[] }>;

/** A message to write to the outbox, in the same transaction as your business data. */
export interface EnqueueMessage {
  /** What kind of event this is. Consumers typically route on this. */
  topic: string;
  /** JSON-serializable payload. Stored as `jsonb`. */
  payload: unknown;
  /**
   * Ordering key. Messages with the same key are never delivered to two
   * workers at once, and are delivered in the order they were enqueued.
   * Defaults to `topic`, so same-topic messages are ordered unless you pass
   * something more specific (e.g. an aggregate id).
   */
  key?: string;
  /** Overrides the outbox-level default attempt budget for this message. */
  maxAttempts?: number;
}

/** A message handed to your dispatch handler. */
export interface OutboxMessage {
  /** Stable id. Use it to deduplicate — delivery is at-least-once, never exactly-once. */
  id: string;
  topic: string;
  key: string;
  payload: unknown;
  /** How many times delivery has been attempted, including this one. */
  attempts: number;
  maxAttempts: number;
  createdAt: Date;
}

/** Outcome of one claim-and-process pass. */
export interface ProcessResult {
  /** Messages claimed this pass (at most one per distinct key). */
  claimed: number;
  succeeded: number;
  /** Failed but under `maxAttempts`; requeued with backoff. */
  retried: number;
  /** Failed and out of attempts; moved to the dead-letter state. */
  deadLettered: number;
}

export interface ProcessBatchOptions {
  /** Messages claimed per pass; at most one per distinct key. Default 10. */
  batchSize?: number;
  /** Recorded on claimed rows; useful in logs and for `SELECT * FROM table`. Default `os.hostname()-pid`. */
  workerId?: string;
  /** Called for every handler failure, before the retry/dead-letter decision is written. */
  onError?: (error: unknown, message: OutboxMessage) => void;
}

export interface DispatchOptions extends ProcessBatchOptions {
  /** Fallback poll interval in ms, used when idle and (if configured) between NOTIFY wake-ups. Default 1000. */
  pollInterval?: number;
  /** Stops the loop. `dispatch()` resolves once the in-flight batch finishes. */
  signal?: AbortSignal;
}

/**
 * Subscribes to the outbox's NOTIFY channel and calls `onNotify` for every
 * notification. Returns a function that unsubscribes.
 *
 * There is no driver-agnostic way to receive `NOTIFY` — it is a push from a
 * held connection, not a query/response — so this is the seam you wire up
 * yourself for whichever client you use. See the README for a `pg` example.
 * Without it, `dispatch` still works correctly; it just polls.
 */
export type ListenFn = (channel: string, onNotify: () => void) => Promise<() => Promise<void>>;

export interface CreateOutboxOptions {
  /**
   * Runs SQL and returns rows. Used for dispatch, retries, and schema
   * creation — not for `enqueue`. Must be bound to one stable
   * connection/session, not a pool — see `Query`'s doc comment.
   */
  query: Query;
  /** Table name. Default `outbox_messages`. Must be a bare identifier. */
  table?: string;
  /** Default attempt budget for messages that don't set their own. Default 5. */
  maxAttempts?: number;
  /** `attempt` (1-based) -> delay in ms before the next try. Default capped exponential. */
  backoff?: (attempt: number) => number;
  /** `NOTIFY`/`LISTEN` channel name. Default `pg_outbox`. Must be a bare identifier. */
  channel?: string;
  /** Optional `LISTEN` fast path. See `ListenFn`. */
  listen?: ListenFn;
}
