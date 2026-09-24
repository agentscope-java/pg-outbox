import { InvalidIdentifierError } from './errors.ts';

const IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

/**
 * Table and channel names can't be bound as parameters — Postgres only lets you
 * parameterize values, not identifiers — so anything that ends up interpolated
 * into SQL text is checked against this pattern first and quoted after.
 */
export function validateIdentifier(kind: string, value: string): string {
  if (!IDENTIFIER.test(value)) throw new InvalidIdentifierError(kind, value);
  return value;
}

function quoted(identifier: string): string {
  return `"${identifier}"`;
}

/**
 * The raw schema SQL, for people who want it in their own migration rather
 * than applied behind their back. `createOutbox(...).createSchema()` runs
 * exactly this.
 */
export function schemaSQL(table: string): string {
  const t = quoted(table);
  return `-- pg-outbox:schema
CREATE TABLE IF NOT EXISTS ${t} (
  id BIGSERIAL PRIMARY KEY,
  key TEXT NOT NULL,
  topic TEXT NOT NULL,
  payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  locked_at TIMESTAMPTZ,
  locked_by TEXT,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ${quoted(table + '_status_check')} CHECK (status IN ('pending', 'processing', 'dead_letter'))
);

CREATE INDEX IF NOT EXISTS ${quoted(table + '_dispatch_idx')}
  ON ${t} (key, id)
  WHERE status IN ('pending', 'processing');
`;
}

export function insertSQL(table: string): string {
  return `-- pg-outbox:insert
INSERT INTO ${quoted(table)} (key, topic, payload, max_attempts)
VALUES ($1, $2, $3::jsonb, $4)
RETURNING id;`;
}

export function notifySQL(): string {
  return `-- pg-outbox:notify
SELECT pg_notify($1, $2);`;
}

/**
 * Claims up to `$1` messages, at most one per distinct `key`.
 *
 * **Must be run as the first statement of an explicit transaction that
 * stays open until every claimed message is resolved** (`processBatch`
 * wraps itself in `BEGIN`/`COMMIT` for exactly this reason — see its doc
 * comment). Two CTEs, two different jobs:
 *
 * - `candidates` picks *one specific row id* per key — the oldest pending
 *   one — using an ordinary MVCC-snapshot read. This is where a key's
 *   "next message" gets decided, and critically, it's decided before
 *   anything tries to lock anything.
 * - `locked` then tries to lock *only those chosen rows*, live, via
 *   `FOR UPDATE OF m SKIP LOCKED`. If a key's chosen row is currently
 *   locked — another transaction claimed it and hasn't resolved it yet —
 *   that row is dropped. Because `candidates` already committed to exactly
 *   one row per key before any locking happened, there's no second row for
 *   `locked` to fall back to: a key whose head message is in flight
 *   contributes *nothing* to this batch. Row-lock checks are never
 *   snapshot-stale the way a `status` column read can be, which is what
 *   makes this exclusion hold regardless of how long the scan takes or how
 *   busy the table is — see the README's "How claiming works" section for
 *   the bug this replaced, and the numbers that exposed it.
 */
export function claimSQL(table: string): string {
  const t = quoted(table);
  return `-- pg-outbox:claim
WITH candidates AS (
  SELECT DISTINCT ON (key) id
  FROM ${t}
  WHERE status = 'pending'
    AND available_at <= now()
  ORDER BY key, id
),
locked AS (
  SELECT m.id
  FROM ${t} m
  JOIN candidates c ON c.id = m.id
  ORDER BY m.id
  LIMIT $1
  FOR UPDATE OF m SKIP LOCKED
)
UPDATE ${t} m
SET status = 'processing',
    attempts = m.attempts + 1,
    locked_at = now(),
    locked_by = $2
FROM locked
WHERE m.id = locked.id
RETURNING m.id, m.key, m.topic, m.payload, m.attempts, m.max_attempts, m.created_at;`;
}

export function completeSQL(table: string): string {
  return `-- pg-outbox:complete
DELETE FROM ${quoted(table)} WHERE id = $1;`;
}

export function retrySQL(table: string): string {
  return `-- pg-outbox:retry
UPDATE ${quoted(table)}
SET status = 'pending',
    available_at = now() + ($2 || ' milliseconds')::interval,
    locked_at = NULL,
    locked_by = NULL,
    last_error = $3
WHERE id = $1;`;
}

export function deadLetterSQL(table: string): string {
  return `-- pg-outbox:dead-letter
UPDATE ${quoted(table)}
SET status = 'dead_letter',
    locked_at = NULL,
    locked_by = NULL,
    last_error = $2
WHERE id = $1;`;
}
