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
 * Three mechanisms, layered:
 *
 * 1. `NOT EXISTS (... status = 'processing')` — a key with a message already
 *    in flight is skipped entirely, for as long as that message is in flight
 *    (this is what makes ordering durable across the whole processing time,
 *    not just the instant of the claim).
 * 2. `pg_try_advisory_xact_lock` — closes the narrow race where two workers
 *    run this query concurrently against the *same* key's first-ever pending
 *    message: whichever worker's query reaches that key first holds the lock
 *    for the rest of this statement, so the other worker's query excludes
 *    every row of that key rather than falling through to a later one.
 * 3. `FOR UPDATE SKIP LOCKED` — the standard non-blocking claim: if another
 *    concurrent statement already has a candidate row locked, this query skips
 *    it and moves on instead of blocking behind it. This is what lets several
 *    dispatchers run against the same table without serializing on each other.
 */
export function claimSQL(table: string): string {
  const t = quoted(table);
  return `-- pg-outbox:claim
WITH locked AS (
  SELECT t.id, t.key
  FROM ${t} t
  WHERE t.status = 'pending'
    AND t.available_at <= now()
    AND NOT EXISTS (
      SELECT 1 FROM ${t} p WHERE p.key = t.key AND p.status = 'processing'
    )
    AND pg_try_advisory_xact_lock(hashtext($3), hashtext(t.key))
  ORDER BY t.id
  FOR UPDATE SKIP LOCKED
),
head AS (
  SELECT DISTINCT ON (key) id
  FROM locked
  ORDER BY key, id
  LIMIT $1
)
UPDATE ${t} m
SET status = 'processing',
    attempts = m.attempts + 1,
    locked_at = now(),
    locked_by = $2
FROM head
WHERE m.id = head.id
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
