/** Base class for every error this library throws. */
export class OutboxError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/**
 * A `table` or `channel` option was not a safe, bare SQL identifier.
 *
 * Table and channel names cannot be bound as query parameters — they have to be
 * interpolated into the SQL text — so they are validated against a strict
 * identifier pattern before that happens. This is what stands between an
 * options object sourced from config and a SQL-injection hole.
 */
export class InvalidIdentifierError extends OutboxError {
  kind: string;
  value: string;

  constructor(kind: string, value: string) {
    super(`Invalid ${kind} ${JSON.stringify(value)}: must match /^[a-zA-Z_][a-zA-Z0-9_]*$/`);
    this.kind = kind;
    this.value = value;
  }
}
