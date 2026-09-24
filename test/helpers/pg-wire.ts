/**
 * A minimal, hand-rolled Postgres wire-protocol client, used only by the
 * integration test suite. The package itself has zero dependencies and takes
 * a `query` function from *you* (see src/); this file is not that — it is
 * what stands in for `pg` in our own tests, because devDependencies are
 * limited to `typescript` and `@types/node`, so pulling in a real driver just
 * to test against a real database isn't an option.
 *
 * It implements only what the integration suite needs: startup, trust /
 * cleartext / MD5 password auth, and the simple query protocol (params are
 * substituted as escaped SQL literals client-side rather than bound
 * server-side — fine here because every query text originates from this
 * package's own src/sql.ts, never from untrusted input).
 *
 * SCRAM-SHA-256 (Postgres's default auth since v14) is deliberately not
 * implemented — that's a real protocol with real cryptographic exposure, and
 * hand-rolling it for a test harness is the wrong tradeoff. If the server
 * demands it, `connect()` throws `UnsupportedAuthError` and the integration
 * suite skips with a clear message rather than pretending to have covered it.
 */
import net from 'node:net';
import crypto from 'node:crypto';

export class UnsupportedAuthError extends Error {}
export class PgWireError extends Error {}

export interface ConnectionOptions {
  host: string;
  port: number;
  user: string;
  password?: string;
  database: string;
}

export function parseConnectionString(url: string): ConnectionOptions {
  const u = new URL(url);
  return {
    host: u.hostname || '127.0.0.1',
    port: u.port ? Number(u.port) : 5432,
    user: decodeURIComponent(u.username || 'postgres'),
    password: u.password ? decodeURIComponent(u.password) : undefined,
    database: decodeURIComponent(u.pathname.replace(/^\//, '')) || u.username || 'postgres',
  };
}

function md5hex(input: Buffer | string): string {
  return crypto.createHash('md5').update(input).digest('hex');
}

function literal(value: unknown): string {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'number') return String(value);
  return `'${String(value).replace(/'/g, "''")}'`;
}

/** Substitutes `$1, $2, ...` with escaped literals. See the file doc comment for why. */
export function substitute(text: string, params: unknown[] = []): string {
  return text.replace(/\$(\d+)/g, (_, n) => literal(params[Number(n) - 1]));
}

interface Field {
  name: string;
  typeOid: number;
}

function decode(raw: Buffer | null, typeOid: number): unknown {
  if (raw === null) return null;
  const text = raw.toString('utf8');
  switch (typeOid) {
    case 20: // int8
    case 23: // int4
      return Number(text);
    case 16: // bool
      return text === 't';
    case 114: // json
    case 3802: // jsonb
      return JSON.parse(text);
    case 1114: // timestamp
    case 1184: // timestamptz
      return new Date(text);
    default:
      return text;
  }
}

export class PgConnection {
  #socket: net.Socket;
  #buffer = Buffer.alloc(0);
  #waiters: Array<(msg: { type: string; body: Buffer }) => boolean> = [];
  #pending: Array<{ type: string; body: Buffer }> = [];

  private constructor(socket: net.Socket) {
    this.#socket = socket;
    socket.on('data', (chunk) => this.#onData(chunk));
  }

  static async connect(opts: ConnectionOptions, timeoutMs = 3000): Promise<PgConnection> {
    const socket = await new Promise<net.Socket>((resolve, reject) => {
      const s = net.createConnection({ host: opts.host, port: opts.port });
      const timer = setTimeout(() => {
        s.destroy();
        reject(new PgWireError(`connect timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      s.once('connect', () => {
        clearTimeout(timer);
        resolve(s);
      });
      s.once('error', (err) => {
        clearTimeout(timer);
        reject(err);
      });
    });

    const conn = new PgConnection(socket);
    await conn.#startup(opts);
    return conn;
  }

  #onData(chunk: Buffer): void {
    this.#buffer = Buffer.concat([this.#buffer, chunk]);
    for (;;) {
      if (this.#buffer.length < 5) return;
      const type = String.fromCharCode(this.#buffer[0]!);
      const len = this.#buffer.readInt32BE(1);
      if (this.#buffer.length < 1 + len) return;
      const body = this.#buffer.subarray(5, 1 + len);
      this.#buffer = this.#buffer.subarray(1 + len);
      this.#dispatch({ type, body });
    }
  }

  #dispatch(msg: { type: string; body: Buffer }): void {
    // A waiter that returns true has consumed the message and should be removed.
    this.#waiters = this.#waiters.filter((w) => !w(msg));
    this.#pending.push(msg);
  }

  /** Resolves the next time a message matching `type` arrives. */
  #next(...types: string[]): Promise<{ type: string; body: Buffer }> {
    return new Promise((resolve) => {
      // Replay anything already buffered before this call started waiting.
      const idx = this.#pending.findIndex((m) => types.includes(m.type));
      if (idx >= 0) {
        const [msg] = this.#pending.splice(idx, 1);
        resolve(msg!);
        return;
      }
      this.#waiters.push((msg) => {
        if (!types.includes(msg.type)) return false;
        const i = this.#pending.indexOf(msg);
        if (i >= 0) this.#pending.splice(i, 1);
        resolve(msg);
        return true;
      });
    });
  }

  async #startup(opts: ConnectionOptions): Promise<void> {
    const params = Buffer.concat([
      Buffer.from(`user\0${opts.user}\0`),
      Buffer.from(`database\0${opts.database}\0`),
      Buffer.from('\0'),
    ]);
    const header = Buffer.alloc(8);
    header.writeInt32BE(8 + params.length, 0);
    header.writeInt32BE(196608, 4); // protocol version 3.0
    this.#socket.write(Buffer.concat([header, params]));

    const auth = await this.#next('R', 'E');
    if (auth.type === 'E') throw new PgWireError(parseErrorFields(auth.body));

    const code = auth.body.readInt32BE(0);
    if (code === 0) {
      // AuthenticationOk
    } else if (code === 3) {
      await this.#sendPassword(opts.password ?? '');
    } else if (code === 5) {
      const salt = auth.body.subarray(4);
      const inner = md5hex(Buffer.concat([Buffer.from(opts.password ?? ''), Buffer.from(opts.user)]));
      const hashed = 'md5' + md5hex(Buffer.concat([Buffer.from(inner), salt]));
      await this.#sendPassword(hashed);
    } else {
      this.#socket.destroy();
      throw new UnsupportedAuthError(`unsupported auth method (code ${code}); likely SCRAM`);
    }

    // Drain ParameterStatus/BackendKeyData until ReadyForQuery.
    for (;;) {
      const msg = await this.#next('Z', 'E', 'R');
      if (msg.type === 'E') throw new PgWireError(parseErrorFields(msg.body));
      if (msg.type === 'Z') return;
      if (msg.type === 'R' && msg.body.readInt32BE(0) !== 0) {
        this.#socket.destroy();
        throw new UnsupportedAuthError('unexpected additional authentication step');
      }
    }
  }

  async #sendPassword(password: string): Promise<void> {
    const body = Buffer.from(password + '\0');
    const header = Buffer.alloc(5);
    header[0] = 0x70; // 'p'
    header.writeInt32BE(4 + body.length, 1);
    this.#socket.write(Buffer.concat([header, body]));
    const reply = await this.#next('R', 'E');
    if (reply.type === 'E') throw new PgWireError(parseErrorFields(reply.body));
    if (reply.body.readInt32BE(0) !== 0) throw new PgWireError('authentication failed');
  }

  /** Runs one statement over the simple query protocol. No prepared statements. */
  async query(text: string, params: unknown[] = []): Promise<{ rows: any[] }> {
    const sql = substitute(text, params);
    const body = Buffer.from(sql + '\0');
    const header = Buffer.alloc(5);
    header[0] = 0x51; // 'Q'
    header.writeInt32BE(4 + body.length, 1);
    this.#socket.write(Buffer.concat([header, body]));

    let fields: Field[] = [];
    const rows: any[] = [];
    let error: string | undefined;

    for (;;) {
      const msg = await this.#next('T', 'D', 'C', 'E', 'Z');
      if (msg.type === 'T') {
        fields = parseRowDescription(msg.body);
      } else if (msg.type === 'D') {
        rows.push(parseDataRow(msg.body, fields));
      } else if (msg.type === 'E') {
        error = parseErrorFields(msg.body);
      } else if (msg.type === 'Z') {
        break;
      }
      // 'C' (CommandComplete) carries no data we need here.
    }

    if (error) throw new PgWireError(error);
    return { rows };
  }

  async close(): Promise<void> {
    const header = Buffer.alloc(5);
    header[0] = 0x58; // 'X' Terminate
    header.writeInt32BE(4, 1);
    this.#socket.write(header);
    this.#socket.end();
  }
}

function parseRowDescription(body: Buffer): Field[] {
  let offset = 0;
  const count = body.readInt16BE(offset);
  offset += 2;
  const fields: Field[] = [];
  for (let i = 0; i < count; i++) {
    const nameEnd = body.indexOf(0, offset);
    const name = body.toString('utf8', offset, nameEnd);
    offset = nameEnd + 1;
    offset += 4; // table OID
    offset += 2; // column attnum
    const typeOid = body.readInt32BE(offset);
    offset += 4;
    offset += 2; // type size
    offset += 4; // type modifier
    offset += 2; // format code
    fields.push({ name, typeOid });
  }
  return fields;
}

function parseDataRow(body: Buffer, fields: Field[]): Record<string, unknown> {
  let offset = 0;
  const count = body.readInt16BE(offset);
  offset += 2;
  const row: Record<string, unknown> = {};
  for (let i = 0; i < count; i++) {
    const len = body.readInt32BE(offset);
    offset += 4;
    const field = fields[i];
    if (!field) continue;
    if (len === -1) {
      row[field.name] = null;
      continue;
    }
    const raw = body.subarray(offset, offset + len);
    offset += len;
    row[field.name] = decode(raw, field.typeOid);
  }
  return row;
}

function parseErrorFields(body: Buffer): string {
  let offset = 0;
  let message = 'unknown error';
  while (offset < body.length && body[offset] !== 0) {
    const code = String.fromCharCode(body[offset]!);
    offset += 1;
    const end = body.indexOf(0, offset);
    const value = body.toString('utf8', offset, end);
    offset = end + 1;
    if (code === 'M') message = value;
  }
  return message;
}
