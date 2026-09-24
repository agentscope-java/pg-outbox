/**
 * Enqueues a pile of messages across a handful of ordering keys, then runs
 * several dispatchers concurrently against the same table and checks what the
 * claim query (src/sql.ts's `claimSQL`) actually promises: every message
 * delivered exactly once, and never out of order within a key.
 *
 * Runs against the in-memory fake (test/helpers/memory-db.ts), which
 * implements the same claim/retry/dead-letter invariants the real SQL does —
 * see that file's doc comment. This is what `npm run demo` runs; no database
 * needed.
 *
 *   node examples/concurrency-demo.ts
 */
import { createOutbox } from '../src/index.ts';
import { MemoryDb } from '../test/helpers/memory-db.ts';

const KEYS = 25;
const MESSAGES_PER_KEY = 20;
const WORKERS = 6;
const TOTAL = KEYS * MESSAGES_PER_KEY;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const db = new MemoryDb();
const outbox = createOutbox({ query: db.query });

for (let k = 0; k < KEYS; k++) {
  for (let i = 0; i < MESSAGES_PER_KEY; i++) {
    await outbox.enqueue(db.query, { topic: 'demo', key: `key-${k}`, payload: { k, i } });
  }
}

const deliveries: { k: number; i: number }[] = [];
const seen = new Set<string>();
const activeKeys = new Set<string>();
let duplicates = 0;
let concurrencyViolations = 0;

async function worker(): Promise<void> {
  let idleRounds = 0;
  while (idleRounds < 3) {
    const result = await outbox.processBatch(async (msg) => {
      const dedupeKey = msg.id;
      if (seen.has(dedupeKey)) duplicates++;
      seen.add(dedupeKey);

      if (activeKeys.has(msg.key)) concurrencyViolations++;
      activeKeys.add(msg.key);
      await sleep(Math.random() * 2); // simulated publish latency
      activeKeys.delete(msg.key);

      deliveries.push(msg.payload as { k: number; i: number });
    });
    idleRounds = result.claimed === 0 ? idleRounds + 1 : 0;
    await sleep(1);
  }
}

const start = Date.now();
await Promise.all(Array.from({ length: WORKERS }, () => worker()));
const elapsed = Date.now() - start;

let orderingViolations = 0;
for (let k = 0; k < KEYS; k++) {
  const order = deliveries.filter((d) => d.k === k).map((d) => d.i);
  const expected = Array.from({ length: MESSAGES_PER_KEY }, (_, i) => i);
  if (JSON.stringify(order) !== JSON.stringify(expected)) orderingViolations++;
}

console.log(`
  ${WORKERS} concurrent workers, ${TOTAL} messages across ${KEYS} keys

  delivered              ${deliveries.length} / ${TOTAL}
  duplicate deliveries   ${duplicates}
  concurrency violations ${concurrencyViolations}  (two workers holding the same key at once)
  ordering violations    ${orderingViolations} / ${KEYS} keys
  elapsed                ${elapsed}ms
`);

if (deliveries.length !== TOTAL || duplicates > 0 || concurrencyViolations > 0 || orderingViolations > 0) {
  console.error('demo FAILED an invariant it exists to demonstrate');
  process.exitCode = 1;
}
