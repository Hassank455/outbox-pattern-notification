#!/usr/bin/env node
// Stage 5: end-to-end failure experiment.
//
//   npm run chaos                                   # 500 orders, 3 relays, 2 consumers, kill -9 every ~1s, 1 RabbitMQ restart
//   npm run chaos -- --orders=2000 --postgres-stop  # bigger, and stop Postgres for a few seconds too
//   npm run chaos -- --unsafe-consumer              # consumers without the inbox: the check should FAIL
//
// It starts its OWN processes (the API with the relay off, N standalone relays, M notification consumers),
// sends orders tagged with a run id, kills processes with SIGKILL at random and restarts them, restarts
// RabbitMQ once, then waits for everything to drain and checks the invariants with SQL.
// Stop your own `npm run dev:*` processes first (the API port must be free).
import { spawn, execFile } from 'node:child_process';
import { mkdirSync, openSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { parseArgs, promisify } from 'node:util';
import { NOTIFICATION_DIR, ORDERS_DIR, QUEUES, ROOT, pools, rabbitAdmin, sleep } from './lib.mjs';

const exec = promisify(execFile);

const { values: opts } = parseArgs({
  options: {
    orders: { type: 'string', default: '500' },
    rate: { type: 'string', default: '100' }, // orders per second
    relays: { type: 'string', default: '3' },
    consumers: { type: 'string', default: '2' },
    'relay-batch': { type: 'string', default: '20' }, // bigger batches = more re-sends when a relay is killed mid-batch
    'kill-every': { type: 'string', default: '1000' }, // ms between kills (± 50% jitter)
    'api-port': { type: 'string', default: '3001' },
    'skip-broker-restart': { type: 'boolean', default: false },
    'postgres-stop': { type: 'boolean', default: false },
    'kill-api': { type: 'boolean', default: false },
    'unsafe-consumer': { type: 'boolean', default: false },
    'skip-build': { type: 'boolean', default: false },
  },
});
const N_ORDERS = Number(opts.orders);
const RATE = Number(opts.rate);
const N_RELAYS = Number(opts.relays);
const N_CONSUMERS = Number(opts.consumers);
const RELAY_BATCH = Number(opts['relay-batch']);
const KILL_EVERY = Number(opts['kill-every']);
const API_PORT = Number(opts['api-port']);
const API = `http://localhost:${API_PORT}`;

const RUN_ID = new Date().toISOString().replace(/[-:T]/g, '').slice(0, 14) + '-' + Math.random().toString(36).slice(2, 6);
const EMAIL = `chaos-${RUN_ID}@example.com`;
const LOG_DIR = join(ROOT, '.chaos', RUN_ID);
const db = pools();
const rabbit = rabbitAdmin();
const t0 = Date.now();
const elapsed = () => `${((Date.now() - t0) / 1000).toFixed(1).padStart(5)}s`;
const log = (msg) => console.log(`[${elapsed()}] ${msg}`);

// ---------------------------------------------------------------- processes
// Children get the parent env minus DATABASE_URL, so each app reads its own from its .env.
const baseEnv = { ...process.env };
delete baseEnv.DATABASE_URL;

class Proc {
  constructor(name, cwd, script, env = {}, healthPort = undefined) {
    Object.assign(this, { name, cwd, script, env, healthPort, child: undefined, kills: 0, starts: 0 });
  }
  get running() {
    return this.child !== undefined;
  }
  start() {
    if (this.child) return; // already running (e.g. a delayed restart after chaos stopped)
    const out = openSync(join(LOG_DIR, `${this.name}.log`), 'a');
    const child = spawn('node', [this.script], { cwd: this.cwd, env: { ...baseEnv, ...this.env }, stdio: ['ignore', out, out] });
    child.on('exit', () => {
      if (this.child === child) this.child = undefined;
    });
    this.child = child;
    this.starts++;
  }
  kill(signal = 'SIGKILL') {
    if (!this.child) return;
    this.child.kill(signal);
    if (signal === 'SIGKILL') this.kills++;
  }
  async waitHealthy(timeoutMs = 30_000) {
    if (!this.healthPort) return sleep(1500); // relays have no HTTP: give them a moment
    const until = Date.now() + timeoutMs;
    while (Date.now() < until) {
      if (await fetch(`http://localhost:${this.healthPort}/health`).then((r) => r.ok, () => false)) return;
      await sleep(250);
    }
    throw new Error(`${this.name} did not become healthy (see ${join(LOG_DIR, `${this.name}.log`)})`);
  }
}

const api = new Proc('api', ORDERS_DIR, 'dist/main.js', { PORT: String(API_PORT), RELAY_ENABLED: 'false' }, API_PORT);
const relays = Array.from(
  { length: N_RELAYS },
  (_, i) => new Proc(`relay-${i + 1}`, ORDERS_DIR, 'dist/relay.main.js', { RELAY_BATCH_SIZE: String(RELAY_BATCH), RELAY_INTERVAL_MS: '200' }),
);
const consumers = Array.from(
  { length: N_CONSUMERS },
  (_, i) =>
    new Proc(
      `consumer-${i + 1}`,
      NOTIFICATION_DIR,
      'dist/main.js',
      {
        PORT: String(3101 + i),
        // 5 retries x 3s = 15s: enough to ride out the Postgres stop without sending messages to the DLQ
        CONSUMER_MAX_RETRIES: '5',
        CONSUMER_RETRY_DELAY_MS: '3000',
        ...(opts['unsafe-consumer'] ? { CONSUMER_DISABLE_INBOX: 'true' } : {}) },
      3101 + i,
    ),
);
const all = [api, ...relays, ...consumers];
const victims = [...relays, ...consumers, ...(opts['kill-api'] ? [api] : [])];

async function stopAll() {
  for (const p of all) p.kill('SIGTERM');
  await sleep(3000);
  for (const p of all) if (p.running) p.child.kill('SIGKILL');
}
process.on('SIGINT', async () => {
  console.log('\ninterrupted, stopping processes...');
  await stopAll();
  process.exit(130);
});

// ---------------------------------------------------------------- chaos
const events = [];
let chaosOn = false;

async function chaosLoop() {
  while (chaosOn) {
    await sleep(KILL_EVERY * (0.5 + Math.random()));
    const candidates = victims.filter((p) => p.running);
    if (!chaosOn || !candidates.length) continue;
    const victim = candidates[Math.floor(Math.random() * candidates.length)];
    victim.kill('SIGKILL');
    const downFor = 300 + Math.floor(Math.random() * 1200);
    events.push(`kill -9 ${victim.name}`);
    setTimeout(() => victim.start(), downFor);
  }
}

async function docker(action, container) {
  const start = Date.now();
  await exec('docker', [action, container]);
  return Date.now() - start;
}

async function restartRabbit() {
  log('🐇 restarting RabbitMQ...');
  const took = await docker('restart', 'outbox-rabbitmq');
  events.push(`RabbitMQ restart (${(took / 1000).toFixed(1)}s)`);
  log(`🐇 RabbitMQ is back (${(took / 1000).toFixed(1)}s)`);
}

async function stopPostgres() {
  log('🐘 stopping Postgres for 4s...');
  await docker('stop', 'outbox-postgres');
  await sleep(4000);
  await docker('start', 'outbox-postgres');
  events.push('Postgres stopped for 4s');
  log('🐘 Postgres started again');
}

// ---------------------------------------------------------------- load
async function sendOrders() {
  const stats = { ok: 0, failed: 0 };
  const inFlight = new Set();
  const gap = 1000 / RATE;
  for (let i = 0; i < N_ORDERS; i++) {
    const p = fetch(`${API}/orders`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ customerEmail: EMAIL, product: `Item ${i + 1}`, quantity: 1, unitPriceCents: 100 + i }),
      signal: AbortSignal.timeout(10_000),
    })
      .then((r) => (r.status === 201 ? stats.ok++ : stats.failed++))
      .catch(() => stats.failed++)
      .finally(() => inFlight.delete(p));
    inFlight.add(p);
    if (inFlight.size >= 50) await Promise.race(inFlight);
    await sleep(gap);
    if (i > 0 && i % Math.max(1, Math.floor(N_ORDERS / 10)) === 0) log(`sent ${i}/${N_ORDERS} orders`);
  }
  await Promise.allSettled(inFlight);
  return stats;
}

// ---------------------------------------------------------------- verification
async function query(pool, sql, params) {
  for (let attempt = 1; ; attempt++) {
    try {
      return (await pool.query(sql, params)).rows;
    } catch (err) {
      if (attempt >= 10) throw err;
      await sleep(1000); // e.g. Postgres just restarted
    }
  }
}

async function runSnapshot() {
  const orders = await query(db.orders, 'SELECT id FROM orders WHERE customer_email = $1', [EMAIL]);
  const orderIds = orders.map((o) => o.id);
  const outbox = await query(
    db.orders,
    'SELECT id, aggregate_id, status, attempts, last_error FROM outbox_messages WHERE aggregate_id = ANY($1::uuid[])',
    [orderIds],
  );
  const notified = await query(
    db.notifications,
    'SELECT order_id, count(*)::int AS n FROM notifications WHERE order_id = ANY($1::uuid[]) GROUP BY order_id',
    [orderIds],
  );
  const inbox = await query(
    db.notifications,
    'SELECT message_id, duplicate_deliveries FROM processed_messages WHERE message_id = ANY($1::text[])',
    [outbox.map((m) => m.id)],
  );
  return { orderIds, outbox, notified, inbox };
}

async function waitForDrain(timeoutMs = 180_000) {
  const until = Date.now() + timeoutMs;
  let stable = 0;
  let last = -1;
  while (Date.now() < until) {
    const s = await runSnapshot();
    const pending = s.outbox.filter((m) => m.status === 'PENDING').length;
    const [main, retry] = await Promise.all([rabbit.queueDepth(QUEUES.main), rabbit.queueDepth(QUEUES.retry)]);
    const notifications = s.notified.reduce((a, r) => a + r.n, 0);
    stable = pending === 0 && main === 0 && retry === 0 && notifications === last ? stable + 1 : 0;
    last = notifications;
    if (stable >= 3) return;
    log(`draining: outbox pending ${pending}, queue ${main ?? '?'}, retry ${retry ?? '?'}, notifications ${notifications}`);
    await sleep(2000);
  }
  log('⚠ drain timed out, verifying anyway');
}

function verify(s, dlqFromRun, dlqDepth) {
  const outboxPerOrder = new Map();
  for (const m of s.outbox) outboxPerOrder.set(m.aggregate_id, (outboxPerOrder.get(m.aggregate_id) ?? 0) + 1);
  const notifPerOrder = new Map(s.notified.map((r) => [r.order_id, r.n]));

  const badOutbox = s.orderIds.filter((id) => outboxPerOrder.get(id) !== 1);
  const notPublished = s.outbox.filter((m) => m.status !== 'PUBLISHED');
  const missing = s.orderIds.filter((id) => !notifPerOrder.has(id));
  const duplicated = s.orderIds.filter((id) => (notifPerOrder.get(id) ?? 0) > 1);
  const extraNotifications = duplicated.reduce((a, id) => a + notifPerOrder.get(id) - 1, 0);
  const skipped = s.inbox.reduce((a, r) => a + r.duplicate_deliveries, 0);
  const republished = s.outbox.reduce((a, m) => a + Math.max(0, m.attempts - 1), 0);

  const checks = [
    ['1. every order has exactly one outbox row', badOutbox.length === 0, `${s.orderIds.length} orders, ${s.outbox.length} outbox rows` + (badOutbox.length ? `, ${badOutbox.length} orders wrong` : '')],
    ['2. every outbox row is PUBLISHED', notPublished.length === 0, notPublished.length ? notPublished.slice(0, 3).map((m) => `${m.id} ${m.status}: ${m.last_error}`).join('; ') : `${s.outbox.length} PUBLISHED`],
    ['3a. no order is missing its notification', missing.length === 0, `${missing.length} missing` + (missing.length ? ` (e.g. ${missing.slice(0, 3).join(', ')})` : '')],
    ['3b. no order has a duplicate notification', duplicated.length === 0, `${duplicated.length} orders with duplicates (${extraNotifications} extra notifications)`],
    ['5. nothing from this run in the DLQ', dlqFromRun === 0, dlqFromRun === null ? 'could not read the DLQ' : `${dlqFromRun} from this run (DLQ holds ${dlqDepth} in total)`],
  ];
  return { checks, skipped, republished, missing, duplicated };
}

// ---------------------------------------------------------------- main
async function main() {
  mkdirSync(LOG_DIR, { recursive: true });
  console.log(`chaos run ${RUN_ID}`);
  console.log(
    `  ${N_ORDERS} orders @ ${RATE}/s, ${N_RELAYS} relays (batch ${RELAY_BATCH}), ${N_CONSUMERS} consumers, kill -9 every ~${KILL_EVERY}ms` +
      `${opts['skip-broker-restart'] ? '' : ', 1 RabbitMQ restart'}${opts['postgres-stop'] ? ', 1 Postgres stop' : ''}` +
      `${opts['kill-api'] ? ', API is a victim too' : ''}${opts['unsafe-consumer'] ? ', ⚠ INBOX DISABLED' : ''}`,
  );
  console.log(`  tag ${EMAIL}, logs in ${LOG_DIR}\n`);

  if (await fetch(`${API}/health`).then(() => true, () => false)) {
    throw new Error(`something is already listening on ${API}. Stop your own orders-service (npm run dev:orders) first.`);
  }
  if (!opts['skip-build']) {
    log('building...');
    await exec('npm', ['run', 'build'], { cwd: ROOT });
  }
  for (const f of [join(ORDERS_DIR, 'dist/main.js'), join(ORDERS_DIR, 'dist/relay.main.js'), join(NOTIFICATION_DIR, 'dist/main.js')]) {
    if (!existsSync(f)) throw new Error(`${f} is missing, run npm run build`);
  }

  for (const p of all) p.start();
  await Promise.all(all.map((p) => p.waitHealthy()));
  log(`started ${all.map((p) => p.name).join(', ')}`);

  chaosOn = true;
  const chaos = chaosLoop();
  const loadSeconds = N_ORDERS / RATE;
  const brokerRestart = opts['skip-broker-restart'] ? null : sleep(loadSeconds * 400).then(restartRabbit);
  const pgStop = opts['postgres-stop'] ? sleep(loadSeconds * 700).then(stopPostgres) : null;

  const sent = await sendOrders();
  log(`load done: ${sent.ok} × 201, ${sent.failed} failed requests`);
  await Promise.all([brokerRestart, pgStop]);
  await sleep(3000); // a bit more chaos while the backlog drains
  chaosOn = false;
  await chaos;
  for (const p of all) if (!p.running) p.start(); // everyone back up for the drain
  log('chaos stopped, waiting for the system to drain...');
  await waitForDrain();

  const snapshot = await runSnapshot();
  // Look INSIDE the DLQ for this run's messages: comparing counts is unreliable (older non-persistent
  // test messages vanish when RabbitMQ restarts).
  const dlqMessages = await rabbit.peek(QUEUES.dlq);
  const dlqFromRun = dlqMessages === null ? null : dlqMessages.filter((m) => String(m.payload).includes(EMAIL)).length;
  const dlqDepth = dlqMessages === null ? null : dlqMessages.length;
  const { checks, skipped, republished } = verify(snapshot, dlqFromRun, dlqDepth);

  const kills = victims.map((p) => `${p.name} ×${p.kills}`).join(', ');
  const ok = checks.every(([, pass]) => pass);
  console.log(`\n================ REPORT ${RUN_ID} ================`);
  console.log(`client:   ${sent.ok} orders accepted (201), ${sent.failed} requests failed`);
  console.log(`chaos:    ${events.filter((e) => e.startsWith('kill')).length} kill -9 (${kills})`);
  for (const e of events.filter((e) => !e.startsWith('kill'))) console.log(`          ${e}`);
  console.log('');
  for (const [name, pass, detail] of checks) console.log(`${pass ? '✅' : '❌'} ${name.padEnd(44)} ${detail}`);
  console.log(`ℹ️  4. duplicates delivered but skipped by inbox ${skipped}`);
  console.log(`ℹ️     failed publish attempts recorded       ${republished} (re-sends after a relay crash leave no trace in the outbox: the rollback erases them)`);
  console.log(`\n${ok ? 'PASS ✅ every order → exactly one notification' : 'FAIL ❌ see the checks above'}`);
  console.log(`(orders in DB: ${snapshot.orderIds.length}; client 201s: ${sent.ok}; the DB is the source of truth)\n`);
  return ok;
}

let ok = false;
try {
  ok = await main();
} catch (err) {
  console.error(`\nchaos run failed: ${err.message}`);
} finally {
  await stopAll();
  await db.orders.end().catch(() => undefined);
  await db.notifications.end().catch(() => undefined);
}
process.exit(ok ? 0 : 1);
