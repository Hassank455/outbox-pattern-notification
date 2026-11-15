#!/usr/bin/env node
// Stage 6: compare relay modes. For each mode it starts its own API (no relay), ONE relay in that
// mode and one consumer, sends orders at a steady rate, stays idle for a while, then reports:
//   - end-to-end latency: order created → notification stored (both timestamps from Postgres)
//   - relay latency:      order created → outbox row published
//   - DB load:            how many "claim" queries the relay ran, and how many found nothing
//
//   npm run latency                              # polling vs notify, 200 orders @ 20/s, then 20s idle
//   npm run latency -- --modes=notify --orders=500 --rate=50
//   npm run latency -- --modes=none             # with Debezium running (CDC publishes, our relay is off)
//
// Stop your own `npm run dev:*` processes first (port 3001 must be free).
import { execFile, spawn } from 'node:child_process';
import { mkdirSync, openSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { parseArgs, promisify } from 'node:util';
import { NOTIFICATION_DIR, ORDERS_DIR, ROOT, pools, sleep } from './lib.mjs';

const exec = promisify(execFile);
const { values: opts } = parseArgs({
  options: {
    modes: { type: 'string', default: 'polling,notify' },
    orders: { type: 'string', default: '200' },
    rate: { type: 'string', default: '20' },
    idle: { type: 'string', default: '20' }, // seconds of no traffic after the load, to see idle DB load
    'skip-build': { type: 'boolean', default: false },
  },
});
const MODES = opts.modes.split(',').map((m) => m.trim());
const N = Number(opts.orders);
const RATE = Number(opts.rate);
const IDLE_S = Number(opts.idle);
const RUN_ID = new Date().toISOString().replace(/[-:T]/g, '').slice(0, 14);
const LOG_DIR = join(ROOT, '.chaos', `latency-${RUN_ID}`);
const db = pools();

const baseEnv = { ...process.env };
delete baseEnv.DATABASE_URL;

function startProc(name, cwd, script, env) {
  const out = openSync(join(LOG_DIR, `${name}.log`), 'a');
  return spawn('node', [script], { cwd, env: { ...baseEnv, ...env }, stdio: ['ignore', out, out] });
}
async function stopProc(child) {
  if (child.exitCode !== null) return;
  const exited = new Promise((r) => child.once('exit', r));
  child.kill('SIGTERM');
  await Promise.race([exited, sleep(5000)]);
  if (child.exitCode === null) child.kill('SIGKILL');
}
async function waitHealthy(port) {
  for (let i = 0; i < 120; i++) {
    if (await fetch(`http://localhost:${port}/health`).then((r) => r.ok, () => false)) return;
    await sleep(250);
  }
  throw new Error(`port ${port} never became healthy (logs in ${LOG_DIR})`);
}
const pct = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
const fmt = (ms) => (ms === undefined || Number.isNaN(ms) ? '–' : ms >= 1000 ? `${(ms / 1000).toFixed(2)}s` : `${Math.round(ms)}ms`);

async function runMode(mode) {
  const email = `latency-${mode}-${RUN_ID}@example.com`;
  console.log(`\n▶ ${mode}: ${N} orders @ ${RATE}/s, then ${IDLE_S}s idle`);
  const api = startProc(`${mode}-api`, ORDERS_DIR, 'dist/main.js', { PORT: '3001', RELAY_MODE: 'none' });
  const consumer = startProc(`${mode}-consumer`, NOTIFICATION_DIR, 'dist/main.js', { PORT: '3101' });
  await Promise.all([waitHealthy(3001), waitHealthy(3101)]);
  const relay = startProc(`${mode}-relay`, ORDERS_DIR, 'dist/relay.main.js', { RELAY_MODE: mode });
  await sleep(2000);

  const t0 = Date.now();
  const inFlight = [];
  for (let i = 0; i < N; i++) {
    inFlight.push(
      fetch('http://localhost:3001/orders', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ customerEmail: email, product: `Item ${i}`, quantity: 1, unitPriceCents: 100 }),
      }).catch(() => undefined),
    );
    await sleep(1000 / RATE);
  }
  await Promise.all(inFlight);
  await sleep(IDLE_S * 1000);
  const seconds = (Date.now() - t0) / 1000;
  await stopProc(relay);
  await Promise.all([stopProc(api), stopProc(consumer)]);

  const orders = (
    await db.orders.query(
      `SELECT o.id, o.created_at, m.published_at FROM orders o
       JOIN outbox_messages m ON m.aggregate_id = o.id WHERE o.customer_email = $1`,
      [email],
    )
  ).rows;
  const notes = (
    await db.notifications.query('SELECT order_id, sent_at FROM notifications WHERE order_id = ANY($1::uuid[])', [
      orders.map((o) => o.id),
    ])
  ).rows;
  const sentAt = new Map(notes.map((n) => [n.order_id, n.sent_at]));
  const e2e = orders.filter((o) => sentAt.has(o.id)).map((o) => sentAt.get(o.id) - o.created_at).sort((a, b) => a - b);
  const relayLag = orders.filter((o) => o.published_at).map((o) => o.published_at - o.created_at).sort((a, b) => a - b);

  const relayLog = readFileSync(join(LOG_DIR, `${mode}-relay.log`), 'utf8').replace(/\x1b\[[0-9;]*m/g, '');
  const m = relayLog.match(/(\d+) claim queries \((\d+) found nothing\)/);
  return {
    mode,
    orders: orders.length,
    notified: e2e.length,
    e2e: { p50: pct(e2e, 50), p95: pct(e2e, 95), max: e2e.at(-1) },
    relay: { p50: pct(relayLag, 50), p95: pct(relayLag, 95), max: relayLag.at(-1) },
    claims: m ? Number(m[1]) : null,
    empty: m ? Number(m[2]) : null,
    seconds,
  };
}

try {
  mkdirSync(LOG_DIR, { recursive: true });
  if (await fetch('http://localhost:3001/health').then(() => true, () => false)) {
    throw new Error('port 3001 is in use. Stop your own orders-service first.');
  }
  if (!opts['skip-build']) await exec('npm', ['run', 'build'], { cwd: ROOT });
  const results = [];
  for (const mode of MODES) results.push(await runMode(mode));

  console.log('\n| mode | orders → notified | e2e p50 | e2e p95 | e2e max | relay p50 | relay p95 | claim queries (empty) | queries/min |');
  console.log('|---|---|---|---|---|---|---|---|---|');
  for (const r of results) {
    console.log(
      `| ${r.mode} | ${r.orders} → ${r.notified} | ${fmt(r.e2e.p50)} | ${fmt(r.e2e.p95)} | ${fmt(r.e2e.max)} | ` +
        `${fmt(r.relay.p50)} | ${fmt(r.relay.p95)} | ${r.claims ?? '–'} (${r.empty ?? '–'}) | ${r.claims === null ? '–' : ((r.claims / r.seconds) * 60).toFixed(0)} |`,
    );
  }
  console.log(`\nlogs: ${LOG_DIR}`);
} catch (err) {
  console.error(`latency run failed: ${err.message}`);
  process.exitCode = 1;
} finally {
  await db.orders.end();
  await db.notifications.end();
}
