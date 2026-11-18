#!/usr/bin/env node
// Stage 7 experiment: crash the email worker AFTER the provider accepted an email but BEFORE the
// transaction that marks it SENT commits. Then count how many emails customers really received.
//
//   npm run email-chaos                        # with Idempotency-Key (expected: no duplicates)
//   npm run email-chaos -- --no-key            # without it         (expected: duplicates)
//   npm run email-chaos -- --emails 500 --crashes 5 --crash-after 37
//
// What it does:
//   1. starts its own fake email provider (port 3013) so the counts are clean
//   2. inserts N PENDING rows into outbound_emails, tagged with a run id
//   3. runs the standalone email worker K times with EMAIL_WORKER_CRASH_AFTER_SENDS, so each
//      run SIGKILLs itself mid-batch: emails already sent, batch never committed
//   4. runs a normal worker until the run's rows are all SENT
//   5. compares rows vs what the provider actually delivered
//
// Needs: Postgres running, notification-service migrated and built (npm run build).
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { NOTIFICATION_DIR, ROOT, appEnv, pools, sleep } from './lib.mjs';

const args = process.argv.slice(2);
const opt = (name, def) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? Number(args[i + 1]) : def;
};
const N = opt('emails', 300);
const CRASHES = opt('crashes', 3);
const CRASH_AFTER = opt('crash-after', 37);
const USE_KEY = !args.includes('--no-key');
const PROVIDER_PORT = 3013;
const PROVIDER = `http://localhost:${PROVIDER_PORT}`;
const RUN = `chaos7-${randomUUID().slice(0, 8)}`;

const workerJs = join(NOTIFICATION_DIR, 'dist/email-worker.main.js');
if (!existsSync(workerJs)) {
  console.error('notification-service is not built: run `npm run build` first.');
  process.exit(1);
}

const db = pools().notifications;
const children = new Set();
const cleanup = () => children.forEach((c) => c.kill('SIGKILL'));
process.on('exit', cleanup);
process.on('SIGINT', () => process.exit(130));

function start(cmd, argv, env, cwd, label) {
  const child = spawn(cmd, argv, { cwd, env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
  children.add(child);
  child.on('exit', () => children.delete(child));
  const tag = (d) =>
    d
      .toString()
      .replace(/\x1b\[[0-9;]*m/g, '')
      .split('\n')
      .filter((l) => /💥|batch:|FAILED|error/i.test(l))
      .forEach((l) => console.log(`   [${label}] ${l.replace(/^.*?(\[Nest\][^\]]*\]\s*)?/, '').trim().slice(0, 160)}`));
  child.stdout.on('data', tag);
  child.stderr.on('data', tag);
  return child;
}

const workerEnv = (extra) => ({
  ...appEnv(NOTIFICATION_DIR),
  EMAIL_PROVIDER_URL: PROVIDER,
  EMAIL_USE_IDEMPOTENCY_KEY: String(USE_KEY),
  EMAIL_WORKER_BATCH_SIZE: '50',
  EMAIL_WORKER_INTERVAL_MS: '200',
  ...extra,
});

const pending = async () =>
  Number(
    (await db.query(`SELECT count(*) FROM outbound_emails WHERE recipient LIKE $1 AND status = 'PENDING'`, [`${RUN}-%`]))
      .rows[0].count,
  );

async function main() {
  console.log(`\n📧 Stage 7 email chaos  run=${RUN}  emails=${N}  crashes=${CRASHES} (after ${CRASH_AFTER} sends each)`);
  console.log(`   Idempotency-Key: ${USE_KEY ? 'ON' : 'OFF ⚠️'}\n`);

  // 1. our own provider
  start('node', [join(ROOT, 'tools/fake-email-provider.mjs')], { PORT: String(PROVIDER_PORT) }, ROOT, 'provider');
  for (let i = 0; i < 50; i++) {
    try {
      await fetch(`${PROVIDER}/v1/stats`);
      break;
    } catch {
      await sleep(100);
    }
  }

  // Older PENDING rows (from the app or earlier runs) would also be sent to our provider and
  // muddy the numbers: park them out of reach for the duration of the run.
  const parked = await db.query(
    `UPDATE outbound_emails SET next_attempt_at = now() + interval '1 hour'
     WHERE status = 'PENDING' AND recipient NOT LIKE $1 AND (next_attempt_at IS NULL OR next_attempt_at < now() + interval '1 hour')
     RETURNING id`,
    [`${RUN}-%`],
  );
  if (parked.rowCount) console.log(`   (parked ${parked.rowCount} unrelated PENDING emails for 1h)`);

  // 2. N emails waiting in the outbox
  const values = [];
  const params = [];
  for (let i = 0; i < N; i++) {
    const b = i * 5;
    values.push(`(gen_random_uuid(), $${b + 1}, gen_random_uuid(), $${b + 2}, $${b + 3}, $${b + 4}, $${b + 5})`);
    params.push(`${RUN}:${i}`, `${RUN}-${i}@example.com`, `Order confirmed #${i}`, 'Thanks for your order!', new Date(Date.now() + i));
  }
  await db.query(
    `INSERT INTO outbound_emails (id, idempotency_key, order_id, recipient, subject, body, created_at) VALUES ${values.join(',')}`,
    params,
  );
  console.log(`1) inserted ${N} PENDING emails`);

  // 3. crashing workers
  for (let k = 1; k <= CRASHES && (await pending()) > 0; k++) {
    const w = start('node', [workerJs], workerEnv({ EMAIL_WORKER_CRASH_AFTER_SENDS: String(CRASH_AFTER) }), NOTIFICATION_DIR, `worker#${k}`);
    await new Promise((r) => w.on('exit', r));
    console.log(`2.${k}) worker killed (SIGKILL), ${await pending()} still PENDING`);
  }

  // 4. a healthy worker drains the rest
  const w = start('node', [workerJs], workerEnv({}), NOTIFICATION_DIR, 'worker');
  const t0 = Date.now();
  while ((await pending()) > 0 && Date.now() - t0 < 120_000) await sleep(300);
  w.kill('SIGTERM');
  await sleep(500);

  // 5. compare
  const rows = (
    await db.query(
      `SELECT status, count(*)::int AS n FROM outbound_emails WHERE recipient LIKE $1 GROUP BY status`,
      [`${RUN}-%`],
    )
  ).rows;
  const byStatus = Object.fromEntries(rows.map((r) => [r.status, r.n]));
  const p = await (await fetch(`${PROVIDER}/v1/stats?prefix=${RUN}-`)).json();

  const ok = p.delivered === N && p.extraCopies === 0;
  console.log(`
──────────────── result ────────────────
emails in the outbox        ${N}   (SENT ${byStatus.SENT ?? 0}, PENDING ${byStatus.PENDING ?? 0}, FAILED ${byStatus.FAILED ?? 0})
send requests to provider   ${p.requests}
replays (key seen before)   ${p.replays}
emails customers received   ${p.delivered}
duplicate copies received   ${p.extraCopies}
────────────────────────────────────────
${ok ? '✅ PASS: every customer got exactly one email' : `❌ ${p.extraCopies} extra copies delivered: some customers got the same email several times`}
`);
  await db.end();
  process.exit(ok || !USE_KEY ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
