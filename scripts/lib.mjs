// Shared helpers for the Stage 5 scripts (chaos.mjs, trace.mjs).
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import dotenv from 'dotenv';
import pg from 'pg';

export const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
export const ORDERS_DIR = join(ROOT, 'apps/orders-service');
export const NOTIFICATION_DIR = join(ROOT, 'apps/notification-service');

/** Reads an app's .env (falls back to .env.example) without touching process.env. */
export function appEnv(appDir) {
  const file = existsSync(join(appDir, '.env')) ? join(appDir, '.env') : join(appDir, '.env.example');
  return dotenv.parse(readFileSync(file));
}

/** A pg pool per database. Errors on idle clients (e.g. Postgres restarting) must not crash the script. */
export function pools() {
  const make = (appDir) => {
    const pool = new pg.Pool({ connectionString: appEnv(appDir).DATABASE_URL, max: 3 });
    pool.on('error', () => undefined);
    return pool;
  };
  return { orders: make(ORDERS_DIR), notifications: make(NOTIFICATION_DIR) };
}

/** RabbitMQ management API (queue depths). Credentials come from RABBITMQ_URL. */
export function rabbitAdmin() {
  const url = new URL(appEnv(NOTIFICATION_DIR).RABBITMQ_URL ?? 'amqp://outbox:outbox@localhost:5672');
  const base = process.env.RABBITMQ_MGMT_URL ?? `http://${url.hostname}:15672`;
  const auth = 'Basic ' + Buffer.from(`${decodeURIComponent(url.username)}:${decodeURIComponent(url.password)}`).toString('base64');
  return {
    /**
     * Peeks at up to `count` messages and puts them back (ackmode ack_requeue_true).
     * Requeued messages may change order and get `redelivered`, which is fine for a DLQ.
     */
    async peek(name, count = 5000) {
      const res = await fetch(`${base}/api/queues/%2F/${encodeURIComponent(name)}/get`, {
        method: 'POST',
        headers: { authorization: auth, 'content-type': 'application/json' },
        body: JSON.stringify({ count, ackmode: 'ack_requeue_true', encoding: 'auto' }),
      });
      if (!res.ok) return null;
      return res.json();
    },
    async queueDepth(name) {
      const res = await fetch(`${base}/api/queues/%2F/${encodeURIComponent(name)}`, { headers: { authorization: auth } });
      if (!res.ok) return null;
      return (await res.json()).messages ?? 0;
    },
  };
}

export const QUEUES = {
  main: 'notification-service.order-created',
  retry: 'notification-service.order-created.retry',
  dlq: 'notification-service.order-created.dlq',
};

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
