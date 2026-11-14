#!/usr/bin/env node
// Stage 5: follow one order through the whole system, across both databases.
//
//   npm run trace -- <orderId>
//   npm run trace                 (the most recent order)
//
// order (orders_db) → outbox row (orders_db) → inbox row (notifications_db) → notification (notifications_db)
import { pools } from './lib.mjs';

const db = pools();
const ms = (from, to) => (from && to ? `+${new Date(to) - new Date(from)}ms` : '');

try {
  let orderId = process.argv[2];
  if (!orderId) {
    const { rows } = await db.orders.query('SELECT id FROM orders ORDER BY created_at DESC LIMIT 1');
    if (!rows.length) throw new Error('no orders yet');
    orderId = rows[0].id;
  }

  const order = (await db.orders.query('SELECT * FROM orders WHERE id = $1', [orderId])).rows[0];
  if (!order) throw new Error(`order ${orderId} not found`);
  const outbox = (
    await db.orders.query('SELECT * FROM outbox_messages WHERE aggregate_id = $1 ORDER BY created_at', [orderId])
  ).rows;
  const ids = outbox.map((m) => m.id);
  const inbox = (await db.notifications.query('SELECT * FROM processed_messages WHERE message_id = ANY($1)', [ids]))
    .rows;
  const notifications = (
    await db.notifications.query('SELECT * FROM notifications WHERE order_id = $1 ORDER BY sent_at', [orderId])
  ).rows;

  const t0 = order.created_at;
  console.log(`\norder ${order.id}`);
  console.log(`  1. order saved        ${t0.toISOString()}  ${order.customer_email}, ${order.quantity} x ${order.product}`);
  for (const m of outbox) {
    console.log(`  2. outbox row         ${m.created_at.toISOString()}  ${ms(t0, m.created_at)}  id ${m.id}  ${m.event_type}`);
    console.log(
      `     relay              status ${m.status}, attempts ${m.attempts}` +
        (m.published_at ? `, published ${m.published_at.toISOString()}  ${ms(t0, m.published_at)}` : '') +
        (m.last_error ? `, last error: ${m.last_error}` : ''),
    );
  }
  if (!outbox.length) console.log('  2. outbox row         ✗ none (this order was created before Stage 2)');
  for (const p of inbox) {
    console.log(
      `  3. inbox row          ${p.processed_at.toISOString()}  ${ms(t0, p.processed_at)}  message ${p.message_id}` +
        `, duplicate deliveries skipped: ${p.duplicate_deliveries}`,
    );
  }
  if (outbox.length && !inbox.length) console.log('  3. inbox row          ✗ not processed (yet)');
  for (const n of notifications) {
    console.log(`  4. notification       ${n.sent_at.toISOString()}  ${ms(t0, n.sent_at)}  to ${n.recipient}: "${n.subject}"`);
  }
  if (!notifications.length) console.log('  4. notification       ✗ none');
  if (notifications.length > 1) console.log(`  ⚠ ${notifications.length} notifications for one order (duplicates)`);
  console.log();
} catch (err) {
  console.error(`trace failed: ${err.message}`);
  process.exitCode = 1;
} finally {
  await db.orders.end();
  await db.notifications.end();
}
