# Stage 4: Idempotent consumer (inbox, manual ack, retries, DLQ)

## ⚠️ If you ran Stages 1–3 before: one reset step

The main queue now has dead-letter settings. RabbitMQ refuses to change the settings of an existing
queue (`PRECONDITION_FAILED - inequivalent arg 'x-dead-letter-exchange'`), so delete the old queue once.
It must be empty (stop notification-service only after it has drained):

```bash
docker exec outbox-rabbitmq rabbitmqctl delete_queue notification-service.order-created --if-empty
npx prisma migrate deploy   # in apps/notification-service, or: npm run prisma:migrate
```

notification-service recreates the queue with the new settings on startup. If you forget, it logs
this exact command and keeps retrying every 5s. (Already done on the dev Mac.)

## The problem we're solving

Stage 3 showed the outbox delivers **at least once**: kill the relay mid-batch and 222 customers got
the email twice. RabbitMQ itself also re-delivers any message that wasn't acked (consumer crash,
connection drop). Duplicates are normal, so the consumer must make them harmless: it has to be **idempotent**.

## The inbox pattern

New table in `notifications_db` (migration `20261114170425_add_processed_messages_inbox`):

| Column | Why |
|---|---|
| `message_id` (PK) | The AMQP `messageId` = orders-service outbox id. Same event → same id, every delivery. |
| `message_type` | `order.created` |
| `received_at`, `processed_at` | When the first delivery arrived / was committed |
| `duplicate_deliveries`, `last_duplicate_at` | How many later deliveries were skipped (for learning/monitoring) |

Handling one message (`NotificationsService.handleOrderCreated`):

```
BEGIN
  INSERT INTO processed_messages (message_id, ...) ON CONFLICT DO NOTHING
  -- 0 rows? we've handled this id before → count it as a duplicate, COMMIT, ack, done
  INSERT INTO notifications (...)
COMMIT
ack
```

**Why the inbox row and the notification must be in ONE transaction:**
- inbox committed, notification failed → the retry sees "already handled" and skips it: **lost**
- notification committed, inbox failed → the retry writes a second notification: **duplicate**

Together they commit or roll back as one, so a retry always sees the truth. It's the same trick as the
outbox: one database, one transaction, no dual write.

**Concurrent duplicates:** if two deliveries of the same id run at the same time, the primary key makes
the second `INSERT` wait for the first transaction. Commit → the second inserts 0 rows (duplicate).
Rollback → the second one proceeds.

**Caveat: real emails.** Here "sending" means storing a row, so it fits in the transaction. A real
email/SMS API call can't be rolled back. Two common options (not built here):
1. commit first, then call the provider with an **idempotency key** (= `messageId`), so a repeat call doesn't send twice;
2. write a "send this email" row to a **notification outbox** in the same transaction and let a worker call
   the provider. That's the Stage 2–3 pattern again, on the consumer side.

## Manual ack

The consumer uses `noAck: false` and calls `ack` **only after the transaction committed**.
If the process dies before that, RabbitMQ re-delivers the message, and the inbox turns that into a no-op.
So "ack late + idempotent handler" = no loss and no double effect.

## Retries and the dead-letter queue

```
orders.events ──order.created──▶ notification-service.order-created ──▶ consumer
                                       ▲               │
              expiration runs out,     │               │ nack(requeue=false): poison, or out of retries
              dead-lettered back       │               ▼
                                       │       notification-service.dlx ──▶ notification-service.order-created.dlq
                                       │
                         notification-service.order-created.retry ◀── consumer re-publishes a failed message
                         (no consumers; dead-letters to '' → main queue)
```

| What happened | What the consumer does |
|---|---|
| Handled, or a known duplicate | `ack` |
| **Poison**: not JSON, missing fields, **no `messageId`** (can't de-duplicate) | `nack` → DLX → **DLQ** right away (retrying can't fix it) |
| **Transient** failure (DB down, timeout…) | publish a copy to the **retry queue** with `x-retry-count + 1` and `expiration = CONSUMER_RETRY_DELAY_MS`, wait for the confirm, then `ack` the original |
| Still failing after `CONSUMER_MAX_RETRIES` | `nack` → DLX → **DLQ** |

Why a retry queue instead of `nack(requeue=true)`? Requeue puts the message straight back at the head of
the queue: a failing message spins in a tight loop and blocks others. The retry queue adds a delay, and the
header counts attempts so we know when to stop.

(If we crash between "copy to retry queue" and "ack original", the message exists twice. The inbox handles it.)

DLQ messages keep their history: `x-retry-count`, `x-last-error`, and RabbitMQ's `x-death` header
(which queue, why: `rejected` / `expired`, how many times). Look at them in the management UI
(Queues → `...dlq` → Get messages). To replay them after fixing the cause, use "Move messages" (shovel plugin)
or re-publish them.

Settings (all optional, see `apps/notification-service/.env.example`):

| Env var | Default | Meaning |
|---|---|---|
| `CONSUMER_PREFETCH` | `10` | Unacked messages in flight |
| `CONSUMER_MAX_RETRIES` | `3` | Retries before the DLQ (so 4 attempts in total) |
| `CONSUMER_RETRY_DELAY_MS` | `5000` | Wait in the retry queue (fixed delay; exponential needs one retry queue per delay) |
| `CONSUMER_SIMULATED_FAILURE_RATE` | `0` | Experiments: fail after the DB writes (they roll back) |

`GET localhost:3002/notifications/stats` shows the counters used below:
notifications, distinct orders notified, processed messages (inbox rows), duplicate deliveries skipped,
and the depth of the main, retry and DLQ queues.

## Experiments

Helper to publish a hand-made message through the management API:

```bash
pub() { curl -s -u outbox:outbox -XPOST localhost:15672/api/exchanges/%2F/orders.events/publish \
  -H 'content-type: application/json' -d "$1"; echo; }
```

### 1. Duplicate deliveries are skipped

Publish the **same** message (same `message_id`) 3 times:

```bash
pub '{"properties":{"message_id":"11111111-1111-1111-1111-111111111111","type":"order.created"},
      "routing_key":"order.created","payload_encoding":"string",
      "payload":"{\"orderId\":\"22222222-2222-2222-2222-222222222222\",\"customerEmail\":\"dup@example.com\",\"product\":\"Hat\",\"quantity\":1,\"totalCents\":900}"}'
```

Result: **1 notification**, `duplicateDeliveriesSkipped: 2`, log: `duplicate skipped: message ... was already handled`.

### 2. The Stage 3 `kill -9` test again → no duplicate emails

Same as Stage 3 experiment 5: API with `RELAY_ENABLED=false`, 1000 orders, `RELAY_BATCH_SIZE=1000 npm run relay`,
`kill -9` it ~0.4s in, start a relay again.

| | Stage 3 | **Stage 4** |
|---|---|---|
| Published before the kill (and re-published after) | 222 | 209 |
| Notifications for the 1000 orders | **1222** | **1000** |
| Distinct orders notified | 1000 | 1000 |
| Duplicates | 222 emails sent twice | 209 deliveries skipped by the inbox |

The relay still delivered 209 messages twice: that's the outbox's at-least-once guarantee, and it can't be
avoided. The inbox made those repeats harmless.

### 3. Poison messages → DLQ

```bash
pub '{"properties":{"message_id":"poison-1","type":"order.created"},"routing_key":"order.created","payload":"this is not json","payload_encoding":"string"}'
pub '{"properties":{"type":"order.created"},"routing_key":"order.created","payload":"{\"orderId\":\"x\"}","payload_encoding":"string"}'
```

Result: DLQ depth 0 → **2**, no retries, logs:
`poison message poison-1 → DLQ: body is not valid JSON` and
`poison message (no messageId) → DLQ: message has no messageId, so duplicates could not be detected`.
Both carry `x-death: rejected` from the main queue.

### 4. Database down while consuming → retries, then success

```bash
docker stop outbox-postgres; pub '<a valid message>'; sleep 8; docker start outbox-postgres
```

Log from our run:

```
19:07:47  message fa37… failed (connect ECONNREFUSED ::1:5432); retry 1/3 in 5000ms
19:07:52  message fa37… (retry 1) failed (...); retry 2/3 in 5000ms
19:07:57  📧 sent to dbdown@example.com ... (retry 2)
```

Exactly 1 notification, nothing in the DLQ.

### 5. Always failing → DLQ after the retries, nothing half-written

```bash
CONSUMER_SIMULATED_FAILURE_RATE=1 CONSUMER_MAX_RETRIES=2 CONSUMER_RETRY_DELAY_MS=1000 npm run dev:notification
# create one order
```

Log: `failed ...; retry 1/2` → `(retry 1) failed; retry 2/2` → `(retry 2) failed after 2 retries → DLQ`.
The order has **0 notifications and 0 inbox rows**: each attempt wrote both and rolled both back, which is
why the retries weren't mistaken for duplicates. In the DLQ the message has `x-retry-count: 2`,
`x-last-error`, and `x-death`: rejected once by the main queue, expired twice from the retry queue.

## What's still missing

- **Inbox growth**: `processed_messages` grows forever. Keep rows for longer than any possible
  re-delivery window (days), then delete old ones.
- **Ordering**: still not guaranteed (see Stage 3). Idempotency ≠ ordering.
- **DLQ handling** is manual: someone has to look at it, fix the cause, and replay.

## Next: Stage 5

Failure experiments end to end: crash each component at different points, run several relays and
several consumers, and check the invariant **every order → exactly one notification**.
