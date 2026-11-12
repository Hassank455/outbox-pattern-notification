# Stage 3: The polling relay

## What we built

The **relay** is the piece that moves events from the outbox table to RabbitMQ.
It lives in orders-service (`src/relay/`) and runs in two ways:

- **inside the API process** (on by default, `RELAY_ENABLED=false` turns it off)
- **as its own process**, with no HTTP server: `npm run relay` (needs a build, e.g. `npm run build -w orders-service`)

```
               every RELAY_INTERVAL_MS
┌──────────────────────────────────────────────────────────────────┐
│ BEGIN                                                            │
│   SELECT ... FROM outbox_messages                                │
│   WHERE status='PENDING' AND (next_attempt_at IS NULL            │
│                               OR next_attempt_at <= now())       │
│   ORDER BY created_at LIMIT 50                                   │
│   FOR UPDATE SKIP LOCKED                 ── claim a batch        │
│                                                                  │
│   for each row, oldest first:                                    │
│     publish to RabbitMQ (confirm channel, messageId = row.id)    │
│     ok   → UPDATE status='PUBLISHED', published_at=now()         │
│     fail → UPDATE attempts+1, last_error, next_attempt_at=+backoff│
│            (or status='FAILED' after RELAY_MAX_ATTEMPTS)         │
│ COMMIT                                   ── release the locks    │
└──────────────────────────────────────────────────────────────────┘
```

Files:
- `relay/outbox.relay.ts`: the loop and the batch transaction
- `relay/relay.config.ts`: env settings and the backoff formula
- `relay.main.ts` + `relay/relay-app.module.ts`: the standalone relay process
- `messaging/rabbitmq.publisher.ts`: now waits for **each** message's confirm (with a 10s timeout),
  takes the `messageId` from the caller, and reconnects after RabbitMQ restarts
- migration `20261112143158_add_outbox_next_attempt_at`: the `next_attempt_at` column + index

### Settings (all optional)

| Env var | Default | Meaning |
|---|---|---|
| `RELAY_ENABLED` | `true` | Run the relay inside the API process |
| `RELAY_INTERVAL_MS` | `1000` | Pause between ticks (skipped when the last batch was full) |
| `RELAY_BATCH_SIZE` | `50` | Max rows per transaction |
| `RELAY_MAX_ATTEMPTS` | `10` | Then the row becomes `FAILED` |
| `RELAY_BASE_BACKOFF_MS` / `RELAY_MAX_BACKOFF_MS` | `1000` / `60000` | Retry delay: 1s, 2s, 4s, 8s… capped at 60s |
| `RELAY_SIMULATED_FAILURE_RATE` | `0` | Experiments only: share of publishes that fail on purpose |
| `PRISMA_LOG_QUERIES` | `false` | Print every SQL statement (shows `COMMIT` / `ROLLBACK`; Prisma doesn't print `BEGIN`) |

## Key ideas

### The loop never overlaps itself

Each tick schedules the next one with `setTimeout` **after** it finishes. So there's never more than one batch
running inside one relay, however slow a batch is. With `setInterval`, a slow batch could overlap the next.

### Why `FOR UPDATE SKIP LOCKED`

- `FOR UPDATE` locks the claimed rows until `COMMIT`. Nobody else can claim them meanwhile.
- `SKIP LOCKED` makes another relay **skip** those rows instead of waiting for them.

Without it you'd have two bad options: plain `SELECT` (two relays pick the same rows → double publishing),
or `FOR UPDATE` without `SKIP LOCKED` (relay #2 waits for relay #1 → no parallelism).
With it, N relays split the backlog between them, and each row is in at most one batch at a time.

### Why publish while holding the locks, and the price

We publish **inside** the transaction, and only mark a row `PUBLISHED` after RabbitMQ confirms it.
The lock is what stops a second relay from sending the same row at the same moment.

But RabbitMQ and Postgres still can't commit together. If the relay dies after publishing and
before `COMMIT`, the row goes back to `PENDING` and gets published **again**. So the outbox gives:

> **at-least-once delivery**: an event is never lost, but it can arrive more than once.

The consumer has to cope with duplicates (Stage 4). Experiment 5 below shows exactly this.

Also: a long transaction holds locks and a DB connection, so batches stay small, each publish has a
10s confirm timeout, the whole transaction has a 30s timeout, and a real broker error ends the batch early.

### Broker down ≠ message failure

Before claiming rows, the relay checks it can reach RabbitMQ. If it can't, it **doesn't touch the rows**
and checks again every 5s. An outage isn't the message's fault, so it shouldn't use up its attempts.
Otherwise a 5-minute outage would push perfectly good events to `FAILED`.
`attempts`/backoff are for failures of a **specific** publish (a nack, a timeout, or the simulated failure).

### Backoff and `FAILED`

After a failed publish: `attempts + 1`, `last_error`, `next_attempt_at = now + 1s·2^(attempts-1)` (max 60s).
The claim query skips rows whose `next_attempt_at` is still in the future, so a failing message doesn't
block the rest. After `RELAY_MAX_ATTEMPTS` it becomes `FAILED` and needs a human
(`GET /outbox?status=FAILED`). To retry it: `UPDATE outbox_messages SET status='PENDING', attempts=0 WHERE id=...`.

### Ordering caveats

- One relay publishes a batch in `created_at` order.
- But a message waiting for a retry is overtaken by newer ones.
- And with several relays, batches go out in parallel, so the global order is lost.

For "order created" emails that's fine. If order matters (e.g. `order.created` before `order.cancelled`
for the **same** order), the usual fixes are: one relay, or partition by `aggregate_id` so one entity's
events always go through the same relay/queue, or let the consumer handle out-of-order events (versions).

## Experiments

Start notification-service (`npm run dev:notification`) and orders-service (`npm run dev:orders`).

```bash
J='content-type: application/json'
q() { docker exec outbox-postgres psql -U outbox -d orders_db -c "$1"; }
q "select status, count(*), max(attempts) from outbox_messages group by status"
```

### 1. The Stage 2 backlog gets delivered

The 3 `PENDING` rows from Stage 2 are published as soon as orders-service starts:
`batch: claimed 3, published 3, failed 0`. Their notifications appear in `GET localhost:3002/notifications`.
A new order now reaches notification-service in about **0.2s** (published ~0.1s after it was created).

### 2. RabbitMQ down → events wait, then flow

```bash
docker stop outbox-rabbitmq
curl -s -o /dev/null -w "%{http_code}\n" -XPOST localhost:3001/orders -H "$J" \
  -d '{"customerEmail":"down@example.com","product":"Mouse","quantity":1,"unitPriceCents":1999}'   # 201
q "select status, attempts from outbox_messages where payload->>'customerEmail'='down@example.com'"
docker start outbox-rabbitmq
```

Log: `RabbitMQ unavailable ...; events stay PENDING, checking every 5s`, then `RabbitMQ is back after 20s, resuming`.
Our run: 3 orders → `PENDING` with `attempts = 0` during the outage → all 3 `PUBLISHED` (1 attempt) within a
second of the broker coming back, 3 notifications.

### 3. Three relays in parallel → work is split, nothing is sent twice

```bash
RELAY_BATCH_SIZE=10 npm run relay   # terminal 3
RELAY_BATCH_SIZE=10 npm run relay   # terminal 4
seq 1 300 | xargs -P 20 -I{} curl -s -o /dev/null -XPOST localhost:3001/orders -H "$J" \
  -d '{"customerEmail":"bulk@example.com","product":"Cable","quantity":1,"unitPriceCents":500}'
```

Our run (API relay + 2 standalone relays): 300 rows `PUBLISHED`, all with `attempts = 1`.
The relays published **79 / 111 / 110**, and notification-service got **300 notifications for 300 distinct orders, 0 duplicates**.

### 4. Retries, backoff and `FAILED`

```bash
# every publish fails, give up after 4 attempts, start backoff at 500ms
RELAY_ENABLED=false npm run dev:orders
RELAY_SIMULATED_FAILURE_RATE=1 RELAY_MAX_ATTEMPTS=4 RELAY_BASE_BACKOFF_MS=500 npm run relay
```

The log shows `attempt 1 failed; retry in 500ms` → `1000ms` → `2000ms` → `FAILED after 4 attempts`.
The row ends as `FAILED | attempts 4 | last_error = simulated publish failure`.

With `RELAY_SIMULATED_FAILURE_RATE=0.5` and 20 orders, all 20 still end up `PUBLISHED`. `attempts`
(including the successful one) came out as: 1 → 8 rows, 2 → 6, 3 → 1, 4 → 3, 5 → 1, 6 → 1.

### 5. Kill the relay mid-batch → duplicates (at-least-once)

```bash
RELAY_ENABLED=false npm run dev:orders            # API without a relay
# create 1000 orders, then:
RELAY_BATCH_SIZE=1000 npm run relay               # kill -9 it ~0.4s after it starts
npm run relay                                     # start it again
```

Our run:

| Moment | Outbox | Notifications |
|---|---|---|
| Before | 1000 `PENDING` | 0 |
| After `kill -9` | **1000 `PENDING`** (the transaction never committed) | **222** (already published!) |
| After restart | 1000 `PUBLISHED` | **1222 for 1000 orders: 222 duplicates** |

Nothing was lost, but 222 customers got the email twice. That's the price of at-least-once, and the
reason for Stage 4.

### 6. See the SQL

```bash
PRISMA_LOG_QUERIES=true npm run dev:orders
```

A normal order prints `INSERT orders`, `INSERT outbox_messages`, `COMMIT`.
With `?fail=fail-inside-transaction` it prints the same two INSERTs and then `ROLLBACK`.

## What's still missing

- **Duplicates** (experiment 5): Stage 4 makes the consumer idempotent with an inbox table keyed by
  `messageId` (= outbox id), plus manual ack and a dead-letter queue.
- **Cleanup**: `PUBLISHED` rows stay forever. Real systems delete or archive them after some days.
- **Polling cost**: one query per second per relay even when idle. Stage 6 looks at `LISTEN/NOTIFY` and CDC (Debezium).

## Next: Stage 4

The notification consumer: an inbox (`processed_messages`) table written in the same transaction as the
notification, manual ack after commit, and a dead-letter queue for messages that keep failing.
