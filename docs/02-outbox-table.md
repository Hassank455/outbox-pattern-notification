# Stage 2: The outbox table

## What changed

`POST /orders` no longer talks to RabbitMQ. It writes two rows **in one database transaction**:

```
BEGIN
  INSERT INTO orders          (...)                         -- the business change
  INSERT INTO outbox_messages (..., 'order.created', PENDING) -- "this event happened, send it"
COMMIT
```

```ts
await this.prisma.$transaction(async (tx) => {
  const order = await tx.order.create({ ... });
  await this.outbox.enqueue(tx, { eventType: 'order.created', payload: ..., ... });
});
```

New in orders-service:
- `OutboxMessage` model + migration `20261111174950_add_outbox_messages`
- `OutboxService.enqueue(tx, message)`: takes the **transaction client** so it can't be called outside the transaction by accident
- `GET /outbox?status=PENDING|PUBLISHED|FAILED`: a read-only view of the table

`RabbitMqPublisher` is still there but nothing calls it. The relay will use it in Stage 3.

## Why one transaction fixes the dual write

In Stage 1 we wrote to **two systems** (Postgres, then RabbitMQ) and there was no way to make that atomic.
Now we write to **one system** twice, inside one transaction. Postgres guarantees atomicity:
after the transaction, either both rows exist or neither does. There is no window in between.

So "the order exists" and "the event must be sent" can't disagree anymore. Sending is now a separate job
that reads committed rows from the outbox and can retry for as long as it needs. That's Stage 3.

## The schema, column by column

| Column | Why it exists |
|---|---|
| `id` (uuid) | The event's identity. It will be sent as the AMQP `messageId`, so the consumer can spot duplicates (Stage 4). |
| `aggregate_type`, `aggregate_id` | Which entity the event is about (`Order`, `<order id>`). Useful for debugging, and for ordering per entity later. |
| `event_type` | What happened (`order.created`). |
| `routing_key` | Where the relay should publish it. The relay stays generic and doesn't need to know about orders. |
| `payload` (jsonb) | The event body, **frozen at the moment it happened**. The relay doesn't re-read the order, which might have changed by then. |
| `status` | `PENDING` → `PUBLISHED`, or `FAILED` after too many attempts. |
| `attempts`, `last_error` | Retry bookkeeping for the relay (Stage 3): how many tries, and why the last one failed. |
| `created_at` | The relay sends the oldest first. |
| `published_at` | When it actually left. `published_at - created_at` = delivery lag. |
| index `(status, created_at)` | Makes the relay's query fast: `WHERE status = 'PENDING' ORDER BY created_at LIMIT n`. |

## Experiments

```bash
B='{"customerEmail":"test@example.com","product":"Keyboard","quantity":2,"unitPriceCents":4999}'
J='content-type: application/json'
```

Count rows at any point with:

```bash
docker exec outbox-postgres psql -U outbox -d orders_db -c \
  "select (select count(*) from orders) orders, (select count(*) from outbox_messages) outbox"
```

### 1. Happy path

```bash
curl -s -XPOST localhost:3001/orders -H "$J" -d "$B"
curl -s 'localhost:3001/outbox?status=PENDING'
```

One order, one `PENDING` outbox row with the same `aggregateId`. No notification yet: nobody publishes.

### 2. Failure inside the transaction → both roll back

```bash
curl -s -XPOST 'localhost:3001/orders?fail=fail-inside-transaction' -H "$J" -d "$B"   # 500
```

Both INSERTs run, then the code throws before `COMMIT`. The log prints the order id and outbox id that
were written. Neither exists afterwards: the counts don't change.

### 3. Crash right after commit → nothing lost (compare with Stage 1, experiment 2)

```bash
curl -s -XPOST 'localhost:3001/orders?fail=crash-after-commit' -H "$J" -d "$B"
```

Same crash as Stage 1, at the same point. This time the event is sitting in the outbox as `PENDING`,
and the relay will send it when it runs. Restart orders-service afterwards.

### 4. RabbitMQ down → the request still succeeds

```bash
docker stop outbox-rabbitmq
curl -s -o /dev/null -w "%{http_code}\n" -XPOST localhost:3001/orders -H "$J" -d "$B"   # 201
docker start outbox-rabbitmq
```

In Stage 1 this was a 500 and a lost event. Now the broker isn't on the request path at all:
the order is created and its event waits in the outbox.

### Results on our machine

| Step | orders \| outbox |
|---|---|
| Before | 5 \| 0 (Stage 1 orders) |
| 1. happy path | 6 \| 1 |
| 2. fail inside transaction (500) | 6 \| 1, nothing written |
| 3. crash after commit | 7 \| 2 |
| 4. RabbitMQ stopped (201) | 8 \| 3 |

Every new order has exactly one outbox row, and the 3 rows are all `PENDING` with `attempts = 0`.

## What's still missing

- **Nobody publishes the outbox.** Notifications stopped arriving. In **Stage 3** a relay polls
  `PENDING` rows (`FOR UPDATE SKIP LOCKED`, so several relays can run safely), publishes them,
  marks them `PUBLISHED`, and retries failures with backoff.
- **Duplicates.** The relay can publish a message and crash before marking it `PUBLISHED`.
  It will publish it again: the outbox gives **at-least-once** delivery, not exactly-once.
  In **Stage 4** the consumer uses the message `id` and an inbox table to ignore repeats.
- **Table growth.** `PUBLISHED` rows pile up forever. Real systems delete or archive them after a while.

## Next: Stage 3

The polling relay: read a batch of `PENDING` rows, publish each with a confirm channel, mark it
`PUBLISHED`, retry failures with backoff, and give up (`FAILED`) after N attempts.
