# Stage 1: The naive way (dual write)

> The code for this stage is in commit `85dfd7e`. Stage 2 replaced the `?fail=` switches,
> so run `git checkout 85dfd7e` to repeat these experiments, and `git checkout main` to come back.

## What we built

**orders-service**
- `POST /orders`: validates the body, saves the order, **then** publishes `order.created` to RabbitMQ.
- `GET /orders`: the last 50 orders.
- `RabbitMqPublisher`: a confirm channel, so `publish()` only resolves once the broker has the message.
  Exchange `orders.events` (topic, durable), routing key `order.created`, persistent messages,
  a fresh `messageId` (UUID) on every message.

**notification-service**
- `RabbitMqConsumer`: declares queue `notification-service.order-created`, binds it to
  `orders.events` / `order.created`, stores a `Notification` row, then acks.
  Reconnects every 5s if RabbitMQ goes away.
- `GET /notifications`: the last 50 notifications "sent".

```
POST /orders ──▶ INSERT order (commit) ──▶ publish order.created ──▶ RabbitMQ ──▶ notification-service
                 └──────── step 1 ───────┘ └─────── step 2 ───────┘
                       nothing ties these two steps together
```

## The problem: two writes, no shared transaction

Postgres and RabbitMQ are two separate systems. There is no transaction that covers both.
So for every order there are four possible outcomes, and only one of them is right:

| Order saved? | Event published? | Result |
|:---:|:---:|---|
| ✅ | ✅ | Correct |
| ✅ | ❌ | **Lost event**: the customer never hears about their order |
| ❌ | ✅ | **Phantom event**: a confirmation email for an order that doesn't exist |
| ❌ | ❌ | Fine (the request failed cleanly) |

The code can make the window between the two steps smaller, but it can never close it.

## Experiments

Run both services (`npm run dev:orders`, `npm run dev:notification`), then:

```bash
B='{"customerEmail":"test@example.com","product":"Keyboard","quantity":2,"unitPriceCents":4999}'
```

### 1. Happy path

```bash
curl -s -XPOST localhost:3001/orders -H 'content-type: application/json' -d "$B"
curl -s localhost:3002/notifications
```

The order and its notification share the same `orderId`.

### 2. Crash between the two steps → lost event

```bash
curl -s -XPOST 'localhost:3001/orders?fail=crash-after-save' -H 'content-type: application/json' -d "$B"
```

orders-service calls `process.exit(1)` right after the INSERT commits.
Restart it (in watch mode, save any file or re-run `npm run dev:orders`), then compare
`GET /orders` with `GET /notifications`: the new order has **no** notification, and nothing
will ever send one. In real life this is a deploy, an OOM kill or a pod eviction at the wrong millisecond.

### 3. Broker down → lost event and a confused client

```bash
docker stop outbox-rabbitmq
curl -s -XPOST localhost:3001/orders -H 'content-type: application/json' -d "$B"   # 500
docker start outbox-rabbitmq
curl -s localhost:3001/orders
```

The client gets a `500 Internal server error`, but the order **was** saved.
If the client retries, you get a second order. Either way, no notification for the first one.

### 4. "Just publish first" → phantom event

```bash
curl -s -XPOST 'localhost:3001/orders?fail=publish-before-save' -H 'content-type: application/json' -d "$B"
```

Swapping the order of the steps only swaps the bug: the event goes out, then the INSERT fails
(simulated here; for real it's a constraint violation, a deadlock or a DB failover).
`GET /notifications` shows a notification whose `orderId` is not in `GET /orders`.

### 5. (Bonus) Look at RabbitMQ

Open http://localhost:15672 (outbox / outbox) → Queues → `notification-service.order-created`.
Stop notification-service, create a few orders, and watch messages pile up in "Ready".
Start it again and they drain. A durable queue protects you **after** the publish succeeds;
it does nothing for the gap before it.

## Why the obvious fixes don't work

- **Wrap both in a DB transaction**: the publish isn't part of the DB transaction. If you publish
  inside it and then the commit fails, you've sent a phantom event anyway.
- **Retry the publish**: retries help while the process is alive. A crash takes the retry loop with it.
- **Two-phase commit (XA)**: RabbitMQ doesn't support it, and 2PC brings blocking and availability
  problems we don't want.

## Takeaway

We need the "an order was created" fact to be written **in the same transaction** as the order
itself, in the same database. Something else then makes sure it reaches RabbitMQ, retrying
as long as it takes. That table is the **outbox**.

## Next: Stage 2

Add an `outbox_messages` table to `orders_db` and write the order and its outbox row in one
`prisma.$transaction`. Experiment 2 should then leave an unsent outbox row instead of a lost event.
