# Outbox Pattern with Notifications

A hands-on learning project for the **Transactional Outbox Pattern**, built step by step with
**NestJS**, **Prisma**, **PostgreSQL** and **RabbitMQ**.

The scenario: `orders-service` creates an order and must reliably tell `notification-service`
to notify the customer, without losing or inventing events, and without distributed transactions.

```
┌────────────────┐   1 tx: order + outbox row   ┌──────────────┐
│ orders-service │ ───────────────────────────▶ │  orders_db   │
└────────────────┘                              │  (outbox)    │
        ▲                                       └──────┬───────┘
        │ HTTP                                         │ 2. relay reads unsent rows
        │                                       ┌──────▼───────┐
     client                                     │ message relay│
                                                └──────┬───────┘
                                                       │ 3. publish
                                                ┌──────▼───────┐
                                                │   RabbitMQ   │
                                                └──────┬───────┘
                                                       │ 4. consume (idempotent)
                                          ┌────────────▼─────────┐     ┌──────────────────┐
                                          │ notification-service │ ──▶ │ notifications_db │
                                          └──────────────────────┘     │ (inbox)          │
                                                                       └──────────────────┘
```

## Roadmap

| Stage | Goal | Status |
|------:|------|--------|
| 0 | Project setup: Docker (Postgres + RabbitMQ), two NestJS services, Prisma | ✅ |
| 1 | The naive way: save order, then publish directly. Break it on purpose | ✅ |
| 2 | Outbox table, written in the same transaction as the order | ✅ |
| 3 | Polling relay with `FOR UPDATE SKIP LOCKED`, batching, retries with backoff | ✅ |
| 4 | Notification consumer: inbox table for idempotency, manual ack, DLQ | ✅ |
| 5 | Failure experiments: crashes, duplicates, multiple relay instances | ✅ |
| 6 | Alternative relays: Postgres `LISTEN/NOTIFY` (`RELAY_MODE=notify`), Debezium CDC (experimental) | ✅ |

Notes for each stage live in [`docs/`](./docs).

## Project structure

```
.
├── docker-compose.yml          # Postgres 16 (wal_level=logical) + RabbitMQ 3.13 (management UI) [+ Debezium, profile cdc]
├── docker/postgres/init.sql    # creates notifications_db
├── apps/
│   ├── orders-service/         # port 3001, owns orders_db
│   └── notification-service/   # port 3002, owns notifications_db
├── docker/debezium/            # Debezium Server config (CDC relay, Stage 6, `--profile cdc`)
├── scripts/                    # chaos.mjs (failure experiment), trace.mjs (one order's journey), latency.mjs (relay modes)
└── docs/                       # learning notes per stage
```

Each service owns its own database. They never read each other's tables; they only talk through RabbitMQ.

## Getting started

Requirements: Node.js 20+, Docker.

```bash
# 1. start infrastructure
npm run infra:up

# 2. install dependencies
npm install

# 3. env files
cp apps/orders-service/.env.example apps/orders-service/.env
cp apps/notification-service/.env.example apps/notification-service/.env

# 4. generate Prisma clients and apply migrations
npm run prisma:generate
npm run prisma:migrate

# 5. run the services (two terminals)
npm run dev:orders
npm run dev:notification

# optional: extra outbox relays as separate processes (Stage 3)
npm run relay

# Stage 5: chaos experiment (stop dev:orders first) and one order's journey
npm run chaos
npm run trace -- <orderId>

# Stage 6: wake the relay with LISTEN/NOTIFY instead of polling, and compare latency
RELAY_MODE=notify npm run dev:orders
npm run latency
```

Check it works:

```bash
curl http://localhost:3001/health   # orders-service
curl http://localhost:3002/health   # notification-service
```

RabbitMQ UI: http://localhost:15672 (user `outbox`, password `outbox`).

## What I learned

1. **A dual write can't be made safe by ordering or retries.** Saving to Postgres and then publishing to RabbitMQ
   (or the reverse) loses or invents events at the first crash in between (Stage 1).
2. **The outbox turns two systems into one transaction.** Writing the event to a table in the same transaction as
   the order makes "order exists" and "event must be sent" atomic. Sending becomes a separate, retryable job (Stage 2).
3. **`FOR UPDATE SKIP LOCKED` makes a table a work queue.** Several relays split the backlog without
   double-publishing or waiting on each other (Stage 3).
4. **The outbox gives at-least-once, never exactly-once.** A relay that dies between "published" and "COMMIT"
   re-sends. We measured it: 222 duplicates from one `kill -9` (Stage 3).
5. **So the consumer must be idempotent.** An inbox row keyed by the event id, written in the same transaction as
   the side effect, plus ack-after-commit, turns duplicates into no-ops: 1000 orders → 1000 notifications despite
   209 re-deliveries (Stage 4).
6. **Failures need a path, not just a log line.** Separate poison from transient errors, retry with a delay,
   then a dead-letter queue a human can look at (Stage 4).
7. **Test the invariant, under chaos, and prove the test can fail.** Random `kill -9`s, broker restarts and DB
   outages all passed "every order → exactly one notification". Turning the inbox off made the same check fail
   with 423 duplicates (Stage 5).
8. **The relay mechanism is swappable, the guarantees aren't.** LISTEN/NOTIFY cut latency from ~610ms to ~15ms and
   idle queries from 62/min to 3/min, but needs a fallback poll because NOTIFY isn't durable. CDC removes the relay
   but adds infrastructure to watch. Delivery stays at-least-once either way (Stage 6).
9. **What the pattern doesn't give you:** global ordering across relays, exactly-once delivery, or safe external
   side effects (a real email API needs its own idempotency key).

## References

- [Pattern: Transactional outbox (microservices.io)](https://microservices.io/patterns/data/transactional-outbox.html)
- [Implementing the Outbox Pattern with NestJS, RabbitMQ and Postgres (Medium)](https://medium.com/@sebastian.iwanczyszyn/implementing-the-outbox-pattern-in-distributed-systems-with-nestjs-rabbitmq-and-postgres-65fcdb593f9b)
