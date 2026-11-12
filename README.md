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
| 4 | Notification consumer: inbox table for idempotency, manual ack, DLQ | ⏳ |
| 5 | Failure experiments: crashes, duplicates, multiple relay instances | ⏳ |
| 6 | (Optional) Alternative relays: Postgres `LISTEN/NOTIFY`, Debezium CDC | ⏳ |

Notes for each stage live in [`docs/`](./docs).

## Project structure

```
.
├── docker-compose.yml          # Postgres 16 + RabbitMQ 3.13 (management UI)
├── docker/postgres/init.sql    # creates notifications_db
├── apps/
│   ├── orders-service/         # port 3001, owns orders_db
│   └── notification-service/   # port 3002, owns notifications_db
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
```

Check it works:

```bash
curl http://localhost:3001/health   # orders-service
curl http://localhost:3002/health   # notification-service
```

RabbitMQ UI: http://localhost:15672 (user `outbox`, password `outbox`).

## References

- [Pattern: Transactional outbox (microservices.io)](https://microservices.io/patterns/data/transactional-outbox.html)
- [Implementing the Outbox Pattern with NestJS, RabbitMQ and Postgres (Medium)](https://medium.com/@sebastian.iwanczyszyn/implementing-the-outbox-pattern-in-distributed-systems-with-nestjs-rabbitmq-and-postgres-65fcdb593f9b)
