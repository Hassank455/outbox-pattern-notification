# Stage 0: Setup

## What we built

- `docker-compose.yml` with PostgreSQL 16 and RabbitMQ 3.13 (with the management UI).
- One Postgres server, **two databases**: `orders_db` and `notifications_db`.
  In microservices each service owns its data, so the notification service can't "peek" into orders.
- An npm workspaces monorepo with two NestJS apps.
- Prisma 7 in each service (`prisma.config.ts` + the `@prisma/adapter-pg` driver adapter).
- A `/health` endpoint in each service that runs `SELECT 1` against its own database.

## Data models so far

- `orders-service`: `Order` (id, customer_email, product, quantity, total_cents, status).
- `notification-service`: `Notification` (a log of what we sent to whom).

## Things to try

1. `npm run infra:up`, then open the RabbitMQ UI and look around (Exchanges, Queues).
2. `npm run prisma:studio -w orders-service` to browse the database.
3. Stop Postgres (`docker stop outbox-postgres`) and call `/health`. What happens?

## Next: Stage 1

We'll add `POST /orders` and publish `order.created` **directly** to RabbitMQ after saving,
then deliberately crash between the two steps to see why we need the outbox.
