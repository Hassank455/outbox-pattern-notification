# Stage 6: Alternative relays (LISTEN/NOTIFY and Debezium CDC)

The polling relay (Stage 3) works, but it asks the database "anything new?" every second even when
nothing happens, and an event waits up to one interval before it leaves. This stage tries two ways
around that. Polling stays the default.

| `RELAY_MODE` | What runs | Wakes up when |
|---|---|---|
| `polling` (default) | `OutboxRelay` every `RELAY_INTERVAL_MS` | the timer fires |
| `notify` | `OutboxRelay` + `OutboxListener` | Postgres sends `NOTIFY outbox_channel` (+ fallback poll every 30s) |
| `none` | nothing in orders-service | (Debezium, or another process, publishes instead) |

## A) LISTEN/NOTIFY

### How it works

1. **Trigger** (migration `20261115114224_add_outbox_notify_trigger`):
   `AFTER INSERT ON outbox_messages FOR EACH ROW` → `pg_notify('outbox_channel', NEW.id::text)`.
   Only the **id** is sent, never the row.
2. **Listener** (`relay/outbox.listener.ts`): one dedicated `pg` connection runs `LISTEN outbox_channel`.
   It's not taken from Prisma's pool, because `LISTEN` belongs to a session and a pooled connection gets reused.
3. **On a notification the relay doesn't publish the payload.** It just runs the normal Stage 3 batch:
   `SELECT … FOR UPDATE SKIP LOCKED`, publish with confirms, mark `PUBLISHED`. The notification is a doorbell,
   not the parcel. So:
   - all of Stage 3 still applies: locking, several relays, retries, backoff, `FAILED`;
   - a burst of 100 NOTIFYs costs a handful of batches, not 100 publishes (notifications that arrive during a
     batch just set "run again right after").
4. **Safety nets, because NOTIFY is not durable:**
   - a notification sent while nobody is listening is simply gone;
   - so the relay sweeps at startup and after every reconnect of the LISTEN connection,
   - and polls slowly as a fallback (`RELAY_FALLBACK_INTERVAL_MS`, 30s).
5. NOTIFY is **transactional**: it's delivered only when the inserting transaction commits. A relay is never woken
   for a row it can't see yet, or for a rolled-back order.

### Results

`npm run latency` starts the API (relay off), one relay in the chosen mode and one consumer, sends orders at a steady
rate, stays idle for a while, and reads both timestamps from Postgres.

**200 orders at 20/s, then 20s idle:**

| Mode | Orders → notified | End-to-end p50 | p95 | max | Relay lag p50 | p95 | Claim queries (found nothing) |
|---|---|---|---|---|---|---|---|
| polling (1s) | 200 → 200 | 610ms | 1.05s | 1.10s | 609ms | 1.04s | 32 (21) |
| **notify** | 200 → 200 | **15ms** | **23ms** | **53ms** | 14ms | 22ms | 200 (1) |

**Idle: 1 order, then 60s of nothing:**

| Mode | Claim queries per minute | of which found nothing |
|---|---|---|
| polling (1s) | 62 | 61 |
| **notify** | **3** | 2 (startup sweep + fallback poll) |

How to read the query numbers:
- **Polling** does work by the clock: ~60 queries a minute whether or not there are orders. Under load each query
  picks up a batch, so it's cheap per event.
- **Notify** does work when there is work: ~0 when idle, and about one query per order under a slow trickle
  (each order arrives alone). Under a burst, the "run again after this batch" coalescing kicks in.

End-to-end latency drops about **40×**, and the "something new?" queries go away when the system is idle.

**Chaos with notify relays** (`npm run chaos -- --relay-mode=notify --orders=2000 --postgres-stop`):
23 × `kill -9`, a RabbitMQ restart and a 4s Postgres stop. **1603 orders → 1603 notifications, 0 missing, 0 duplicates: PASS.**
The relay logs show the LISTEN connections dropping when Postgres stopped
(`terminating connection due to administrator command`), retrying while it was down, reconnecting, and sweeping.
No events were missed.

## B) Debezium CDC (works; optional, compose profile `cdc`)

### The idea

Change Data Capture reads the **Postgres write-ahead log** (WAL): the same stream Postgres uses for replication.
Every committed INSERT into `outbox_messages` shows up there, in commit order, without anyone querying the table.
Debezium turns those log entries into messages and publishes them. No relay code, no polling, no NOTIFY.

### What's in the repo

- `docker-compose.yml`: Postgres now starts with `wal_level=logical` (needed for logical replication), plus a
  `debezium` service in the **`cdc` profile**, so it only starts when you ask for it.
- `docker/debezium/conf/application.properties`: **Debezium Server** (no Kafka needed) with:
  - the Postgres connector using `pgoutput` (built into Postgres) and a replication slot `debezium_outbox`,
    limited to `public.outbox_messages`, with `snapshot.mode=no_data` so the rows that are already published aren't re-sent;
  - the **Outbox Event Router** transform: the `payload` column becomes the message body (JSON expanded), the
    `routing_key` column becomes the topic, and the row `id` goes into an `id` header;
  - the **RabbitMQ sink**, publishing to the existing `orders.events` exchange with the topic name as routing key,
    persistent delivery.
- notification-service: if a message has no AMQP `messageId`, the consumer takes the event id from the `id` header
  (JSON-decoded if needed). So the inbox de-duplicates CDC messages exactly like relay messages.

### How to run it

```bash
docker compose up -d postgres                  # once: recreate Postgres with wal_level=logical (data is kept)
docker compose --profile cdc up -d debezium    # start CDC
RELAY_MODE=none npm run dev:orders             # no relay: Debezium publishes
npm run dev:notification
```

Stop it and **clean up the replication slot**, otherwise Postgres keeps WAL for it forever and the disk fills:

```bash
docker compose --profile cdc stop debezium
docker exec outbox-postgres psql -U outbox -d orders_db -c "select pg_drop_replication_slot('debezium_outbox')"
# optional full reset: forget Debezium's offsets too
docker compose --profile cdc rm -f debezium && docker volume rm outbox-pattern-notification_debezium-data
```

### Results on the dev Mac

It works end to end, with the relay off (`RELAY_MODE=none`) and notification-service unchanged except for the header fallback.

| Test | Result |
|---|---|
| 20 orders | 20 notifications; the 20 inbox `message_id`s are exactly the 20 outbox ids |
| 600 orders, `docker kill` (SIGKILL) Debezium mid-stream, start it again 3s later | **600 → 600 notifications**, 0 missing, 0 duplicates (it resumed from its saved offset) |
| `npm run latency -- --modes=none`, 200 orders @ 20/s, default settings | end-to-end p50 **417ms**, p95 676ms |
| same, with `debezium.source.poll.interval.ms=50` | end-to-end p50 **77ms**, p95 123ms, max 149ms |

What a Debezium message looks like in RabbitMQ (routing key `order.created`):

```
properties: delivery_mode 2, NO message_id
headers:    id = "\"cd736605-…\""   ← the outbox id, JSON-encoded
            eventType = "\"order.created\"", __debezium.context.* = …
body:       {"orderId":"86f28c11-…","product":"Peek","quantity":1,"totalCents":100,…}   ← the payload column
```

So Debezium **can't set the AMQP `messageId`**. The consumer's fallback (read the `id` header and JSON-decode it)
is what keeps the inbox working.

Getting there took three config fixes, all now in `application.properties`:
1. `route.topic.replacement=${routedByValue}` crashed Debezium Server at startup with a `NullPointerException`:
   Quarkus tries to expand `${…}` as one of its own config variables. It must be escaped: `\\${routedByValue}`.
2. `table.op.invalid.behavior` accepts only `warn | error | fatal` (`debug` failed with a `ConfigException`).
3. `routingKeyFromTopicName=true` is deprecated → `routingKey.source=topic`.

Plus one tuning fix: Debezium Server hands changes to the sink every `poll.interval.ms` (500ms by default), which
was most of the 417ms. At 50ms the latency drops to ~77ms.

After testing, Debezium was stopped, its replication slot and publication were dropped, and its offsets volume was
removed, so the dev setup is back to plain polling.

### Things to know about CDC with this schema

- **`status` stays `PENDING`.** Debezium doesn't write back to the table, so the status/attempts columns (made for
  our relay) mean nothing in CDC mode. Real CDC setups often `DELETE` the outbox row right after inserting it, in the
  same transaction: the INSERT is still in the WAL, and the table stays empty.
- **Don't run a relay at the same time** (`RELAY_MODE=none`), or every event is published twice. The inbox would absorb
  it, but it's pointless work.
- Delivery is still **at least once**: Debezium can re-send after a restart (its offset is flushed after publishing),
  so the inbox is still needed.

## Comparison

| | Polling (Stage 3) | LISTEN/NOTIFY (Stage 6A) | CDC / Debezium (Stage 6B) |
|---|---|---|---|
| Latency (e2e p50) | 610ms (up to 1 interval) | **15ms** | 417ms by default, **77ms** with `poll.interval.ms=50` |
| DB load when idle | 1 query per interval per relay (62/min) | ~0 (3/min with a 30s fallback) | none on the table; reads the WAL |
| DB load under load | one query per batch: efficient | one per wake-up; coalesces in bursts | none on the table |
| Durability of the trigger | n/a: the table *is* the queue | NOTIFY is lost if not listening → needs sweep + fallback poll | WAL + replication slot: nothing missed, even after downtime |
| Ordering | per relay, by `created_at` | same as polling | **commit order** from the log (with one Debezium task) |
| Several relays | yes (`SKIP LOCKED`) | yes (`SKIP LOCKED`, all woken) | one connector per slot (active/passive) |
| Retries / `FAILED` | built in (our code) | built in (same code) | Debezium's own retry; no per-row status |
| Moving parts | our relay | our relay + one LISTEN connection + a trigger | Debezium Server, `wal_level=logical`, a replication slot to monitor |
| Main risk | wasted queries, latency | forgetting the fallback → stuck events | abandoned slot → **WAL grows until the disk is full** |
| Use when | simple, low/moderate traffic, latency of ~1s is fine | you want near-real-time with no new infra | high volume, many tables, a team that already runs Kafka/Debezium |

## Review: the Medium article's implementation, and what we did differently

The README links to *Implementing the Outbox Pattern in Distributed Systems with NestJS, RabbitMQ and Postgres*
(Sebastian Iwanczyszyn, Apr 2025). I couldn't fetch it from here (Medium returns 403). The points below come from an earlier
reading of the article, made in this project, and weren't re-checked against the page.

| Topic | Article | This project |
|---|---|---|
| Writing the outbox | business code writes an `outbox_messages` row (`status` default `READY`) | same idea; order + outbox row in one `prisma.$transaction` (Stage 2), tested by rollback |
| Trigger | `AFTER INSERT OR UPDATE`, `pg_notify` with `row_to_json(NEW)`: the **whole row incl. payload** | `AFTER INSERT`, **id only**. NOTIFY payloads max out at 8000 bytes; a big event would make the INSERT fail |
| On notification | publishes the row from the notification payload, without `await` → no ordering, errors not handled | the notification only wakes the relay, which re-reads and **locks** rows, then publishes in order with confirms |
| After publishing | `DELETE` the row (not awaited, no error handling) | `UPDATE … PUBLISHED` in the same transaction that holds the lock |
| Several instances | no locking → each instance publishes every row | `FOR UPDATE SKIP LOCKED`; 3 relays split 300 events with 0 double-publishes (Stage 3), chaos-tested (Stage 5) |
| Missed notifications | sweep only at startup; no LISTEN reconnect; events stuck until restart | sweep at startup **and** after every reconnect, plus a 30s fallback poll; reconnect tested by stopping Postgres |
| Failures | `status = FAILED`, manual retry | attempts, backoff via `next_attempt_at`, `FAILED` after N; broker outages don't count as attempts |
| Broker connection | new channel per message | one confirm channel, reconnects lazily, 10s confirm timeout |
| Consumer | not covered | inbox table, ack after commit, retry queue, DLQ (Stage 4) |
| Delivery guarantee | not discussed | at-least-once made explicit, measured (8–222 duplicates per experiment), absorbed by the inbox |
| Verification | none | chaos script with SQL invariants and a control run that's shown to fail without the inbox |

The article is a good, short intro to the idea. The gaps are the usual ones: it shows the happy path, and every
failure mode we tested in Stages 3–5 (crash mid-batch, two instances, missed notifications, duplicates) would
lose or duplicate events there.

## Takeaways

- LISTEN/NOTIFY is a cheap upgrade **on top of** polling, not a replacement: keep the table as the source of truth,
  use NOTIFY to wake up early, and keep a slow poll for whatever NOTIFY missed.
- CDC removes the relay entirely and gives commit-order streaming, but adds infrastructure that must be monitored
  (replication slots!). It pays off at scale or when you already run it.
- Whatever moves the events, delivery is at least once, so the consumer-side inbox (Stage 4) stays.
