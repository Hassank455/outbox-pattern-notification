# Stage 5: End-to-end failure experiments

Stages 1–4 tested one failure at a time. Here we break **everything at once**, repeatedly, and check one rule:

> **Every order that exists gets exactly one notification.** None missing, none twice.

## The tools

### `npm run chaos`: the experiment

`scripts/chaos.mjs` starts its **own** processes, separate from your `npm run dev:*` ones (stop `dev:orders` first, it needs port 3001):

| Process | How many | Notes |
|---|---|---|
| `api` (orders-service) | 1 | port 3001, `RELAY_ENABLED=false` |
| `relay-N` (`dist/relay.main.js`) | 3 (`--relays`) | batch 20 (`--relay-batch`), every 200ms |
| `consumer-N` (notification-service) | 2 (`--consumers`) | ports 3101+, 5 retries × 3s, so the Postgres stop doesn't push messages to the DLQ |

Then it:
1. sends `--orders` orders at `--rate` per second, all tagged with a run email `chaos-<runId>@example.com`
   (verification only looks at that run's rows, so old data doesn't matter);
2. meanwhile, every ~`--kill-every` ms, **`kill -9`s a random relay or consumer** (and the API with `--kill-api`),
   restarting it 0.3–1.5s later;
3. **restarts RabbitMQ** once at 40% of the load (`--skip-broker-restart` to turn off);
4. **stops Postgres for 4s** at 70% (`--postgres-stop`);
5. waits until the outbox has no `PENDING` rows, the main and retry queues are empty, and the notification count is stable;
6. checks the invariants with SQL across both databases and prints a PASS/FAIL report.

Logs of every process: `.chaos/<runId>/*.log` (git-ignored).

```bash
npm run chaos                                     # 500 orders, defaults
npm run chaos -- --orders=2000 --postgres-stop
npm run chaos -- --orders=2000 --rate=200 --relay-batch=500 --kill-every=500   # aggressive
npm run chaos -- ... --unsafe-consumer            # control: consumers WITHOUT the inbox
```

The checks:

| # | Check | Proves |
|---|---|---|
| 1 | every order has exactly one outbox row | the order and its event are written atomically (Stage 2) |
| 2 | every outbox row is `PUBLISHED` | the relay eventually delivers everything (Stage 3) |
| 3a | no order is missing its notification | **no lost events**, end to end |
| 3b | no order has more than one notification | **no double processing** (Stage 4) |
| 4 | duplicates delivered but skipped by the inbox (info) | at-least-once really happened, and was absorbed |
| 5 | no message from this run in the DLQ | nothing was given up on |

### `npm run trace -- <orderId>`: one order's journey

Follows one order across both databases (no id = most recent order):

```
order ebdefc03-e8d3-4780-9a6c-6043fe65294b
  1. order saved        15:45:56.640Z  chaos-...@example.com, 1 x Item 1538
  2. outbox row         15:45:56.641Z  +1ms  id 821ce469-...  order.created
     relay              status PUBLISHED, attempts 1, published 15:45:59.936Z  +3296ms
  3. inbox row          15:45:59.936Z  +3296ms  message 821ce469-..., duplicate deliveries skipped: 1
  4. notification       15:45:59.936Z  +3296ms  to chaos-...@example.com: "Order confirmed: Item 1538"
```

This order was delivered **twice** (`duplicate deliveries skipped: 1`) during the chaos, and still has exactly one notification.

## Results (on the dev Mac)

| Run | Settings | Chaos | Orders in DB | Notifications | Missing | Duplicate notifications | Repeats skipped by inbox | Result |
|---|---|---|---|---|---|---|---|---|
| A | 500 @100/s, `--postgres-stop` | 13 × kill -9, RabbitMQ restart, Postgres 4s | 331 | 331 | 0 | 0 | 8 | ✅* |
| B | 2000 @100/s, `--postgres-stop` | 24 × kill -9, RabbitMQ restart, Postgres 4s | 1607 | 1607 | 0 | 0 | 21 | ✅ PASS |
| C | 1000 @100/s, **inbox off** | 14 × kill -9, RabbitMQ restart | 1000 | 1000 | 0 | 0 | — | ✅ (lucky, see below) |
| D | 2000 @200/s, batch 500, kill every 500ms, **inbox off** | 28 × kill -9, RabbitMQ restart | 2000 | **2423** | 0 | **298 orders** | — | ❌ **FAIL** |
| E | same as D, **inbox on** | 30 × kill -9, RabbitMQ restart | 2000 | 2000 | 0 | 0 | **61** | ✅ PASS |
| F | same as D + `--kill-api --postgres-stop` | 27 × kill -9 (6 × API), RabbitMQ restart, Postgres 4s | 389 | 389 | 0 | 0 | 0 | ✅ PASS |

\* Run A's checks 1–3 passed. Check 5 then compared DLQ **counts** and failed because the count went *down* (5 → 1):
the RabbitMQ restart wiped older hand-made test messages that weren't persistent. Check 5 now looks
**inside** the DLQ for this run's messages instead; runs B–F use the fixed version.

### What the runs teach

**Runs A, B: real outages don't break the rule.** Requests failed while Postgres was down (169 and 393 client errors),
but every order that *was* created got exactly one notification. Client 201s = orders in the DB: no "ghost" orders
and no lost ones. That's the Stage 2 transaction: a failed request writes nothing.

**Run C → D: the check must be able to fail.** With the inbox off and gentle settings (C), no duplicates happened to
occur, so the run passed by luck. Duplicates need a crash in a tiny window: a relay killed **mid-batch**
(after publishing, before `COMMIT`) or a consumer killed **after commit, before ack**. Bigger relay batches and more
frequent kills (D) make that window likely, and the same check then caught **423 extra notifications on 298 orders**.
A test that can't fail proves nothing; D shows this one can.

**Run D vs E: the inbox is what makes it correct.** Identical chaos. Without the inbox: 423 duplicate emails.
With it: 61 duplicate deliveries arrived and were all skipped, so 2000 orders → 2000 notifications.

**Run F: even killing the API is safe.** The API was killed 6 times mid-request, so most requests failed
(1611). Still: 389 orders in the DB, 389 outbox rows, 389 notifications. A request either committed both
rows or neither.

**"failed publish attempts recorded" is ~0, even though relays were killed mid-batch.** A relay that dies before
`COMMIT` leaves no trace in the outbox: the rollback erases its updates, so the row just looks `PENDING` again.
The re-sends only show up on the consumer side, as duplicates skipped by the inbox.

## What outbox + inbox guarantee, and what they don't

| | Guaranteed? | Why |
|---|---|---|
| No lost events (order exists ⇒ event is eventually published) | ✅ | Order + outbox row in one transaction; relay retries until `PUBLISHED` |
| No phantom events (event ⇒ order exists) | ✅ | The outbox row only exists if the order's transaction committed |
| Effectively-once **processing** (one notification per order) | ✅ | Inbox row + notification in one transaction, ack after commit |
| Survives crashes of API / relay / consumer, broker restarts, short DB outages | ✅ | Runs A–F |
| Exactly-once **delivery** | ❌ | Impossible across Postgres + RabbitMQ; duplicates happen (8–61 per run) and are absorbed |
| Global ordering across relays | ❌ | Batches go out in parallel; retries overtake (see Stage 3) |
| Low latency under load | ❌ by design | Polling adds up to `RELAY_INTERVAL_MS`; backlog adds more (e.g. +3.3s in the trace above) |
| External side effects (a real email API) | ❌ not by itself | Can't roll back an HTTP call: use an idempotency key, or a notification outbox (Stage 4 notes) |
| Long outages (longer than the consumer's retry window) | ⚠️ | Messages go to the DLQ and need a human to replay them. Nothing is lost, but it's not automatic |

## Next: Stage 6 (optional)

Alternative relays: Postgres `LISTEN/NOTIFY` to wake the relay instantly instead of polling, and CDC
(Debezium) to read the outbox from the Postgres WAL with no polling at all.
