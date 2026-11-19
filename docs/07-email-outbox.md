# Stage 7: The email outbox (side effects you can't roll back)

Stage 4 made the consumer idempotent: an inbox row and the notification row commit together, so a
duplicate delivery from RabbitMQ is harmless. That works because both writes live **inside our database**,
and a rollback undoes both.

A real notification service doesn't stop at a row. It sends an **email**. An email is a side effect
**outside our database**: once the provider accepts it, the customer has it, and no `ROLLBACK` can take it back.

## The problem: always start with the 💥

Calling the provider from inside the consumer transaction looks fine until you put a crash in it:

```
BEGIN
  INSERT processed_messages, notifications
  POST https://provider/v1/emails      -> accepted, the email is out
  💥 process dies
COMMIT                                  -> never happens
```

The rollback erases the inbox row, RabbitMQ redelivers, and the consumer sends **the email again**.
The inbox can't help: the proof that "this was handled" was inside the transaction that got rolled back.

Moving the send after `COMMIT` doesn't fix it either: crash after commit and before the send, and the
email is **never** sent, because the inbox already says "done".

## The fix, in two parts

### 1. Defer the side effect with a second outbox

The consumer no longer talks to the provider. In the same transaction as the inbox and notification rows,
it inserts a row into `outbound_emails`:

```ts
await tx.outboundEmail.createMany({
  data: [{ ...email, idempotencyKey: `order-confirmation:${meta.messageId}` }],
  skipDuplicates: true,
});
```

"The order was handled" and "an email must be sent" are now atomic, exactly like "order exists" and
"event must be published" in Stage 2. The key is **stable**: it comes from the RabbitMQ `messageId`, so
every redelivery of the same event gives the same key (and `skipDuplicates` keeps one row even with the inbox off).

### 2. An email worker that is at-least-once, plus an Idempotency-Key

`EmailWorker` (`src/email/email.worker.ts`) is the Stage 3 relay again, with HTTP instead of AMQP:

1. claim up to `EMAIL_WORKER_BATCH_SIZE` `PENDING` rows: `FOR UPDATE SKIP LOCKED`
2. `POST /v1/emails` with header `Idempotency-Key: <row.idempotency_key>`
3. mark `SENT`, or schedule a retry with exponential backoff, or give up (`FAILED`)
4. `COMMIT`

The 💥 window is still there, between step 2 and step 4. **We can't remove the second call. We make it harmless.**
The retry carries the same key; the provider remembers keys it has seen (SendGrid, Postmark, Resend and
Stripe all work this way) and answers with the original message id **without sending anything**.

> The proof of "already sent" must live where our rollback can't reach it: at the provider.

Why send first and mark second? The other order (mark `SENT`, then send) turns a crash into an email that
is marked sent but never left. "Maybe twice" can be fixed with a key. "Maybe never" can't.

Errors:

| Provider answer | Worker does |
|---|---|
| 2xx | `SENT`, stores `provider_message_id` (a replay is logged as such) |
| 4xx (not 408/409/429) | `FAILED` right away: a bad address won't get better with retries |
| 5xx, 408, 409, 429, timeout, network error | stays `PENDING`, `next_attempt_at = now + backoff` (1s, 2s, 4s… max 60s), `FAILED` after 8 attempts |

## The fake provider

`tools/fake-email-provider.mjs` (`npm run dev:email-provider`, port 3003) behaves like a real one but
"delivers" into memory, so we can count what customers actually received:

- new key → `202`, delivered, key remembered
- known key → `200`, original id, header `Idempotent-Replayed: true`, **nothing delivered**
- `GET /v1/stats` → requests, replays, deliveries, duplicate deliveries
- `PROVIDER_FAILURE_RATE=0.3` returns 503s to exercise retries and backoff

## Experiment: kill the worker after the send, before the commit

`npm run email-chaos` (needs Postgres and `npm run build`):

1. starts its own provider on port 3013
2. inserts 300 `PENDING` emails
3. runs the standalone worker 3 times with `EMAIL_WORKER_CRASH_AFTER_SENDS=37`: each run sends 37 emails,
   then `SIGKILL`s itself. The batch never commits, so all 300 rows stay `PENDING`
4. a healthy worker sends everything
5. compares the outbox with the provider's inboxes

**300 emails, 3 crashes after 37 sends each:**

| | `--no-key` | with `Idempotency-Key` (default) |
|---|---|---|
| rows `SENT` | 300 | 300 |
| send requests to the provider | 411 | 411 |
| replays recognised by the provider | 0 | **111** |
| emails customers received | **411** | **300** |
| duplicate copies | **111** (37 customers got 4 copies) | **0** |

Same crashes, same number of HTTP calls. The only difference is one header. The script exits non-zero
when the key is on and a duplicate shows up.

## Running it

```bash
npm run dev:email-provider          # terminal 1: fake provider on :3003
npm run dev:notification            # the worker runs inside notification-service
curl localhost:3002/emails/stats    # { PENDING, SENT, FAILED }
curl localhost:3003/v1/emails       # what customers received

npm run email-worker                # extra standalone workers: SKIP LOCKED splits the rows
npm run email-chaos                 # the experiment above
npm run email-chaos -- --no-key     # the control run that fails
```

## Limits worth knowing

- **Keys expire at the provider** (about 24h for most). A row stuck `PENDING` longer than that could
  still be sent twice. `FAILED` after 8 attempts (about 2 minutes of backoff) keeps us far inside that window.
- **Not every provider supports idempotency keys.** Without one you're back to "maybe twice"; the outbox
  still guarantees "never zero", and you pick which failure you prefer.
- **The batch holds row locks during HTTP calls**, like the Stage 3 relay. Fine at this size; at scale you'd
  claim rows with a lease (`locked_until`) and send outside the transaction.

## What I learned

1. **The inbox only protects writes inside the same database.** An email, a payment or a webhook escapes the rollback.
2. **Never call an external side effect from inside a transaction you might roll back.** Write the intention
   to an outbox and let a worker do it.
3. **The worker is at-least-once, so the side effect must be idempotent at its destination.** A stable
   `Idempotency-Key` moves the "already done" proof to the provider.
4. **Measure it.** Same crashes, same 411 calls: 111 duplicate emails without the key, zero with it.
