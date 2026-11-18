import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { hostname } from 'node:os';
import { EmailStatus } from '../generated/prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { EmailProviderClient, PermanentEmailError } from './email-provider.client';
import { emailBackoffMs, loadEmailWorkerConfig } from './email.config';

/** Upper bound for one batch transaction (claim + send + mark). */
const TRANSACTION_TIMEOUT_MS = 60_000;

interface ClaimedEmail {
  id: string;
  idempotencyKey: string;
  recipient: string;
  subject: string;
  body: string;
  attempts: number;
}

interface BatchResult {
  claimed: number;
  sent: number;
  replayed: number;
  failed: number;
}

/**
 * STAGE 7: THE EMAIL WORKER (the "relay" of the email outbox).
 *
 * It is the Stage 3 relay again, with an HTTP email provider instead of RabbitMQ.
 * Every EMAIL_WORKER_INTERVAL_MS, in ONE transaction:
 *   1. claim up to EMAIL_WORKER_BATCH_SIZE PENDING emails   (FOR UPDATE SKIP LOCKED)
 *   2. send each one to the provider with   Idempotency-Key: <row.idempotencyKey>
 *   3. mark it SENT, or schedule a retry with backoff, or give up (FAILED)
 * COMMIT releases the row locks.
 *
 * The dangerous moment, with 💥 = the process dies:
 *
 *     provider.send(email)   -> the provider accepted it, the email is out
 *     💥
 *     UPDATE ... SET status = 'SENT'; COMMIT   -> never happens
 *
 * The row is still PENDING, so after a restart the worker sends it AGAIN (at-least-once,
 * exactly like the relay). We can't avoid the second CALL. We make it harmless: the second call
 * carries the SAME Idempotency-Key, the provider recognises it and returns the original result
 * without sending a second email. The proof of "already sent" lives at the provider, where our
 * rollback can't erase it.
 *
 * Why not mark SENT first and send after? Then a crash in between leaves a row marked SENT
 * for an email that never went out: lost. "Maybe twice" (fixable with a key) beats
 * "maybe never" (not fixable).
 */
@Injectable()
export class EmailWorker implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(EmailWorker.name);
  private readonly config = loadEmailWorkerConfig();
  private readonly instance = `${hostname()}:${process.pid}`;
  private timer?: NodeJS.Timeout;
  private currentTick?: Promise<void>;
  private stopping = false;
  private sendsThisProcess = 0;

  constructor(
    private readonly prisma: PrismaService,
    private readonly provider: EmailProviderClient,
  ) {}

  onApplicationBootstrap() {
    if (!this.config.enabled) {
      this.logger.log('email worker disabled (EMAIL_WORKER_ENABLED=false)');
      return;
    }
    const { providerUrl, intervalMs, batchSize, useIdempotencyKey, crashAfterSends } = this.config;
    this.logger.log(
      `email worker ${this.instance} started: provider ${providerUrl}, every ${intervalMs}ms, batch ${batchSize}` +
        (useIdempotencyKey ? '' : ', ⚠️ WITHOUT Idempotency-Key (experiment)') +
        (crashAfterSends ? `, will SIGKILL itself after ${crashAfterSends} sends (experiment)` : ''),
    );
    this.schedule(0);
  }

  async onModuleDestroy() {
    this.stopping = true;
    clearTimeout(this.timer);
    await this.currentTick;
  }

  private schedule(delayMs: number) {
    if (this.stopping) return;
    this.timer = setTimeout(() => {
      this.currentTick = this.tick().finally(() => (this.currentTick = undefined));
    }, delayMs);
  }

  private async tick() {
    let next = this.config.intervalMs;
    try {
      const r = await this.processBatch();
      if (r.claimed > 0) {
        this.logger.log(
          `[${this.instance}] batch: claimed ${r.claimed}, sent ${r.sent}` +
            (r.replayed ? ` (${r.replayed} were idempotent replays: provider had them already)` : '') +
            `, failed ${r.failed}`,
        );
      }
      if (r.claimed === this.config.batchSize) next = 0; // probably more waiting
    } catch (err) {
      // DB down, transaction timeout... nothing committed, the rows are still PENDING.
      this.logger.error(`email batch failed, will retry: ${(err as Error).message}`);
    }
    this.schedule(next);
  }

  private processBatch(): Promise<BatchResult> {
    return this.prisma.$transaction(
      async (tx) => {
        const rows = await tx.$queryRaw<ClaimedEmail[]>`
          SELECT id, idempotency_key AS "idempotencyKey", recipient, subject, body, attempts
          FROM outbound_emails
          WHERE status = 'PENDING'
            AND (next_attempt_at IS NULL OR next_attempt_at <= now())
          ORDER BY created_at
          LIMIT ${this.config.batchSize}
          FOR UPDATE SKIP LOCKED`;

        const result: BatchResult = { claimed: rows.length, sent: 0, replayed: 0, failed: 0 };

        for (const row of rows) {
          try {
            const sent = await this.provider.send(
              { to: row.recipient, subject: row.subject, body: row.body },
              row.idempotencyKey,
            );
            this.maybeCrash(); // experiment only: die AFTER the send, BEFORE the COMMIT

            await tx.outboundEmail.update({
              where: { id: row.id },
              data: {
                status: EmailStatus.SENT,
                sentAt: new Date(),
                providerMessageId: sent.providerMessageId,
                attempts: { increment: 1 },
                lastError: null,
                nextAttemptAt: null,
              },
            });
            result.sent++;
            if (sent.replayed) result.replayed++;
          } catch (err) {
            result.failed++;
            const permanent = err instanceof PermanentEmailError;
            const attempts = row.attempts + 1;
            const giveUp = permanent || attempts >= this.config.maxAttempts;
            const delay = emailBackoffMs(attempts, this.config);
            const message = (err as Error).message;
            await tx.outboundEmail.update({
              where: { id: row.id },
              data: {
                attempts,
                lastError: message.slice(0, 500),
                status: giveUp ? EmailStatus.FAILED : EmailStatus.PENDING,
                nextAttemptAt: giveUp ? null : new Date(Date.now() + delay),
              },
            });
            if (giveUp) {
              this.logger.error(`email ${row.id} FAILED after ${attempts} attempt(s): ${message}`);
            } else {
              this.logger.warn(`email ${row.id} attempt ${attempts} failed (${message}); retry in ${delay}ms`);
            }
          }
        }
        return result;
      },
      { timeout: TRANSACTION_TIMEOUT_MS, maxWait: 5_000 },
    );
  }

  private maybeCrash() {
    this.sendsThisProcess++;
    if (this.config.crashAfterSends && this.sendsThisProcess >= this.config.crashAfterSends) {
      this.logger.error(
        `💥 simulated crash: ${this.sendsThisProcess} emails accepted by the provider, but this batch never commits`,
      );
      process.kill(process.pid, 'SIGKILL');
    }
  }
}
