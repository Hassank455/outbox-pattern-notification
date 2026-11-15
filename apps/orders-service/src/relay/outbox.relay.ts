import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { hostname } from 'node:os';
import { OutboxStatus } from '../generated/prisma/client';
import { RabbitMqPublisher } from '../messaging/rabbitmq.publisher';
import { PrismaService } from '../prisma/prisma.service';
import { OutboxListener } from './outbox.listener';
import { backoffMs, loadRelayConfig } from './relay.config';

/** How often to re-check a broker that is down. Rows are not touched meanwhile. */
const BROKER_RETRY_MS = 5_000;
/** Upper bound for one batch transaction (claim + publish + mark). */
const TRANSACTION_TIMEOUT_MS = 30_000;

interface ClaimedRow {
  id: string;
  routingKey: string;
  payload: unknown;
  attempts: number;
  createdAt: Date;
}

interface BatchResult {
  claimed: number;
  published: number;
  failed: number;
  stoppedEarly: boolean;
}

class SimulatedPublishFailure extends Error {}

/**
 * STAGE 3: THE POLLING RELAY (Stage 6 adds RELAY_MODE=notify).
 *
 * Loop: every RELAY_INTERVAL_MS (or when woken by NOTIFY), in ONE transaction:
 *   1. claim up to RELAY_BATCH_SIZE PENDING rows  (SELECT ... FOR UPDATE SKIP LOCKED)
 *   2. publish each one to RabbitMQ, oldest first  (confirm channel, messageId = outbox id)
 *   3. mark it PUBLISHED, or schedule a retry with backoff, or give up (FAILED)
 * COMMIT releases the row locks.
 *
 * The loop is self-scheduling (setTimeout after each tick finishes), so ticks never overlap.
 * If a tick claimed a full batch, the next one starts immediately to drain the backlog.
 */
@Injectable()
export class OutboxRelay implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(OutboxRelay.name);
  private readonly config = loadRelayConfig();
  private readonly instance = `${hostname()}:${process.pid}`;
  private timer?: NodeJS.Timeout;
  private currentTick?: Promise<void>;
  private stopping = false;
  private brokerDownSince?: Date;
  private totalPublished = 0;
  private claimQueries = 0; // how often we asked the DB for work (= DB load when idle)
  private emptyClaims = 0; // ...and found nothing
  private listener?: OutboxListener;
  private wakeRequested = false;

  constructor(
    private readonly prisma: PrismaService,
    private readonly publisher: RabbitMqPublisher,
  ) {}

  async onApplicationBootstrap() {
    const { mode, intervalMs, fallbackIntervalMs, batchSize, maxAttempts, simulatedFailureRate } = this.config;
    if (mode === 'none') {
      this.logger.log('relay disabled (RELAY_MODE=none / RELAY_ENABLED=false)');
      return;
    }
    const cadence =
      mode === 'notify' ? `on NOTIFY + fallback poll every ${fallbackIntervalMs}ms` : `polling every ${intervalMs}ms`;
    this.logger.log(
      `relay ${this.instance} started (${mode}): ${cadence}, batch ${batchSize}, max attempts ${maxAttempts}` +
        (simulatedFailureRate > 0 ? `, simulated failure rate ${simulatedFailureRate}` : ''),
    );
    if (mode === 'notify') {
      this.listener = new OutboxListener(() => this.wake(), this.logger);
      await this.listener.start(); // its first connect wakes us for a startup sweep
    } else {
      this.schedule(0);
    }
  }

  /** Graceful shutdown: no new ticks, and wait for the running batch to commit. */
  async onModuleDestroy() {
    this.stopping = true;
    clearTimeout(this.timer);
    await this.listener?.stop();
    await this.currentTick;
    if (this.config.enabled) {
      this.logger.log(
        `relay ${this.instance} stopped: ${this.claimQueries} claim queries (${this.emptyClaims} found nothing), ` +
          `${this.totalPublished} published`,
      );
    }
  }

  /**
   * notify mode: run a tick now. If one is already running, remember to run another right
   * after it (the new row may have committed after that tick's SELECT). A burst of 100
   * NOTIFYs therefore costs a handful of ticks, not 100.
   */
  private wake() {
    if (this.stopping || this.brokerDownSince) return; // the broker-retry timer is in charge then
    if (this.currentTick) {
      this.wakeRequested = true;
      return;
    }
    clearTimeout(this.timer);
    this.schedule(0);
  }

  private schedule(delayMs: number) {
    if (this.stopping) return;
    this.timer = setTimeout(() => {
      this.currentTick = this.tick().finally(() => (this.currentTick = undefined));
    }, delayMs);
  }

  private async tick() {
    // 1. Is the broker there at all? If not, don't touch any rows: an outage is not the
    //    message's fault, so it shouldn't eat into its attempts. Just wait and check again.
    try {
      await this.publisher.ensureConnected();
    } catch (err) {
      if (!this.brokerDownSince) {
        this.brokerDownSince = new Date();
        this.logger.warn(
          `RabbitMQ unavailable (${(err as Error).message}); events stay PENDING, checking every ${BROKER_RETRY_MS / 1000}s`,
        );
      }
      return this.schedule(BROKER_RETRY_MS);
    }
    if (this.brokerDownSince) {
      const seconds = Math.round((Date.now() - this.brokerDownSince.getTime()) / 1000);
      this.logger.log(`RabbitMQ is back after ${seconds}s, resuming`);
      this.brokerDownSince = undefined;
    }

    // 2. Process one batch.
    let next = this.config.mode === 'notify' ? this.config.fallbackIntervalMs : this.config.intervalMs;
    this.wakeRequested = false;
    try {
      const result = await this.processBatch();
      this.claimQueries++;
      if (result.claimed === 0) this.emptyClaims++;
      if (result.claimed > 0) {
        this.totalPublished += result.published;
        this.logger.log(
          `[${this.instance}] batch: claimed ${result.claimed}, published ${result.published}, failed ${result.failed}` +
            ` (total published by this relay: ${this.totalPublished})`,
        );
      }
      // A full batch means there is probably more waiting: go again right away.
      if (result.claimed === this.config.batchSize && !result.stoppedEarly) next = 0;
      // notify mode: a NOTIFY arrived while we were busy, go again right away.
      if (this.wakeRequested) next = 0;
    } catch (err) {
      // e.g. the database is down, or the transaction timed out. Nothing was committed,
      // so every claimed row is still PENDING and will be picked up again.
      this.logger.error(`batch failed, will retry: ${(err as Error).message}`);
    }
    this.schedule(next);
  }

  /**
   * Why publish while holding the row locks (inside the transaction)?
   *   - The lock is what stops another relay from publishing the same row at the same time.
   *   - We only mark a row PUBLISHED after RabbitMQ confirmed it.
   *
   * The trade-off: RabbitMQ and Postgres still can't commit together. If the process dies
   * (or COMMIT fails) after a publish but before COMMIT, the row goes back to PENDING and is
   * published AGAIN later. So the outbox gives AT-LEAST-ONCE delivery: never lost, but
   * sometimes duplicated. The consumer must be idempotent (Stage 4).
   *
   * Also: long transactions hold locks and a DB connection, so batches must stay small
   * and publishing must have a timeout.
   */
  private processBatch(): Promise<BatchResult> {
    return this.prisma.$transaction(
      async (tx) => {
        // SKIP LOCKED: rows another relay has already locked are skipped, not waited for.
        // So N relays split the work instead of queueing behind each other or double-sending.
        // (now() is fine here: Postgres runs in UTC, the same zone Prisma stores timestamps in.)
        const rows = await tx.$queryRaw<ClaimedRow[]>`
          SELECT id, routing_key AS "routingKey", payload, attempts, created_at AS "createdAt"
          FROM outbox_messages
          WHERE status = 'PENDING'
            AND (next_attempt_at IS NULL OR next_attempt_at <= now())
          ORDER BY created_at
          LIMIT ${this.config.batchSize}
          FOR UPDATE SKIP LOCKED`;

        const result: BatchResult = { claimed: rows.length, published: 0, failed: 0, stoppedEarly: false };

        for (const row of rows) {
          try {
            if (Math.random() < this.config.simulatedFailureRate) {
              throw new SimulatedPublishFailure('simulated publish failure (RELAY_SIMULATED_FAILURE_RATE)');
            }
            await this.publisher.publish(row.routingKey, row.payload, {
              messageId: row.id,
              timestamp: row.createdAt,
            });
            await tx.outboxMessage.update({
              where: { id: row.id },
              data: {
                status: OutboxStatus.PUBLISHED,
                publishedAt: new Date(),
                attempts: { increment: 1 },
                lastError: null,
                nextAttemptAt: null,
              },
            });
            result.published++;
          } catch (err) {
            result.failed++;
            const attempts = row.attempts + 1;
            const giveUp = attempts >= this.config.maxAttempts;
            const delay = backoffMs(attempts, this.config);
            const message = (err as Error).message;
            await tx.outboxMessage.update({
              where: { id: row.id },
              data: {
                attempts,
                lastError: message.slice(0, 500),
                status: giveUp ? OutboxStatus.FAILED : OutboxStatus.PENDING,
                nextAttemptAt: giveUp ? null : new Date(Date.now() + delay),
              },
            });
            if (giveUp) {
              this.logger.error(`message ${row.id} FAILED after ${attempts} attempts: ${message}`);
            } else {
              this.logger.warn(`message ${row.id} attempt ${attempts} failed (${message}); retry in ${delay}ms`);
            }
            // A real broker error usually hits every message in the batch. Stop here, commit what
            // we have, and let the next tick (and its connection check) sort it out.
            if (!(err instanceof SimulatedPublishFailure)) {
              result.stoppedEarly = true;
              break;
            }
          }
        }
        return result;
      },
      { timeout: TRANSACTION_TIMEOUT_MS, maxWait: 5_000 },
    );
  }
}
