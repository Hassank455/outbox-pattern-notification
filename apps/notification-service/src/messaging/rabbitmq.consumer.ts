import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { ChannelModel, ConfirmChannel, ConsumeMessage, connect } from 'amqplib';
import { NotificationsService } from '../notifications/notifications.service';
import { parseOrderCreated, PoisonMessageError } from '../notifications/order-created.event';
import { loadConsumerConfig } from './consumer.config';
import {
  DEAD_LETTER_EXCHANGE,
  DEAD_LETTER_QUEUE,
  LAST_ERROR_HEADER,
  ORDER_CREATED_QUEUE,
  ORDER_CREATED_ROUTING_KEY,
  ORDERS_EXCHANGE,
  RETRY_COUNT_HEADER,
  RETRY_QUEUE,
} from './messaging.constants';

const RECONNECT_DELAY_MS = 5_000;

/**
 * The event id used for de-duplication. Our relay sets the AMQP `messageId` property.
 * Debezium's outbox router (Stage 6, CDC) puts it in an `id` header instead, sometimes
 * JSON-encoded ("\"…\""), so fall back to that.
 */
function eventId(msg: ConsumeMessage): string | undefined {
  if (msg.properties.messageId) return String(msg.properties.messageId);
  let id: unknown = msg.properties.headers?.id;
  if (Buffer.isBuffer(id)) id = id.toString();
  if (typeof id !== 'string' || !id) return undefined;
  if (id.startsWith('"')) {
    try {
      return String(JSON.parse(id));
    } catch {
      return id;
    }
  }
  return id;
}

/** Some driver errors (e.g. ECONNREFUSED wrapped in AggregateError) have an empty message. */
function describeError(err: unknown): string {
  if (!(err instanceof Error)) return String(err);
  if (err.message) return err.message;
  const { code, cause } = err as { code?: string; cause?: unknown };
  if (err instanceof AggregateError && err.errors.length) return describeError(err.errors[0]);
  if (cause) return describeError(cause);
  return code ?? err.name;
}

/**
 * Consumes `order.created` (Stage 4: idempotent, manual ack, retries, DLQ).
 *
 * For every message:
 *   - no messageId / not parseable      -> poison: nack -> dead-letter exchange -> DLQ
 *   - handled (or a known duplicate)    -> ack, but only AFTER the DB transaction committed
 *   - failed (DB down, timeout, ...)    -> copy to the retry queue with retry-count + 1, ack the original;
 *                                          after CONSUMER_MAX_RETRIES retries -> nack -> DLQ
 *
 * If we crash before acking, RabbitMQ re-delivers the message. That's fine: the inbox makes
 * a second delivery harmless. Every "maybe twice" in this file relies on that.
 */
@Injectable()
export class RabbitMqConsumer implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(RabbitMqConsumer.name);
  private readonly config = loadConsumerConfig();
  private connection?: ChannelModel;
  private reconnectTimer?: NodeJS.Timeout;
  private shuttingDown = false;

  constructor(private readonly notifications: NotificationsService) {}

  async onApplicationBootstrap() {
    await this.start();
  }

  async onModuleDestroy() {
    this.shuttingDown = true;
    clearTimeout(this.reconnectTimer);
    // Unacked messages go back to the queue when the connection closes; the inbox handles the re-delivery.
    await this.connection?.close().catch(() => undefined);
  }

  /** Message counts per queue, for GET /notifications/stats. Uses a throwaway channel. */
  async queueDepths(): Promise<Record<string, number | null>> {
    const result: Record<string, number | null> = {};
    for (const queue of [ORDER_CREATED_QUEUE, RETRY_QUEUE, DEAD_LETTER_QUEUE]) {
      result[queue] = null;
      if (!this.connection) continue;
      const channel = await this.connection.createChannel().catch(() => undefined);
      if (!channel) continue;
      channel.on('error', () => undefined); // a missing queue closes this channel only
      result[queue] = await channel
        .checkQueue(queue)
        .then((q) => q.messageCount)
        .catch(() => null);
      await channel.close().catch(() => undefined);
    }
    return result;
  }

  private async start() {
    try {
      const url = process.env.RABBITMQ_URL ?? 'amqp://outbox:outbox@localhost:5672';
      const connection = await connect(url);
      connection.on('error', (err) => this.logger.error(`connection error: ${err.message}`));
      connection.on('close', () => {
        this.connection = undefined;
        this.scheduleReconnect('connection closed');
      });
      this.connection = connection;

      // A confirm channel, so re-publishing to the retry queue is confirmed before we ack the original.
      const channel = await connection.createConfirmChannel();
      channel.on('error', (err) => this.logger.error(`channel error: ${err.message}`));
      await this.declareTopology(channel);
      await channel.prefetch(this.config.prefetch);
      // noAck: false = manual ack. RabbitMQ keeps the message until we ack/nack it.
      await channel.consume(
        ORDER_CREATED_QUEUE,
        (msg) => {
          if (msg) void this.handle(channel, msg);
        },
        { noAck: false },
      );
      const { maxRetries, retryDelayMs, prefetch } = this.config;
      this.logger.log(
        `consuming "${ORDER_CREATED_QUEUE}" (prefetch ${prefetch}, ${maxRetries} retries every ${retryDelayMs}ms, then ${DEAD_LETTER_QUEUE})`,
      );
    } catch (err) {
      // Don't leak a half-set-up connection: close it before the next attempt opens a new one.
      const half = this.connection;
      this.connection = undefined;
      half?.removeAllListeners('close');
      await half?.close().catch(() => undefined);

      const message = (err as Error).message;
      if (message.includes('PRECONDITION_FAILED')) {
        // A queue already exists with different arguments (e.g. created by Stage 1-3 code).
        this.logger.error(
          `${message}\n  → An old queue has different settings. Delete it once (it must be empty) and restart:\n` +
            `    docker exec outbox-rabbitmq rabbitmqctl delete_queue ${ORDER_CREATED_QUEUE}`,
        );
      }
      this.scheduleReconnect(message);
    }
  }

  private async declareTopology(channel: ConfirmChannel) {
    // Shared exchange (orders-service publishes here).
    await channel.assertExchange(ORDERS_EXCHANGE, 'topic', { durable: true });

    // Dead letters: messages we give up on end up in the DLQ for a human to inspect.
    await channel.assertExchange(DEAD_LETTER_EXCHANGE, 'direct', { durable: true });
    await channel.assertQueue(DEAD_LETTER_QUEUE, { durable: true });
    await channel.bindQueue(DEAD_LETTER_QUEUE, DEAD_LETTER_EXCHANGE, DEAD_LETTER_QUEUE);

    // Retry queue: nobody consumes it. Each copy carries an `expiration`; when it runs out,
    // RabbitMQ dead-letters it to the default exchange ('') with the main queue as routing key,
    // i.e. straight back into the main queue.
    await channel.assertQueue(RETRY_QUEUE, {
      durable: true,
      deadLetterExchange: '',
      deadLetterRoutingKey: ORDER_CREATED_QUEUE,
    });

    // Main queue: a nack(requeue=false) sends the message to the dead-letter exchange -> DLQ.
    await channel.assertQueue(ORDER_CREATED_QUEUE, {
      durable: true,
      deadLetterExchange: DEAD_LETTER_EXCHANGE,
      deadLetterRoutingKey: DEAD_LETTER_QUEUE,
    });
    await channel.bindQueue(ORDER_CREATED_QUEUE, ORDERS_EXCHANGE, ORDER_CREATED_ROUTING_KEY);
  }

  private scheduleReconnect(reason: string) {
    if (this.shuttingDown || this.reconnectTimer) return;
    this.logger.warn(`RabbitMQ unavailable (${reason.split('\n')[0]}); retrying in ${RECONNECT_DELAY_MS / 1000}s`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.start();
    }, RECONNECT_DELAY_MS);
  }

  private async handle(channel: ConfirmChannel, msg: ConsumeMessage) {
    const receivedAt = new Date();
    const messageId = eventId(msg);
    const retries = Number(msg.properties.headers?.[RETRY_COUNT_HEADER] ?? 0);
    const label = `${messageId ?? '(no messageId)'}${retries ? ` (retry ${retries})` : ''}${msg.fields.redelivered ? ' [redelivered]' : ''}`;

    try {
      if (!messageId) throw new PoisonMessageError('message has no messageId, so duplicates could not be detected');
      const event = parseOrderCreated(msg.content);

      const result = await this.notifications.handleOrderCreated(event, {
        messageId,
        messageType: msg.properties.type ?? msg.fields.routingKey,
        receivedAt,
      });

      channel.ack(msg); // only now: the transaction has committed
      if (result === 'duplicate') {
        this.logger.warn(`duplicate skipped: message ${label}, order ${event.orderId} was already handled`);
      } else {
        this.logger.log(`📧 sent to ${event.customerEmail} for order ${event.orderId} (message ${label})`);
      }
    } catch (err) {
      const reason = describeError(err);
      try {
        if (err instanceof PoisonMessageError) {
          this.logger.error(`poison message ${label} → DLQ: ${reason}`);
          channel.nack(msg, false, false);
        } else if (retries < this.config.maxRetries) {
          await this.sendToRetryQueue(channel, msg, retries + 1, reason);
          channel.ack(msg);
          this.logger.warn(
            `message ${label} failed (${reason}); retry ${retries + 1}/${this.config.maxRetries} in ${this.config.retryDelayMs}ms`,
          );
        } else {
          this.logger.error(`message ${label} failed after ${retries} retries → DLQ: ${reason}`);
          channel.nack(msg, false, false);
        }
      } catch (routingErr) {
        // Couldn't even route it (channel closed?). Leave it unacked: RabbitMQ re-delivers it.
        this.logger.error(`could not route failed message ${label}: ${(routingErr as Error).message}`);
      }
    }
  }

  /** Publishes a copy to the retry queue and waits for the broker's confirm. */
  private sendToRetryQueue(channel: ConfirmChannel, msg: ConsumeMessage, retryCount: number, reason: string) {
    return new Promise<void>((resolve, reject) => {
      channel.sendToQueue(
        RETRY_QUEUE,
        msg.content,
        {
          messageId: msg.properties.messageId,
          type: msg.properties.type,
          contentType: msg.properties.contentType,
          timestamp: msg.properties.timestamp,
          persistent: true,
          expiration: String(this.config.retryDelayMs), // how long it waits before going back
          headers: { ...msg.properties.headers, [RETRY_COUNT_HEADER]: retryCount, [LAST_ERROR_HEADER]: reason.slice(0, 500) },
        },
        (err) => (err ? reject(err instanceof Error ? err : new Error('retry publish was nacked')) : resolve()),
      );
    });
  }
}
