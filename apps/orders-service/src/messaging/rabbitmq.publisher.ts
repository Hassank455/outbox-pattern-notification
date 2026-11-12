import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ChannelModel, ConfirmChannel, connect } from 'amqplib';
import { randomUUID } from 'node:crypto';
import { ORDERS_EXCHANGE } from './messaging.constants';

const CONFIRM_TIMEOUT_MS = 10_000;

export interface PublishOptions {
  /** Defaults to a random UUID. The relay passes the outbox row id, so the consumer can de-duplicate (Stage 4). */
  messageId?: string;
  /** When the event happened. Defaults to now. */
  timestamp?: Date;
}

/**
 * Publishes events to RabbitMQ using a *confirm channel*:
 * `publish()` only resolves once the broker says it has taken the message.
 *
 * Stage 1 called this straight from the request path (the dual-write bug).
 * Since Stage 2 the request path only writes to the outbox, and since Stage 3
 * the OutboxRelay is the only caller.
 *
 * Connection handling: connects lazily, and after the connection drops
 * (broker restart, network blip) the next publish reconnects. A failed
 * connect just throws; it never crashes the app.
 */
@Injectable()
export class RabbitMqPublisher implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RabbitMqPublisher.name);
  private connection?: ChannelModel;
  private channel?: ConfirmChannel;
  private connecting?: Promise<ConfirmChannel>;

  async onModuleInit() {
    try {
      await this.getChannel();
    } catch (err) {
      // Don't block startup: orders are still accepted, events wait in the outbox.
      this.logger.warn(`RabbitMQ not reachable at startup: ${(err as Error).message}`);
    }
  }

  async onModuleDestroy() {
    await this.connection?.close().catch(() => undefined);
  }

  /** Opens the connection if needed. Throws if the broker is unreachable. */
  async ensureConnected(): Promise<void> {
    await this.getChannel();
  }

  async publish(routingKey: string, payload: unknown, options: PublishOptions = {}): Promise<string> {
    const channel = await this.getChannel();
    const messageId = options.messageId ?? randomUUID();
    const timestamp = options.timestamp ?? new Date();

    // Wait for this message's own confirm (ack/nack) from the broker, with a timeout
    // so a stuck broker can't hang the caller forever.
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`no confirm from RabbitMQ within ${CONFIRM_TIMEOUT_MS}ms`)),
        CONFIRM_TIMEOUT_MS,
      );
      channel.publish(
        ORDERS_EXCHANGE,
        routingKey,
        Buffer.from(JSON.stringify(payload)),
        {
          messageId,
          type: routingKey,
          contentType: 'application/json',
          persistent: true, // survive a broker restart (queue must be durable too)
          timestamp: Math.floor(timestamp.getTime() / 1000),
        },
        (err) => {
          clearTimeout(timer);
          if (err) reject(err instanceof Error ? err : new Error('RabbitMQ rejected (nacked) the message'));
          else resolve();
        },
      );
    });
    return messageId;
  }

  private getChannel(): Promise<ConfirmChannel> {
    if (this.channel) return Promise.resolve(this.channel);
    this.connecting ??= this.openChannel().finally(() => (this.connecting = undefined));
    return this.connecting;
  }

  private async openChannel(): Promise<ConfirmChannel> {
    const url = process.env.RABBITMQ_URL ?? 'amqp://outbox:outbox@localhost:5672';
    const connection = await connect(url);
    connection.on('error', (err) => this.logger.error(`connection error: ${err.message}`));
    connection.on('close', () => {
      this.logger.warn('connection closed; will reconnect on next publish');
      this.connection = undefined;
      this.channel = undefined;
    });

    const channel = await connection.createConfirmChannel();
    channel.on('error', (err) => this.logger.error(`channel error: ${err.message}`));
    channel.on('close', () => (this.channel = undefined));
    await channel.assertExchange(ORDERS_EXCHANGE, 'topic', { durable: true });

    this.connection = connection;
    this.channel = channel;
    this.logger.log(`connected, exchange "${ORDERS_EXCHANGE}" ready`);
    return channel;
  }
}
