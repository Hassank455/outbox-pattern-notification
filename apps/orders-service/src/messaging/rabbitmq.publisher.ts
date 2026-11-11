import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ChannelModel, ConfirmChannel, connect } from 'amqplib';
import { randomUUID } from 'node:crypto';
import { ORDERS_EXCHANGE } from './messaging.constants';

/**
 * Publishes events to RabbitMQ using a *confirm channel*:
 * `publish()` only resolves once the broker says it has taken the message.
 *
 * Stage 1 called this straight from the request path (the dual-write bug).
 * Since Stage 2 nothing calls it: events wait in the outbox table, and the
 * Stage 3 relay will use this publisher to send them.
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
      // Don't block startup: the service can still take orders (and lose events, see Stage 1 docs).
      this.logger.warn(`RabbitMQ not reachable at startup: ${(err as Error).message}`);
    }
  }

  async onModuleDestroy() {
    await this.connection?.close().catch(() => undefined);
  }

  async publish(routingKey: string, payload: unknown): Promise<string> {
    const channel = await this.getChannel();
    const messageId = randomUUID();
    channel.publish(ORDERS_EXCHANGE, routingKey, Buffer.from(JSON.stringify(payload)), {
      messageId,
      type: routingKey,
      contentType: 'application/json',
      persistent: true, // survive a broker restart (queue must be durable too)
      timestamp: Math.floor(Date.now() / 1000),
    });
    await channel.waitForConfirms();
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
