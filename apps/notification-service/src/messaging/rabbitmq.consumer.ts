import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { Channel, ChannelModel, ConsumeMessage, connect } from 'amqplib';
import { NotificationsService } from '../notifications/notifications.service';
import { OrderCreatedEvent } from '../notifications/order-created.event';
import { ORDER_CREATED_QUEUE, ORDER_CREATED_ROUTING_KEY, ORDERS_EXCHANGE } from './messaging.constants';

const RECONNECT_DELAY_MS = 5_000;

/**
 * Consumes `order.created` and stores a notification.
 *
 * Stage 1 note: deliberately simple. Ack after the DB write, no inbox table,
 * so a redelivered message creates a duplicate notification. Stage 4 fixes that.
 */
@Injectable()
export class RabbitMqConsumer implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(RabbitMqConsumer.name);
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
    await this.connection?.close().catch(() => undefined);
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

      const channel = await connection.createChannel();
      await channel.assertExchange(ORDERS_EXCHANGE, 'topic', { durable: true });
      await channel.assertQueue(ORDER_CREATED_QUEUE, { durable: true });
      await channel.bindQueue(ORDER_CREATED_QUEUE, ORDERS_EXCHANGE, ORDER_CREATED_ROUTING_KEY);
      await channel.prefetch(10);
      await channel.consume(ORDER_CREATED_QUEUE, (msg) => {
        if (msg) void this.handle(channel, msg);
      });
      this.logger.log(`consuming "${ORDER_CREATED_QUEUE}" (bound to ${ORDERS_EXCHANGE} / ${ORDER_CREATED_ROUTING_KEY})`);
    } catch (err) {
      this.scheduleReconnect((err as Error).message);
    }
  }

  private scheduleReconnect(reason: string) {
    if (this.shuttingDown || this.reconnectTimer) return;
    this.logger.warn(`RabbitMQ unavailable (${reason}); retrying in ${RECONNECT_DELAY_MS / 1000}s`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.start();
    }, RECONNECT_DELAY_MS);
  }

  private async handle(channel: Channel, msg: ConsumeMessage) {
    const messageId = msg.properties.messageId ?? '(none)';
    try {
      const event = JSON.parse(msg.content.toString()) as OrderCreatedEvent;
      await this.notifications.handleOrderCreated(event, messageId);
      channel.ack(msg);
    } catch (err) {
      // Naive: drop the message. Stage 4 adds retries and a dead-letter queue.
      this.logger.error(`failed to handle message ${messageId}, dropping it: ${(err as Error).message}`);
      channel.nack(msg, false, false);
    }
  }
}
