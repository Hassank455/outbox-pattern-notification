import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { RabbitMqPublisher } from '../messaging/rabbitmq.publisher';
import { ORDER_CREATED_ROUTING_KEY } from '../messaging/messaging.constants';
import { PrismaService } from '../prisma/prisma.service';
import { CreateOrderDto } from './dto/create-order.dto';
import { OrderCreatedEvent } from './order-created.event';

export type FailureMode = 'crash-after-save' | 'publish-before-save';

@Injectable()
export class OrdersService {
  private readonly logger = new Logger(OrdersService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly publisher: RabbitMqPublisher,
  ) {}

  /**
   * STAGE 1: THE NAIVE WAY ("dual write").
   *
   *   1. INSERT the order        -> Postgres commits
   *   2. publish order.created   -> RabbitMQ
   *
   * Two separate systems, no shared transaction. Anything that goes wrong
   * between step 1 and step 2 leaves them disagreeing. `failure` lets you
   * trigger that on purpose (see docs/01-naive-publish.md).
   */
  async create(dto: CreateOrderDto, failure?: FailureMode) {
    if (failure === 'publish-before-save') {
      return this.createPublishingFirst(dto);
    }

    const order = await this.prisma.order.create({
      data: {
        customerEmail: dto.customerEmail,
        product: dto.product,
        quantity: dto.quantity,
        totalCents: dto.quantity * dto.unitPriceCents,
      },
    });
    this.logger.log(`order ${order.id} saved`);

    if (failure === 'crash-after-save') {
      this.logger.error(`💥 simulating a crash after saving order ${order.id}, before publishing`);
      process.exit(1);
    }

    // If RabbitMQ is down, this throws: the order is already committed, the event is lost,
    // and the client gets a 500 for an order that actually exists.
    const messageId = await this.publisher.publish(ORDER_CREATED_ROUTING_KEY, this.toEvent(order));
    this.logger.log(`order.created published for ${order.id} (message ${messageId})`);
    return order;
  }

  /** The "fix" people try first: publish, then save. Now a failed INSERT leaves a phantom event. */
  private async createPublishingFirst(dto: CreateOrderDto) {
    const id = randomUUID();
    const createdAt = new Date();
    const totalCents = dto.quantity * dto.unitPriceCents;
    await this.publisher.publish(ORDER_CREATED_ROUTING_KEY, {
      orderId: id,
      customerEmail: dto.customerEmail,
      product: dto.product,
      quantity: dto.quantity,
      totalCents,
      createdAt: createdAt.toISOString(),
    } satisfies OrderCreatedEvent);
    this.logger.log(`order.created published for ${id}, now saving...`);

    this.logger.error(`💥 simulating a failed INSERT for order ${id}, after publishing`);
    throw new Error(`database write failed for order ${id} (simulated)`);
  }

  findAll() {
    return this.prisma.order.findMany({ orderBy: { createdAt: 'desc' }, take: 50 });
  }

  private toEvent(order: {
    id: string;
    customerEmail: string;
    product: string;
    quantity: number;
    totalCents: number;
    createdAt: Date;
  }): OrderCreatedEvent {
    return {
      orderId: order.id,
      customerEmail: order.customerEmail,
      product: order.product,
      quantity: order.quantity,
      totalCents: order.totalCents,
      createdAt: order.createdAt.toISOString(),
    };
  }
}
