import { Injectable, Logger } from '@nestjs/common';
import { ORDER_CREATED_ROUTING_KEY } from '../messaging/messaging.constants';
import { Order } from '../generated/prisma/client';
import { OutboxService } from '../outbox/outbox.service';
import { PrismaService } from '../prisma/prisma.service';
import { CreateOrderDto } from './dto/create-order.dto';
import { OrderCreatedEvent } from './order-created.event';

export type FailureMode = 'fail-inside-transaction' | 'crash-after-commit';

@Injectable()
export class OrdersService {
  private readonly logger = new Logger(OrdersService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly outbox: OutboxService,
  ) {}

  /**
   * STAGE 2: TRANSACTIONAL OUTBOX.
   *
   *   BEGIN
   *     INSERT INTO orders ...
   *     INSERT INTO outbox_messages ... ('order.created', PENDING)
   *   COMMIT
   *
   * One database, one transaction: the order and "an order was created" are
   * saved together or not at all. RabbitMQ is no longer on the request path,
   * so a broker outage can't fail the request or lose the event.
   * Nobody publishes the outbox rows yet: that's the relay, in Stage 3.
   */
  async create(dto: CreateOrderDto, failure?: FailureMode) {
    const order = await this.prisma.$transaction(async (tx) => {
      const order = await tx.order.create({
        data: {
          customerEmail: dto.customerEmail,
          product: dto.product,
          quantity: dto.quantity,
          totalCents: dto.quantity * dto.unitPriceCents,
        },
      });

      const message = await this.outbox.enqueue(tx, {
        aggregateType: 'Order',
        aggregateId: order.id,
        eventType: ORDER_CREATED_ROUTING_KEY,
        routingKey: ORDER_CREATED_ROUTING_KEY,
        payload: { ...this.toEvent(order) },
      });

      if (failure === 'fail-inside-transaction') {
        // Both INSERTs already ran. Throwing here makes Prisma send ROLLBACK:
        // neither the order nor the outbox row survives.
        this.logger.error(`💥 simulating a failure inside the transaction (order ${order.id}, outbox ${message.id})`);
        throw new Error(`simulated failure after writing order ${order.id} and outbox row ${message.id}`);
      }

      return order;
    });
    this.logger.log(`order ${order.id} + outbox row committed together`);

    if (failure === 'crash-after-commit') {
      // In Stage 1 this exact crash lost the event. Now the event is safe in the outbox.
      this.logger.error(`💥 simulating a crash right after committing order ${order.id}`);
      process.exit(1);
    }

    return order;
  }

  findAll() {
    return this.prisma.order.findMany({ orderBy: { createdAt: 'desc' }, take: 50 });
  }

  private toEvent(order: Order): OrderCreatedEvent {
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
