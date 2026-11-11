import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { OrderCreatedEvent } from './order-created.event';

@Injectable()
export class NotificationsService {
  private readonly logger = new Logger(NotificationsService.name);

  constructor(private readonly prisma: PrismaService) {}

  /** "Sends" the order confirmation. For this project, sending = logging + storing a row. */
  async handleOrderCreated(event: OrderCreatedEvent, messageId: string) {
    const total = (event.totalCents / 100).toFixed(2);
    const notification = await this.prisma.notification.create({
      data: {
        orderId: event.orderId,
        recipient: event.customerEmail,
        subject: `Order confirmed: ${event.product}`,
        body: `Thanks for your order of ${event.quantity} x ${event.product}. Total: $${total}.`,
      },
    });
    this.logger.log(`📧 sent to ${event.customerEmail} for order ${event.orderId} (message ${messageId})`);
    return notification;
  }

  findAll() {
    return this.prisma.notification.findMany({ orderBy: { sentAt: 'desc' }, take: 50 });
  }
}
