import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { OrderCreatedEvent } from './order-created.event';

export interface MessageMeta {
  messageId: string;
  messageType: string;
  receivedAt: Date;
}

export type HandleResult = 'processed' | 'duplicate';

@Injectable()
export class NotificationsService {
  private readonly simulatedFailureRate = Math.min(1, Number(process.env.CONSUMER_SIMULATED_FAILURE_RATE ?? 0) || 0);

  constructor(private readonly prisma: PrismaService) {}

  /**
   * STAGE 4: IDEMPOTENT HANDLING WITH AN INBOX.
   *
   *   BEGIN
   *     INSERT INTO processed_messages (message_id, ...) ON CONFLICT DO NOTHING
   *     -- 0 rows inserted? we've seen this messageId before -> duplicate, stop here
   *     INSERT INTO notifications (...)
   *   COMMIT
   *
   * Why both writes must share ONE transaction:
   *   - inbox first, notification fails  -> without a transaction the message is marked "done"
   *     but no notification exists, and the retry would skip it: LOST.
   *   - notification first, inbox fails  -> the retry stores a second notification: DUPLICATE.
   *   With one transaction they commit or roll back together, so a retry always sees the truth.
   *
   * Concurrency: if two deliveries of the same messageId run at the same time, the primary key
   * makes the second INSERT wait for the first transaction. If it commits, the second gets
   * "0 rows" (duplicate). If it rolls back, the second one goes ahead.
   *
   * Caveat: here "sending" = storing a row, so it fits in the DB transaction. A REAL email/SMS
   * call can't be rolled back. Options (not built here):
   *   - commit first, then call the provider with an idempotency key (= messageId) so a repeat
   *     call doesn't send twice (most email/SMS APIs support this), or
   *   - write a "send email" row to a notification outbox in this same transaction and let a
   *     separate worker do the call, the same pattern as orders-service.
   */
  handleOrderCreated(event: OrderCreatedEvent, meta: MessageMeta): Promise<HandleResult> {
    return this.prisma.$transaction(async (tx) => {
      const { count } = await tx.processedMessage.createMany({
        data: [{ messageId: meta.messageId, messageType: meta.messageType, receivedAt: meta.receivedAt }],
        skipDuplicates: true, // ON CONFLICT DO NOTHING: a duplicate doesn't abort the transaction
      });

      if (count === 0) {
        await tx.processedMessage.update({
          where: { messageId: meta.messageId },
          data: { duplicateDeliveries: { increment: 1 }, lastDuplicateAt: new Date() },
        });
        return 'duplicate';
      }

      const total = (event.totalCents / 100).toFixed(2);
      await tx.notification.create({
        data: {
          orderId: event.orderId,
          recipient: event.customerEmail,
          subject: `Order confirmed: ${event.product}`,
          body: `Thanks for your order of ${event.quantity} x ${event.product}. Total: $${total}.`,
        },
      });

      if (Math.random() < this.simulatedFailureRate) {
        // Both rows were written; throwing rolls them back, so the retry starts clean.
        throw new Error('simulated failure after writing inbox + notification (CONSUMER_SIMULATED_FAILURE_RATE)');
      }
      return 'processed';
    });
  }

  findAll() {
    return this.prisma.notification.findMany({ orderBy: { sentAt: 'desc' }, take: 50 });
  }

  async stats() {
    const [notifications, distinct, processedMessages, duplicates, messagesWithDuplicates] = await Promise.all([
      this.prisma.notification.count(),
      this.prisma.notification.groupBy({ by: ['orderId'] }),
      this.prisma.processedMessage.count(),
      this.prisma.processedMessage.aggregate({ _sum: { duplicateDeliveries: true } }),
      this.prisma.processedMessage.count({ where: { duplicateDeliveries: { gt: 0 } } }),
    ]);
    return {
      notifications,
      distinctOrdersNotified: distinct.length,
      processedMessages,
      duplicateDeliveriesSkipped: duplicates._sum.duplicateDeliveries ?? 0,
      messagesDeliveredMoreThanOnce: messagesWithDuplicates,
    };
  }
}
