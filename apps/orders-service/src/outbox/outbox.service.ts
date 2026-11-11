import { Injectable } from '@nestjs/common';
import { OutboxStatus, Prisma } from '../generated/prisma/client';
import { PrismaService } from '../prisma/prisma.service';

export interface NewOutboxMessage {
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  routingKey: string;
  payload: Prisma.InputJsonValue;
}

@Injectable()
export class OutboxService {
  constructor(private readonly prisma: PrismaService) {}

  /**
   * Adds an event to the outbox.
   *
   * It takes the *transaction client* (`tx`) on purpose: the caller must pass the same
   * transaction that writes the business data, otherwise we are back to a dual write.
   */
  enqueue(tx: Prisma.TransactionClient, message: NewOutboxMessage) {
    return tx.outboxMessage.create({ data: message });
  }

  findAll(status?: OutboxStatus) {
    return this.prisma.outboxMessage.findMany({
      where: status ? { status } : undefined,
      orderBy: { createdAt: 'desc' },
      take: 50,
    });
  }
}
