import { Module } from '@nestjs/common';
import { OutboxRelay } from './outbox.relay';

/** Needs PrismaService and RabbitMqPublisher, which come from the global Prisma/Messaging modules. */
@Module({
  providers: [OutboxRelay],
})
export class RelayModule {}
