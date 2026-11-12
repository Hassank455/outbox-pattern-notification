import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { HealthController } from './health.controller';
import { MessagingModule } from './messaging/messaging.module';
import { OrdersModule } from './orders/orders.module';
import { OutboxModule } from './outbox/outbox.module';
import { PrismaModule } from './prisma/prisma.module';
import { RelayModule } from './relay/relay.module';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), PrismaModule, MessagingModule, OutboxModule, OrdersModule, RelayModule],
  controllers: [HealthController],
})
export class AppModule {}
