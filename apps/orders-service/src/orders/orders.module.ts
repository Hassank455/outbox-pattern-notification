import { Module } from '@nestjs/common';
import { OutboxModule } from '../outbox/outbox.module';
import { OrdersController } from './orders.controller';
import { OrdersService } from './orders.service';

@Module({
  imports: [OutboxModule],
  controllers: [OrdersController],
  providers: [OrdersService],
})
export class OrdersModule {}
