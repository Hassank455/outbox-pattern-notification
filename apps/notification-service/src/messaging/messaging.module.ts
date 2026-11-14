import { Module } from '@nestjs/common';
import { NotificationsModule } from '../notifications/notifications.module';
import { RabbitMqConsumer } from './rabbitmq.consumer';
import { StatsController } from './stats.controller';

@Module({
  imports: [NotificationsModule],
  controllers: [StatsController],
  providers: [RabbitMqConsumer],
})
export class MessagingModule {}
