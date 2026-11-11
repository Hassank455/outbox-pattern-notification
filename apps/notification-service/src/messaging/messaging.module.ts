import { Module } from '@nestjs/common';
import { NotificationsModule } from '../notifications/notifications.module';
import { RabbitMqConsumer } from './rabbitmq.consumer';

@Module({
  imports: [NotificationsModule],
  providers: [RabbitMqConsumer],
})
export class MessagingModule {}
