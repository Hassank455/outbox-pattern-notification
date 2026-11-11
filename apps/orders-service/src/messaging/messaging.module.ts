import { Global, Module } from '@nestjs/common';
import { RabbitMqPublisher } from './rabbitmq.publisher';

@Global()
@Module({
  providers: [RabbitMqPublisher],
  exports: [RabbitMqPublisher],
})
export class MessagingModule {}
