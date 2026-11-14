import { Controller, Get } from '@nestjs/common';
import { NotificationsService } from '../notifications/notifications.service';
import { RabbitMqConsumer } from './rabbitmq.consumer';

@Controller('notifications/stats')
export class StatsController {
  constructor(
    private readonly notifications: NotificationsService,
    private readonly consumer: RabbitMqConsumer,
  ) {}

  /** GET /notifications/stats: what the experiments look at. */
  @Get()
  async stats() {
    const [db, queues] = await Promise.all([this.notifications.stats(), this.consumer.queueDepths()]);
    return { ...db, queues };
  }
}
