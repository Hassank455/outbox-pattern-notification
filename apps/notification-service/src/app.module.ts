import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { EmailModule } from './email/email.module';
import { HealthController } from './health.controller';
import { MessagingModule } from './messaging/messaging.module';
import { NotificationsModule } from './notifications/notifications.module';
import { PrismaModule } from './prisma/prisma.module';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), PrismaModule, NotificationsModule, MessagingModule, EmailModule],
  controllers: [HealthController],
})
export class AppModule {}
