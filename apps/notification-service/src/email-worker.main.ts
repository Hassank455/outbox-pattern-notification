import 'dotenv/config';
import { Logger, Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { NestFactory } from '@nestjs/core';
import { EmailWorkerModule } from './email/email.module';
import { PrismaModule } from './prisma/prisma.module';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), PrismaModule, EmailWorkerModule],
})
class EmailWorkerAppModule {}

/**
 * Stage 7: runs ONLY the email worker (no HTTP server, no RabbitMQ consumer).
 * Start several (npm run email-worker) to watch SKIP LOCKED split the emails, or use it in the
 * crash experiment (scripts/email-chaos.mjs).
 */
async function bootstrap() {
  process.env.EMAIL_WORKER_ENABLED = 'true';
  const app = await NestFactory.createApplicationContext(EmailWorkerAppModule);
  app.enableShutdownHooks();
  Logger.log(`standalone email worker running (pid ${process.pid})`, 'Bootstrap');
}
bootstrap();
