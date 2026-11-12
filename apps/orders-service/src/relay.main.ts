import 'dotenv/config';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { RelayAppModule } from './relay/relay-app.module';

/**
 * Runs ONLY the outbox relay, without the HTTP server.
 * Start several of these (npm run relay) to watch FOR UPDATE SKIP LOCKED split the work.
 */
async function bootstrap() {
  process.env.RELAY_ENABLED = 'true';
  const app = await NestFactory.createApplicationContext(RelayAppModule);
  app.enableShutdownHooks();
  Logger.log(`standalone outbox relay running (pid ${process.pid})`, 'Bootstrap');
}
bootstrap();
