import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { MessagingModule } from '../messaging/messaging.module';
import { PrismaModule } from '../prisma/prisma.module';
import { RelayModule } from './relay.module';

/** Root module for the standalone relay process (src/relay.main.ts): no HTTP, just the relay. */
@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), PrismaModule, MessagingModule, RelayModule],
})
export class RelayAppModule {}
