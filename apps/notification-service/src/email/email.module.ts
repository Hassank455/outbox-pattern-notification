import { Controller, Get, Module, Query } from '@nestjs/common';
import { EmailStatus } from '../generated/prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { EmailProviderClient } from './email-provider.client';
import { EmailWorker } from './email.worker';

/** GET /emails?status=PENDING  and  GET /emails/stats : look inside the email outbox. */
@Controller('emails')
export class EmailsController {
  constructor(private readonly prisma: PrismaService) {}

  @Get()
  list(@Query('status') status?: string) {
    const where = status && status in EmailStatus ? { status: status as EmailStatus } : {};
    return this.prisma.outboundEmail.findMany({ where, orderBy: { createdAt: 'desc' }, take: 50 });
  }

  @Get('stats')
  async stats() {
    const groups = await this.prisma.outboundEmail.groupBy({ by: ['status'], _count: true });
    const byStatus = Object.fromEntries(groups.map((g) => [g.status, g._count]));
    return { PENDING: 0, SENT: 0, FAILED: 0, ...byStatus };
  }
}

@Module({
  controllers: [EmailsController],
  providers: [EmailProviderClient, EmailWorker],
})
export class EmailModule {}

/** Root module for the standalone worker process (src/email-worker.main.ts): no HTTP, no consumer. */
@Module({
  providers: [EmailProviderClient, EmailWorker],
})
export class EmailWorkerModule {}
