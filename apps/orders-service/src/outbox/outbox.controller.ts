import { BadRequestException, Controller, Get, Query } from '@nestjs/common';
import { OutboxStatus } from '../generated/prisma/client';
import { OutboxService } from './outbox.service';

/** Read-only view of the outbox, for learning and debugging. */
@Controller('outbox')
export class OutboxController {
  constructor(private readonly outbox: OutboxService) {}

  /** GET /outbox?status=PENDING|PUBLISHED|FAILED */
  @Get()
  findAll(@Query('status') status?: string) {
    if (status !== undefined && !(status in OutboxStatus)) {
      throw new BadRequestException(`status must be one of: ${Object.keys(OutboxStatus).join(', ')}`);
    }
    return this.outbox.findAll(status as OutboxStatus | undefined);
  }
}
