import { BadRequestException, Body, Controller, Get, Post, Query } from '@nestjs/common';
import { CreateOrderDto } from './dto/create-order.dto';
import { FailureMode, OrdersService } from './orders.service';

const FAILURE_MODES: FailureMode[] = ['crash-after-save', 'publish-before-save'];

@Controller('orders')
export class OrdersController {
  constructor(private readonly orders: OrdersService) {}

  /** POST /orders?fail=crash-after-save|publish-before-save  (fail is only for experiments) */
  @Post()
  create(@Body() dto: CreateOrderDto, @Query('fail') fail?: string) {
    if (fail !== undefined && !FAILURE_MODES.includes(fail as FailureMode)) {
      throw new BadRequestException(`fail must be one of: ${FAILURE_MODES.join(', ')}`);
    }
    return this.orders.create(dto, fail as FailureMode | undefined);
  }

  @Get()
  findAll() {
    return this.orders.findAll();
  }
}
