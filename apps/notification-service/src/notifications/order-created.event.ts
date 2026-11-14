// notification-service's own copy of the `order.created` contract.
export interface OrderCreatedEvent {
  orderId: string;
  customerEmail: string;
  product: string;
  quantity: number;
  totalCents: number;
  createdAt: string;
}

/**
 * A message that can never succeed, however often we retry it
 * (not JSON, wrong shape, no messageId...). It goes straight to the DLQ.
 */
export class PoisonMessageError extends Error {}

export function parseOrderCreated(content: Buffer): OrderCreatedEvent {
  let data: unknown;
  try {
    data = JSON.parse(content.toString());
  } catch {
    throw new PoisonMessageError('body is not valid JSON');
  }
  const e = data as Partial<OrderCreatedEvent> | null;
  const problems: string[] = [];
  if (typeof e?.orderId !== 'string') problems.push('orderId');
  if (typeof e?.customerEmail !== 'string') problems.push('customerEmail');
  if (typeof e?.product !== 'string') problems.push('product');
  if (typeof e?.quantity !== 'number') problems.push('quantity');
  if (typeof e?.totalCents !== 'number') problems.push('totalCents');
  if (problems.length) throw new PoisonMessageError(`missing or invalid fields: ${problems.join(', ')}`);
  return e as OrderCreatedEvent;
}
