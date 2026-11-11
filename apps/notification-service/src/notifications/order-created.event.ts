// notification-service's own copy of the `order.created` contract.
export interface OrderCreatedEvent {
  orderId: string;
  customerEmail: string;
  product: string;
  quantity: number;
  totalCents: number;
  createdAt: string;
}
