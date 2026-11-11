// Payload of the `order.created` event. notification-service keeps its own copy of this shape:
// services share a contract, not code.
export interface OrderCreatedEvent {
  orderId: string;
  customerEmail: string;
  product: string;
  quantity: number;
  totalCents: number;
  createdAt: string;
}
