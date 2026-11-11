// The contract between orders-service and notification-service.
// Both services declare the same exchange; notification-service owns its queue.
export const ORDERS_EXCHANGE = 'orders.events';
export const ORDER_CREATED_ROUTING_KEY = 'order.created';
