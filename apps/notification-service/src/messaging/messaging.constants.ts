// Must match orders-service. The exchange is the shared contract;
// the queue belongs to notification-service.
export const ORDERS_EXCHANGE = 'orders.events';
export const ORDER_CREATED_ROUTING_KEY = 'order.created';
export const ORDER_CREATED_QUEUE = 'notification-service.order-created';
