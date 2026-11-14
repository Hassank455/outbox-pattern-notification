// Must match orders-service. The exchange is the shared contract;
// everything below it belongs to notification-service.
export const ORDERS_EXCHANGE = 'orders.events';
export const ORDER_CREATED_ROUTING_KEY = 'order.created';

/**
 * Stage 4 topology:
 *
 *   orders.events ──order.created──▶ ORDER_CREATED_QUEUE ──▶ consumer
 *                                        ▲         │ nack (poison / out of retries)
 *            TTL expires, dead-letter    │         ▼
 *            back to the main queue      │    DEAD_LETTER_EXCHANGE ──▶ DEAD_LETTER_QUEUE
 *                                        │
 *                                   RETRY_QUEUE ◀── consumer re-publishes a failed message here
 *                                   (no consumers, messages just wait)
 */
export const ORDER_CREATED_QUEUE = 'notification-service.order-created';
export const RETRY_QUEUE = 'notification-service.order-created.retry';
export const DEAD_LETTER_EXCHANGE = 'notification-service.dlx';
export const DEAD_LETTER_QUEUE = 'notification-service.order-created.dlq';

/** Header where we count retries on the copies we send to RETRY_QUEUE. */
export const RETRY_COUNT_HEADER = 'x-retry-count';
export const LAST_ERROR_HEADER = 'x-last-error';
