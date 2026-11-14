function numberFromEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  const value = raw === undefined || raw === '' ? fallback : Number(raw);
  if (!Number.isFinite(value) || value < 0) throw new Error(`${name} must be a non-negative number, got "${raw}"`);
  return value;
}

export interface ConsumerConfig {
  /** Unacked messages the broker may push to us at once. */
  prefetch: number;
  /** Retries for a failing message before it goes to the DLQ (so maxRetries + 1 attempts in total). */
  maxRetries: number;
  /** How long a failed message waits in the retry queue before coming back. */
  retryDelayMs: number;
}

export function loadConsumerConfig(): ConsumerConfig {
  return {
    prefetch: Math.max(1, numberFromEnv('CONSUMER_PREFETCH', 10)),
    maxRetries: numberFromEnv('CONSUMER_MAX_RETRIES', 3),
    retryDelayMs: numberFromEnv('CONSUMER_RETRY_DELAY_MS', 5000),
  };
}
