// Stage 7: all email worker settings come from env vars, with defaults that work locally.
function intFromEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  const value = raw === undefined || raw === '' ? fallback : Number(raw);
  if (!Number.isFinite(value) || value < 0) throw new Error(`${name} must be a non-negative number, got "${raw}"`);
  return value;
}

export interface EmailWorkerConfig {
  /** Run the worker in this process. The standalone `npm run email-worker` forces it on. */
  enabled: boolean;
  /** Base URL of the email provider (our fake one: npm run email-provider). */
  providerUrl: string;
  /**
   * Send the Idempotency-Key header. Turn it OFF only for the experiment that shows why it
   * exists (EMAIL_USE_IDEMPOTENCY_KEY=false). Never in production.
   */
  useIdempotencyKey: boolean;
  /** Pause between ticks when there was nothing (or not a full batch) to send. */
  intervalMs: number;
  /** Max rows claimed per transaction. */
  batchSize: number;
  /** After this many failed sends a row becomes FAILED. */
  maxAttempts: number;
  /** Backoff after the 1st failure; doubles each time, capped at maxBackoffMs. */
  baseBackoffMs: number;
  maxBackoffMs: number;
  /** Timeout for one provider call. */
  requestTimeoutMs: number;
  /**
   * Chaos knob for the Stage 7 experiment: after this many successful sends, the process kills
   * itself with SIGKILL, i.e. AFTER the provider accepted the email but BEFORE the transaction
   * that marks it SENT commits. 0 = off.
   */
  crashAfterSends: number;
}

export function loadEmailWorkerConfig(): EmailWorkerConfig {
  return {
    enabled: process.env.EMAIL_WORKER_ENABLED !== 'false',
    providerUrl: (process.env.EMAIL_PROVIDER_URL ?? 'http://localhost:3003').replace(/\/$/, ''),
    useIdempotencyKey: process.env.EMAIL_USE_IDEMPOTENCY_KEY !== 'false',
    intervalMs: intFromEnv('EMAIL_WORKER_INTERVAL_MS', 1000),
    batchSize: Math.max(1, intFromEnv('EMAIL_WORKER_BATCH_SIZE', 20)),
    maxAttempts: Math.max(1, intFromEnv('EMAIL_WORKER_MAX_ATTEMPTS', 8)),
    baseBackoffMs: intFromEnv('EMAIL_WORKER_BASE_BACKOFF_MS', 1000),
    maxBackoffMs: intFromEnv('EMAIL_WORKER_MAX_BACKOFF_MS', 60_000),
    requestTimeoutMs: intFromEnv('EMAIL_PROVIDER_TIMEOUT_MS', 5000),
    crashAfterSends: intFromEnv('EMAIL_WORKER_CRASH_AFTER_SENDS', 0),
  };
}

/** Exponential backoff: 1s, 2s, 4s ... capped. `attempts` = failures so far (>= 1). */
export function emailBackoffMs(attempts: number, config: EmailWorkerConfig): number {
  return Math.min(config.baseBackoffMs * 2 ** (attempts - 1), config.maxBackoffMs);
}
