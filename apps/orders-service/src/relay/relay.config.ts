// All relay settings come from env vars, with defaults that work locally.
function intFromEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  const value = raw === undefined || raw === '' ? fallback : Number(raw);
  if (!Number.isFinite(value) || value < 0) throw new Error(`${name} must be a non-negative number, got "${raw}"`);
  return value;
}

export type RelayMode = 'polling' | 'notify' | 'none';

export interface RelayConfig {
  /**
   * polling: query the outbox every intervalMs (Stage 3).
   * notify:  wake up on Postgres NOTIFY, plus a slow fallback poll (Stage 6).
   * none:    no relay in this process (e.g. Debezium CDC publishes instead).
   */
  mode: RelayMode;
  enabled: boolean;
  /** polling mode: pause between ticks when there was nothing (or not a full batch) to send. */
  intervalMs: number;
  /** notify mode: safety-net poll, for notifications missed while the LISTEN connection was down. */
  fallbackIntervalMs: number;
  /** Max rows claimed per transaction. */
  batchSize: number;
  /** After this many failed publishes a row becomes FAILED and is no longer retried. */
  maxAttempts: number;
  /** Backoff after the 1st failure; doubles each time, capped at maxBackoffMs. */
  baseBackoffMs: number;
  maxBackoffMs: number;
  /** Chaos knob for experiments: probability (0..1) that a publish fails on purpose. */
  simulatedFailureRate: number;
}

function modeFromEnv(): RelayMode {
  if (process.env.RELAY_ENABLED === 'false') return 'none'; // Stage 3 switch, still honoured
  const mode = (process.env.RELAY_MODE ?? 'polling').toLowerCase();
  if (mode !== 'polling' && mode !== 'notify' && mode !== 'none') {
    throw new Error(`RELAY_MODE must be polling, notify or none, got "${process.env.RELAY_MODE}"`);
  }
  return mode;
}

export function loadRelayConfig(): RelayConfig {
  const mode = modeFromEnv();
  return {
    mode,
    enabled: mode !== 'none',
    intervalMs: intFromEnv('RELAY_INTERVAL_MS', 1000),
    fallbackIntervalMs: intFromEnv('RELAY_FALLBACK_INTERVAL_MS', 30_000),
    batchSize: Math.max(1, intFromEnv('RELAY_BATCH_SIZE', 50)),
    maxAttempts: Math.max(1, intFromEnv('RELAY_MAX_ATTEMPTS', 10)),
    baseBackoffMs: intFromEnv('RELAY_BASE_BACKOFF_MS', 1000),
    maxBackoffMs: intFromEnv('RELAY_MAX_BACKOFF_MS', 60_000),
    simulatedFailureRate: Math.min(1, Number(process.env.RELAY_SIMULATED_FAILURE_RATE ?? 0) || 0),
  };
}

/** Exponential backoff: 1s, 2s, 4s, 8s ... capped. `attempts` = failures so far (>= 1). */
export function backoffMs(attempts: number, config: RelayConfig): number {
  return Math.min(config.baseBackoffMs * 2 ** (attempts - 1), config.maxBackoffMs);
}
