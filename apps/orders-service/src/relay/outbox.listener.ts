import { Logger } from '@nestjs/common';
import { Client } from 'pg';

export const OUTBOX_CHANNEL = 'outbox_channel';
const RECONNECT_DELAY_MS = 2_000;

/** pg's ECONNREFUSED comes as an AggregateError with an empty message. */
function describe(err: unknown): string {
  if (err instanceof AggregateError && err.errors.length) return describe(err.errors[0]);
  if (err instanceof Error) return err.message || (err as { code?: string }).code || err.name;
  return String(err);
}

/**
 * Stage 6: LISTEN/NOTIFY doorbell for the relay.
 *
 * Holds ONE dedicated Postgres connection (not from Prisma's pool: a pooled connection
 * could be handed to someone else, and LISTEN belongs to the session) and runs
 * `LISTEN outbox_channel`. The trigger on outbox_messages sends the new row's id on
 * every committed INSERT; we don't use the id, we just wake the relay, which then claims
 * rows the normal way (FOR UPDATE SKIP LOCKED). So N relays can all be woken safely.
 *
 * NOTIFY isn't durable: anything sent while this connection is down is lost. So every
 * (re)connect also wakes the relay to sweep for rows it may have missed.
 */
export class OutboxListener {
  private client?: Client;
  private reconnectTimer?: NodeJS.Timeout;
  private stopped = false;

  constructor(
    private readonly onWake: (reason: string) => void,
    private readonly logger: Logger,
  ) {}

  async start() {
    await this.connect();
  }

  async stop() {
    this.stopped = true;
    clearTimeout(this.reconnectTimer);
    const client = this.client;
    this.client = undefined;
    await client?.end().catch(() => undefined);
  }

  private async connect() {
    if (this.stopped) return;
    const client = new Client({ connectionString: process.env.DATABASE_URL });
    client.on('notification', (msg) => {
      if (msg.channel === OUTBOX_CHANNEL) this.onWake('notify');
    });
    client.on('error', (err) => this.logger.warn(`LISTEN connection error: ${describe(err)}`));
    client.on('end', () => {
      if (this.client !== client) return;
      this.client = undefined;
      this.logger.warn('LISTEN connection lost; notifications are missed until it reconnects');
      this.scheduleReconnect();
    });

    try {
      await client.connect();
      await client.query(`LISTEN ${OUTBOX_CHANNEL}`);
      this.client = client;
      this.logger.log(`listening on "${OUTBOX_CHANNEL}"`);
      this.onWake('listener connected: sweep for anything missed');
    } catch (err) {
      this.logger.warn(`could not LISTEN (${describe(err)}); retrying in ${RECONNECT_DELAY_MS / 1000}s`);
      await client.end().catch(() => undefined);
      this.scheduleReconnect();
    }
  }

  private scheduleReconnect() {
    if (this.stopped || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.connect();
    }, RECONNECT_DELAY_MS);
  }
}
