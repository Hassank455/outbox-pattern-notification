import { Injectable } from '@nestjs/common';
import { loadEmailWorkerConfig } from './email.config';

export interface OutgoingEmail {
  to: string;
  subject: string;
  body: string;
}

export interface SendResult {
  providerMessageId: string;
  /** true when the provider had already seen this Idempotency-Key and did NOT send again. */
  replayed: boolean;
}

/**
 * The provider rejected the email for a reason a retry won't fix (bad address, 4xx).
 * The worker marks the row FAILED right away instead of retrying.
 */
export class PermanentEmailError extends Error {}

/**
 * HTTP client for the email provider. The API is shaped like real ones (SendGrid, Postmark,
 * Resend, Stripe for payments): POST the email, optionally with an `Idempotency-Key` header.
 * If the provider has already accepted a request with that key, it returns the ORIGINAL
 * result and sends nothing. That is what makes a retried send safe.
 */
@Injectable()
export class EmailProviderClient {
  private readonly config = loadEmailWorkerConfig();

  async send(email: OutgoingEmail, idempotencyKey: string): Promise<SendResult> {
    const headers: Record<string, string> = { 'content-type': 'application/json' };
    if (this.config.useIdempotencyKey) headers['idempotency-key'] = idempotencyKey;

    const res = await fetch(`${this.config.providerUrl}/v1/emails`, {
      method: 'POST',
      headers,
      body: JSON.stringify(email),
      signal: AbortSignal.timeout(this.config.requestTimeoutMs),
    });

    if (res.ok) {
      const body = (await res.json()) as { id: string };
      return { providerMessageId: body.id, replayed: res.headers.get('idempotent-replayed') === 'true' };
    }
    const text = (await res.text()).slice(0, 300);
    // 4xx (except 408 timeout / 409 conflict / 429 rate limit) = our request is wrong: don't retry.
    if (res.status >= 400 && res.status < 500 && ![408, 409, 429].includes(res.status)) {
      throw new PermanentEmailError(`provider rejected email (${res.status}): ${text}`);
    }
    throw new Error(`provider error (${res.status}): ${text}`);
  }
}
