// Stage 7: a fake email provider, shaped like real ones (SendGrid, Postmark, Resend; Stripe
// works the same way for payments). It does not send real email; it "delivers" into memory.
//
//   POST /v1/emails            { to, subject, body }    header: Idempotency-Key (optional)
//     - new key (or no key)   -> 202, delivers the email, remembers the key
//     - key seen before       -> 200, returns the ORIGINAL id, header Idempotent-Replayed: true,
//                                delivers NOTHING
//   GET  /v1/emails            -> what each recipient actually received (the "inboxes")
//   GET  /v1/stats[?prefix=]   -> requests, deliveries, replays, duplicate deliveries
//   POST /v1/reset             -> forget everything (used by scripts/email-chaos.mjs)
//
// The important line is "remembers the key". The proof that an email was already sent lives
// HERE, outside our database, so a rollback on our side can't erase it.
//
// Run: npm run dev:email-provider   (PORT=3003 by default)
// Knobs: PROVIDER_LATENCY_MS (default 20), PROVIDER_FAILURE_RATE (0..1, returns 503).
import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';

const PORT = Number(process.env.PORT ?? 3003);
const LATENCY_MS = Number(process.env.PROVIDER_LATENCY_MS ?? 20);
const FAILURE_RATE = Number(process.env.PROVIDER_FAILURE_RATE ?? 0);

let byKey = new Map(); // idempotency key -> { id }  (real providers keep keys ~24h)
let delivered = []; // every email that actually reached someone
let stats = { requests: 0, replays: 0, failuresInjected: 0 };

const json = (res, status, body, headers = {}) => {
  res.writeHead(status, { 'content-type': 'application/json', ...headers });
  res.end(JSON.stringify(body));
};

const readBody = (req) =>
  new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => (data += c));
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function duplicateDeliveries(prefix = '') {
  // Same recipient + subject delivered more than once = a customer got the same email twice.
  const seen = new Map();
  for (const e of delivered) {
    if (!e.to.startsWith(prefix)) continue;
    const k = `${e.to}|${e.subject}`;
    seen.set(k, (seen.get(k) ?? 0) + 1);
  }
  let extra = 0;
  for (const n of seen.values()) if (n > 1) extra += n - 1;
  return { uniqueEmails: seen.size, extraCopies: extra };
}

const server = createServer(async (req, res) => {
  try {
    if (req.method === 'POST' && req.url === '/v1/emails') {
      stats.requests++;
      const key = req.headers['idempotency-key'];
      const email = JSON.parse((await readBody(req)) || '{}');
      if (!email.to || !email.subject) return json(res, 400, { error: 'to and subject are required' });

      await sleep(LATENCY_MS);
      if (Math.random() < FAILURE_RATE) {
        stats.failuresInjected++;
        return json(res, 503, { error: 'simulated provider outage (PROVIDER_FAILURE_RATE)' });
      }

      if (key && byKey.has(key)) {
        stats.replays++;
        return json(res, 200, byKey.get(key), { 'idempotent-replayed': 'true' });
      }

      const result = { id: `msg_${randomUUID()}` };
      if (key) byKey.set(key, result);
      delivered.push({ ...email, id: result.id, key: key ?? null, at: new Date().toISOString() });
      return json(res, 202, result);
    }

    if (req.method === 'GET' && req.url === '/v1/emails') return json(res, 200, delivered.slice(-100));

    if (req.method === 'GET' && req.url.startsWith('/v1/stats')) {
      // ?prefix=chaos7-abc  limits delivery counts to recipients starting with that prefix
      const prefix = new URL(req.url, 'http://x').searchParams.get('prefix') ?? '';
      const mine = delivered.filter((e) => e.to.startsWith(prefix)).length;
      return json(res, 200, { ...stats, delivered: mine, ...duplicateDeliveries(prefix) });
    }

    if (req.method === 'POST' && req.url === '/v1/reset') {
      byKey = new Map();
      delivered = [];
      stats = { requests: 0, replays: 0, failuresInjected: 0 };
      return json(res, 200, { ok: true });
    }

    json(res, 404, { error: 'not found' });
  } catch (err) {
    json(res, 500, { error: String(err?.message ?? err) });
  }
});

server.listen(PORT, () => {
  console.log(`📮 fake email provider on http://localhost:${PORT} (latency ${LATENCY_MS}ms, failure rate ${FAILURE_RATE})`);
});
