-- Stage 6: wake up LISTEN/NOTIFY relays when a new outbox row is inserted.
--
-- Only the row id is sent, never the row itself: NOTIFY payloads are limited to 8000 bytes,
-- and the relay must re-read and lock the row anyway (FOR UPDATE SKIP LOCKED), so the
-- notification is just a "something new is there" doorbell.
--
-- NOTIFY is transactional: it is delivered only when the inserting transaction COMMITs,
-- so a relay is never woken for a row it can't see yet (or for a rolled-back one).
-- It is NOT durable: a relay that isn't connected at that moment misses it, which is why
-- the relay also sweeps at startup and polls slowly as a fallback.
-- Prisma doesn't model triggers, so this lives only in this migration.

CREATE OR REPLACE FUNCTION notify_outbox_insert() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('outbox_channel', NEW.id::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_messages_notify
AFTER INSERT ON outbox_messages
FOR EACH ROW EXECUTE FUNCTION notify_outbox_insert();
