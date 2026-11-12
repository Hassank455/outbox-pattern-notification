-- AlterTable
ALTER TABLE "outbox_messages" ADD COLUMN     "next_attempt_at" TIMESTAMP(3);

-- CreateIndex
CREATE INDEX "outbox_messages_status_next_attempt_at_idx" ON "outbox_messages"("status", "next_attempt_at");
