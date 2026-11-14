-- CreateTable
CREATE TABLE "processed_messages" (
    "message_id" TEXT NOT NULL,
    "message_type" TEXT NOT NULL,
    "received_at" TIMESTAMP(3) NOT NULL,
    "processed_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "duplicate_deliveries" INTEGER NOT NULL DEFAULT 0,
    "last_duplicate_at" TIMESTAMP(3),

    CONSTRAINT "processed_messages_pkey" PRIMARY KEY ("message_id")
);
