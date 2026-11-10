-- CreateEnum
CREATE TYPE "NotificationChannel" AS ENUM ('EMAIL', 'SMS');

-- CreateTable
CREATE TABLE "notifications" (
    "id" UUID NOT NULL,
    "order_id" UUID NOT NULL,
    "recipient" TEXT NOT NULL,
    "channel" "NotificationChannel" NOT NULL DEFAULT 'EMAIL',
    "subject" TEXT NOT NULL,
    "body" TEXT NOT NULL,
    "sent_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "notifications_pkey" PRIMARY KEY ("id")
);
