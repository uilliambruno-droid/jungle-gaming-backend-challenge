import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { GetQueueUrlCommand, SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import {
  IntegrationEventEnvelope,
} from "../../domain/integration/integration-event.js";
import { MetricsService } from "../observability/metrics.service.js";

interface OutboxRow {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: Record<string, unknown> | string;
  occurred_at: Date;
  attempts: number;
}

interface OutboxStats {
  pending: string;
  oldest_lag_seconds: number;
}

function wait(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function errorCode(error: unknown): string {
  if (error && typeof error === "object" && "name" in error) {
    return String((error as { name: unknown }).name);
  }
  return "OUTBOX_ERROR";
}

@Injectable()
export class OutboxPublisher implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxPublisher.name);
  private readonly client = new SQSClient({
    region: process.env.AWS_REGION ?? "us-east-1",
    endpoint: process.env.SQS_ENDPOINT,
    credentials: {
      accessKeyId: process.env.SQS_ENDPOINT?.includes("localhost")
        ? "test"
        : (process.env.AWS_ACCESS_KEY_ID ?? "test"),
      secretAccessKey: process.env.SQS_ENDPOINT?.includes("localhost")
        ? "test"
        : (process.env.AWS_SECRET_ACCESS_KEY ?? "test"),
    },
  });
  private running = true;
  private loopPromise?: Promise<void>;
  private queueUrl?: string;

  constructor(
    private readonly em: EntityManager,
    private readonly metrics: MetricsService,
  ) {}

  async onModuleInit(): Promise<void> {
    this.queueUrl = await this.resolveQueueUrl();
    this.loopPromise = this.loop();
  }

  async onModuleDestroy(): Promise<void> {
    this.running = false;
    await this.loopPromise;
    this.client.destroy();
  }

  private async resolveQueueUrl(): Promise<string> {
    const response = await this.client.send(new GetQueueUrlCommand({
      QueueName: process.env.OUTBOX_QUEUE_NAME ?? "wager-events.fifo",
    }));
    if (!response.QueueUrl) throw new Error("OUTBOX_QUEUE_URL_NOT_FOUND");
    return response.QueueUrl;
  }

  private async loop(): Promise<void> {
    while (this.running) {
      try {
        const published = await this.publishOne();
        await this.refreshMetrics();
        if (!published && this.running) {
          await wait(Number(process.env.OUTBOX_POLL_INTERVAL_MS ?? "1000"));
        }
      } catch (error) {
        this.logger.error(JSON.stringify({
          event: "outbox_worker_failed",
          correlationId: "outbox-worker",
          messageId: null,
          transactionId: null,
          walletId: null,
          providerId: null,
          category: "transient",
          errorCode: errorCode(error),
        }));
        this.metrics.increment("outbox_worker_errors_total");
        if (this.running) await wait(Number(process.env.OUTBOX_ERROR_DELAY_MS ?? "1000"));
      }
    }
  }

  private async publishOne(): Promise<boolean> {
    return this.em.transactional(async tm => {
      const rows = await tm.execute<OutboxRow[]>(
        `SELECT id,aggregate_id,event_type,payload,occurred_at,attempts
         FROM outbox_messages
         WHERE published_at IS NULL AND (next_attempt_at IS NULL OR next_attempt_at<=now())
         ORDER BY occurred_at,id
         FOR UPDATE SKIP LOCKED
         LIMIT 1`,
      );
      if (!rows.length) return false;
      const row = rows[0];
      const envelope = (typeof row.payload === "string" ? JSON.parse(row.payload) : row.payload) as unknown as IntegrationEventEnvelope<unknown>;
      if (envelope.eventId !== row.id || envelope.eventType !== row.event_type
        || envelope.aggregateId !== row.aggregate_id || !envelope.correlationId) {
        throw new Error("OUTBOX_ENVELOPE_INVALID");
      }
      const eventData = envelope.data && typeof envelope.data === "object"
        ? envelope.data as Record<string, unknown>
        : {};
      const transactionId = typeof eventData.transactionId === "string" ? eventData.transactionId : null;
      const walletId = typeof eventData.walletId === "string" ? eventData.walletId : row.aggregate_id;
      const providerId = typeof eventData.providerId === "string" ? eventData.providerId : null;
      const body = JSON.stringify(envelope);
      this.metrics.increment("outbox_publish_attempts_total");
      this.metrics.set(
        "outbox_lag_seconds",
        Math.max(0, (Date.now() - new Date(row.occurred_at).getTime()) / 1000),
      );

      const publishStartedAt = performance.now();
      try {
        await this.client.send(new SendMessageCommand({
          QueueUrl: this.queueUrl,
          MessageBody: body,
          MessageGroupId: row.aggregate_id,
          MessageDeduplicationId: row.id,
        }));
      } catch (error) {
        this.metrics.observe("outbox_publish_latency_ms", performance.now() - publishStartedAt);
        await tm.execute(
          `UPDATE outbox_messages
           SET attempts=attempts+1,
               next_attempt_at=now()+(POWER(2,LEAST(attempts,8))*interval '1 second')
           WHERE id=?`,
          [row.id],
        );
        this.metrics.increment("outbox_retries_total");
        this.logger.warn(JSON.stringify({
          event: "outbox_publish_retry",
          correlationId: envelope.correlationId,
          messageId: null,
          transactionId,
          walletId,
          providerId,
          outboxId: row.id,
          eventType: row.event_type,
          attempts: row.attempts + 1,
          errorCode: errorCode(error),
        }));
        return true;
      }
      this.metrics.observe("outbox_publish_latency_ms", performance.now() - publishStartedAt);

      await tm.execute(
        "UPDATE outbox_messages SET published_at=now(),attempts=attempts+1,next_attempt_at=NULL WHERE id=?",
        [row.id],
      );
      this.metrics.increment("outbox_published_total");
      this.logger.log(JSON.stringify({
        event: "outbox_published",
        correlationId: envelope.correlationId,
        messageId: null,
        transactionId,
        walletId,
        providerId,
        outboxId: row.id,
        eventType: row.event_type,
        attempts: row.attempts + 1,
      }));
      return true;
    });
  }

  private async refreshMetrics(): Promise<void> {
    const rows = await this.em.getConnection().execute<OutboxStats[]>(
      `SELECT COUNT(*)::text AS pending,
              COALESCE(EXTRACT(EPOCH FROM (now()-MIN(occurred_at))),0)::float8 AS oldest_lag_seconds
       FROM outbox_messages
       WHERE published_at IS NULL`,
    );
    this.metrics.set("outbox_pending_total", Number(rows[0]?.pending ?? 0));
    this.metrics.set("outbox_lag_seconds", Number(rows[0]?.oldest_lag_seconds ?? 0));
  }
}
