import { Injectable } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { GetQueueAttributesCommand, GetQueueUrlCommand, SQSClient } from "@aws-sdk/client-sqs";
import { MetricsService } from "./metrics.service.js";

@Injectable()
export class HealthService {
  private readonly sqs = new SQSClient({ region: process.env.AWS_REGION ?? "us-east-1", endpoint: process.env.SQS_ENDPOINT, credentials: { accessKeyId: process.env.SQS_ENDPOINT?.includes("localhost") ? "test" : (process.env.AWS_ACCESS_KEY_ID ?? "test"), secretAccessKey: process.env.SQS_ENDPOINT?.includes("localhost") ? "test" : (process.env.AWS_SECRET_ACCESS_KEY ?? "test") } });
  constructor(private readonly em: EntityManager, private readonly metrics: MetricsService) {}

  async readiness(): Promise<{status:"ok";checks:{postgres:"ok";sqs:"ok"}}> {
    await this.em.getConnection().execute("SELECT 1");
    const lag = await this.em.getConnection().execute<Array<{pending:string;lag_seconds:number}>>(
      `SELECT COUNT(*)::text AS pending,
              COALESCE(EXTRACT(EPOCH FROM (now()-MIN(occurred_at))),0)::float8 AS lag_seconds
       FROM outbox_messages WHERE published_at IS NULL`,
    );
    this.metrics.set("outbox_pending_total", Number(lag[0]?.pending ?? 0));
    this.metrics.set("outbox_lag_seconds", Number(lag[0]?.lag_seconds ?? 0));

    const queueNames = [
      process.env.SQS_QUEUE_NAME ?? "wager-transactions.fifo",
      process.env.SQS_DLQ_NAME ?? "wager-transactions-dlq.fifo",
    ];
    const queueUrls = await Promise.all(queueNames.map(async queueName => {
      const result = await this.sqs.send(new GetQueueUrlCommand({ QueueName: queueName }));
      if (!result.QueueUrl) throw new Error(`SQS_QUEUE_NOT_FOUND:${queueName}`);
      return result.QueueUrl;
    }));
    const dlqAttributes = await this.sqs.send(new GetQueueAttributesCommand({
      QueueUrl: queueUrls[1],
      AttributeNames: ["ApproximateNumberOfMessages"],
    }));
    this.metrics.set("sqs_dlq_messages", Number(dlqAttributes.Attributes?.ApproximateNumberOfMessages ?? 0));
    return { status: "ok", checks: { postgres: "ok", sqs: "ok" } };
  }
}
