import { Injectable } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { GetQueueUrlCommand, SQSClient } from "@aws-sdk/client-sqs";
import { MetricsService } from "./metrics.service.js";

@Injectable()
export class HealthService {
  private readonly sqs = new SQSClient({ region: process.env.AWS_REGION ?? "us-east-1", endpoint: process.env.SQS_ENDPOINT, credentials: { accessKeyId: process.env.SQS_ENDPOINT?.includes("localhost") ? "test" : (process.env.AWS_ACCESS_KEY_ID ?? "test"), secretAccessKey: process.env.SQS_ENDPOINT?.includes("localhost") ? "test" : (process.env.AWS_SECRET_ACCESS_KEY ?? "test") } });
  constructor(private readonly em: EntityManager, private readonly metrics: MetricsService) {}

  async readiness(): Promise<{status:"ok";checks:{postgres:"ok";sqs:"ok"}}> {
    await this.em.getConnection().execute("SELECT 1");
    const lag = await this.em.getConnection().execute<{count:string}[]>("SELECT COUNT(*)::text AS count FROM outbox_messages WHERE published_at IS NULL");
    this.metrics.set("outbox_pending_total", Number(lag[0]?.count ?? 0));
    const result = await this.sqs.send(new GetQueueUrlCommand({ QueueName: process.env.SQS_QUEUE_NAME ?? "wager-transactions.fifo" }));
    if (!result.QueueUrl) throw new Error("SQS_QUEUE_NOT_FOUND");
    return { status: "ok", checks: { postgres: "ok", sqs: "ok" } };
  }
}
