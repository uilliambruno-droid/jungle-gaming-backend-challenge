import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { GetQueueUrlCommand, SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { MetricsService } from "../observability/metrics.service.js";

interface OutboxRow { id:string; aggregate_id:string; event_type:string; payload:Record<string,unknown>|string; occurred_at:Date; attempts:number; }

@Injectable()
export class OutboxPublisher implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxPublisher.name);
  private readonly client = new SQSClient({ region: process.env.AWS_REGION ?? "us-east-1", endpoint: process.env.SQS_ENDPOINT, credentials: { accessKeyId: process.env.SQS_ENDPOINT?.includes("localhost") ? "test" : (process.env.AWS_ACCESS_KEY_ID ?? "test"), secretAccessKey: process.env.SQS_ENDPOINT?.includes("localhost") ? "test" : (process.env.AWS_SECRET_ACCESS_KEY ?? "test") } });
  private running = true;
  private loopPromise?: Promise<void>;
  private queueUrl?: string;

  constructor(private readonly em:EntityManager, private readonly metrics:MetricsService) {}

  async onModuleInit():Promise<void>{ this.queueUrl=await this.resolveQueueUrl(); this.loopPromise=this.loop(); }
  async onModuleDestroy():Promise<void>{ this.running=false; await this.loopPromise; this.client.destroy(); }

  private async resolveQueueUrl():Promise<string>{
    const response=await this.client.send(new GetQueueUrlCommand({QueueName:process.env.OUTBOX_QUEUE_NAME??"wager-events.fifo"}));
    if(!response.QueueUrl)throw new Error("OUTBOX_QUEUE_URL_NOT_FOUND");
    return response.QueueUrl;
  }

  private async loop():Promise<void>{
    while(this.running){
      const published=await this.publishOne();
      if(!published)await new Promise(resolve=>setTimeout(resolve,Number(process.env.OUTBOX_POLL_INTERVAL_MS??"1000")));
    }
  }

  private async publishOne():Promise<boolean>{
    return this.em.transactional(async tm=>{
      const c=tm.getConnection();
      const rows=await c.execute<OutboxRow[]>("SELECT id,aggregate_id,event_type,payload,occurred_at,attempts FROM outbox_messages WHERE published_at IS NULL AND (next_attempt_at IS NULL OR next_attempt_at <= now()) ORDER BY occurred_at,id FOR UPDATE SKIP LOCKED LIMIT 1");
      if(!rows.length)return false;
      const row=rows[0];
      const data=typeof row.payload === "string" ? JSON.parse(row.payload) : row.payload;
      const envelope={eventId:row.id,eventType:row.event_type,aggregateId:row.aggregate_id,correlationId:String(data.correlationId??data.transactionId??row.aggregate_id),occurredAt:new Date(row.occurred_at).toISOString(),version:1,data};
      try{
        await this.client.send(new SendMessageCommand({QueueUrl:this.queueUrl,MessageBody:JSON.stringify(envelope),MessageGroupId:row.aggregate_id,MessageDeduplicationId:row.id}));
        await c.execute("UPDATE outbox_messages SET published_at=now(),attempts=attempts+1,next_attempt_at=NULL WHERE id=?",[row.id]);
        this.logger.log(JSON.stringify({event:"outbox_published",outboxId:row.id,eventType:row.event_type,aggregateId:row.aggregate_id,attempts:row.attempts+1}));
        this.metrics.increment("outbox_published_total");
      }catch(error){
        await c.execute("UPDATE outbox_messages SET attempts=attempts+1,next_attempt_at=now() + (POWER(2, LEAST(attempts, 8)) * interval '1 second') WHERE id=?",[row.id]);
        this.logger.warn(JSON.stringify({event:"outbox_publish_retry",outboxId:row.id,eventType:row.event_type,aggregateId:row.aggregate_id,error:String(error)}));
        this.metrics.increment("outbox_retries_total");
      }
      return true;
    });
  }
}
