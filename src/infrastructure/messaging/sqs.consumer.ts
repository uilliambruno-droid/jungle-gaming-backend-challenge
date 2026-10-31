import { BadRequestException, ConflictException, Injectable, Logger, OnModuleDestroy, OnModuleInit, UnprocessableEntityException } from "@nestjs/common";
import { DeleteMessageCommand, GetQueueUrlCommand, ReceiveMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { WagerService } from "../../application/wager.service.js";
import { MetricsService } from "../observability/metrics.service.js";

const CONSUMER_NAME = process.env.CONSUMER_NAME ?? "wager-processor";
interface SqsEnvelope { messageId?: string; type: string; occurredAt: string; data: Record<string, unknown> & { idempotencyKey: string }; }
function isPermanent(error: unknown): boolean { return error instanceof ConflictException || error instanceof BadRequestException || error instanceof UnprocessableEntityException; }

@Injectable()
export class SqsConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SqsConsumer.name);
  private readonly client = new SQSClient({ region: process.env.AWS_REGION ?? "us-east-1", endpoint: process.env.SQS_ENDPOINT, credentials: { accessKeyId: process.env.SQS_ENDPOINT?.includes("localhost") ? "test" : (process.env.AWS_ACCESS_KEY_ID ?? "test"), secretAccessKey: process.env.SQS_ENDPOINT?.includes("localhost") ? "test" : (process.env.AWS_SECRET_ACCESS_KEY ?? "test") } });
  private running = true;
  private loopPromise?: Promise<void>;
  private queueUrl?: string;
  private ackFaultInjected = false;
  constructor(private readonly service: WagerService, private readonly metrics: MetricsService) {}

  async onModuleInit(): Promise<void> { this.queueUrl = await this.resolveQueueUrl(); this.loopPromise = this.consume(); }
  async onModuleDestroy(): Promise<void> { this.running = false; await this.loopPromise; this.client.destroy(); }

  private async resolveQueueUrl(): Promise<string> {
    const response = await this.client.send(new GetQueueUrlCommand({ QueueName: process.env.SQS_QUEUE_NAME ?? "wager-transactions.fifo" }));
    if (!response.QueueUrl) throw new Error("SQS_QUEUE_URL_NOT_FOUND");
    return response.QueueUrl;
  }

  private async consume(): Promise<void> {
    while (this.running) {
      try {
        const response = await this.client.send(new ReceiveMessageCommand({ QueueUrl: this.queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 10, VisibilityTimeout: Number(process.env.SQS_VISIBILITY_TIMEOUT_SECONDS ?? "30") }));
        for (const message of response.Messages ?? []) { if (!this.running) break; await this.handleMessage(message); }
      } catch (error) {
        if (this.running) this.logger.error(JSON.stringify({ event: "sqs_receive_failed", error: String(error) }));
      }
    }
  }

  private async handleMessage(message: { Body?: string; ReceiptHandle?: string; MessageId?: string }): Promise<void> {
    if (!message.Body || !message.ReceiptHandle) return;
    try {
      const envelope = this.parseEnvelope(message.Body);
      const messageId = envelope.messageId ?? message.MessageId;
      if (!messageId) throw new BadRequestException("SQS_MESSAGE_ID_REQUIRED");
      const result = await this.service.submit({ ...envelope.data, idempotencyKey: envelope.data.idempotencyKey } as never, { messageId, consumerName: CONSUMER_NAME });
      if (process.env.SQS_TEST_FAIL_AFTER_COMMIT_BEFORE_ACK === "1" && !this.ackFaultInjected) {
        this.ackFaultInjected = true;
        throw new Error("TEST_FAILURE_AFTER_COMMIT_BEFORE_ACK");
      }
      await this.client.send(new DeleteMessageCommand({ QueueUrl: this.queueUrl, ReceiptHandle: message.ReceiptHandle }));
      this.metrics.increment("sqs_processed_total");
      this.logger.log(JSON.stringify({ event: "sqs_message_processed", messageId, transactionId: result.transactionId, status: result.status }));
    } catch (error) {
      if (isPermanent(error)) {
        await this.client.send(new DeleteMessageCommand({ QueueUrl: this.queueUrl, ReceiptHandle: message.ReceiptHandle }));
        this.metrics.increment("sqs_rejected_total");
        this.logger.warn(JSON.stringify({ event: "sqs_message_rejected", category: "permanent", messageId: message.MessageId, error: String(error) }));
        return;
      }
      this.logger.warn(JSON.stringify({ event: "sqs_message_retry", category: "transient", messageId: message.MessageId, error: String(error) }));
      this.metrics.increment("sqs_retries_total");
      // Não remover: a visibilidade expira e a mensagem é redeliverada pela SQS.
    }
  }

  private parseEnvelope(body: string): SqsEnvelope {
    const value = JSON.parse(body) as Partial<SqsEnvelope>;
    if (!value.type || !value.data || typeof value.data.idempotencyKey !== "string") throw new BadRequestException("INVALID_SQS_ENVELOPE");
    return value as SqsEnvelope;
  }
}
