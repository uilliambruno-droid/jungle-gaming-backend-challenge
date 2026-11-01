import {
  BadRequestException,
  ConflictException,
  HttpException,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
  UnprocessableEntityException,
} from "@nestjs/common";
import { DeleteMessageCommand, GetQueueUrlCommand, ReceiveMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { WagerInput, WagerService } from "../../application/wager.service.js";
import { MetricsService } from "../observability/metrics.service.js";

const CONSUMER_NAME = process.env.CONSUMER_NAME ?? "wager-processor";
interface SqsEnvelope {
  messageId?: string;
  correlationId?: string;
  causationId?: string;
  type: string;
  occurredAt: string;
  data: WagerInput;
}

function isConflict(error: unknown): boolean {
  return error instanceof ConflictException || error instanceof UnprocessableEntityException;
}

function errorCode(error: unknown): string {
  if (error instanceof HttpException) {
    const response = error.getResponse();
    if (typeof response === "string") return response;
    if (response && typeof response === "object" && "message" in response) {
      const message = (response as { message: unknown }).message;
      if (typeof message === "string") return message;
    }
  }
  if (error && typeof error === "object" && "name" in error) {
    return String((error as { name: unknown }).name);
  }
  return "SQS_PROCESSING_ERROR";
}

function wait(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

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
  async onModuleDestroy(): Promise<void> {
    this.running = false;
    await this.loopPromise;
    this.client.destroy();
  }

  private async resolveQueueUrl(): Promise<string> {
    const response = await this.client.send(new GetQueueUrlCommand({ QueueName: process.env.SQS_QUEUE_NAME ?? "wager-transactions.fifo" }));
    if (!response.QueueUrl) throw new Error("SQS_QUEUE_URL_NOT_FOUND");
    return response.QueueUrl;
  }

  private async consume(): Promise<void> {
    while (this.running) {
      try {
        const response = await this.client.send(new ReceiveMessageCommand({
          QueueUrl: this.queueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 10,
          VisibilityTimeout: Number(process.env.SQS_VISIBILITY_TIMEOUT_SECONDS ?? "30"),
        }));
        for (const message of response.Messages ?? []) {
          if (!this.running) break;
          await this.handleMessage(message);
        }
      } catch (error) {
        if (this.running) {
          this.logger.error(JSON.stringify({
            event: "sqs_receive_failed",
            correlationId: "sqs-consumer",
            messageId: null,
            transactionId: null,
            walletId: null,
            providerId: null,
            category: "transient",
            errorCode: errorCode(error),
          }));
          this.metrics.increment("sqs_receive_errors_total");
          await wait(Number(process.env.SQS_ERROR_DELAY_MS ?? "1000"));
        }
      }
    }
  }

  private async handleMessage(message: {
    Body?: string;
    ReceiptHandle?: string;
    MessageId?: string;
  }): Promise<void> {
    if (!message.Body || !message.ReceiptHandle) return;
    let envelope: SqsEnvelope | undefined;
    let messageId = message.MessageId ?? null;
    try {
      envelope = this.parseEnvelope(message.Body);
      messageId = envelope.messageId ?? message.MessageId ?? null;
      if (!messageId) throw new BadRequestException("SQS_MESSAGE_ID_REQUIRED");
      const context = {
        correlationId: envelope.correlationId ?? messageId,
        causationId: envelope.causationId ?? messageId,
        messageId,
      };
      const result = await this.service.submit(
        envelope.data,
        { messageId, consumerName: CONSUMER_NAME },
        context,
      );
      if (process.env.SQS_TEST_FAIL_AFTER_COMMIT_BEFORE_ACK === "1" && !this.ackFaultInjected) {
        this.ackFaultInjected = true;
        throw new Error("TEST_FAILURE_AFTER_COMMIT_BEFORE_ACK");
      }
      await this.client.send(new DeleteMessageCommand({ QueueUrl: this.queueUrl, ReceiptHandle: message.ReceiptHandle }));
      this.metrics.increment("sqs_processed_total");
      this.logger.log(JSON.stringify({
        event: "sqs_message_processed",
        correlationId: context.correlationId,
        messageId,
        transactionId: result.transactionId,
        walletId: envelope.data.walletId,
        providerId: envelope.data.providerId,
        status: result.status,
      }));
    } catch (error) {
      if (isConflict(error)) {
        await this.client.send(new DeleteMessageCommand({ QueueUrl: this.queueUrl, ReceiptHandle: message.ReceiptHandle }));
        this.metrics.increment("sqs_conflicts_total");
        this.logger.warn(JSON.stringify({
          event: "sqs_message_conflict",
          correlationId: envelope?.correlationId ?? messageId ?? "unknown",
          messageId,
          transactionId: null,
          walletId: envelope?.data.walletId ?? null,
          providerId: envelope?.data.providerId ?? null,
          category: "permanent",
          errorCode: errorCode(error),
        }));
        return;
      }
      if (error instanceof BadRequestException) {
        this.metrics.increment("sqs_dlq_candidates_total");
        this.logger.warn(JSON.stringify({
          event: "sqs_message_dlq_pending",
          correlationId: envelope?.correlationId ?? messageId ?? "unknown",
          messageId,
          transactionId: null,
          walletId: envelope?.data.walletId ?? null,
          providerId: envelope?.data.providerId ?? null,
          category: "permanent",
          errorCode: errorCode(error),
        }));
        return;
      }
      this.logger.warn(JSON.stringify({
        event: "sqs_message_retry",
        correlationId: envelope?.correlationId ?? messageId ?? "unknown",
        messageId,
        transactionId: null,
        walletId: envelope?.data.walletId ?? null,
        providerId: envelope?.data.providerId ?? null,
        category: "transient",
        errorCode: errorCode(error),
      }));
      this.metrics.increment("sqs_retries_total");
    }
  }

  private parseEnvelope(body: string): SqsEnvelope {
    let value: Partial<SqsEnvelope>;
    try {
      value = JSON.parse(body) as Partial<SqsEnvelope>;
    } catch {
      throw new BadRequestException("INVALID_SQS_ENVELOPE");
    }
    if (
      value.type !== "WagerTransactionRequested"
      || !value.data
      || typeof value.data.idempotencyKey !== "string"
      || !value.data.idempotencyKey
      || typeof value.data.providerId !== "string"
      || typeof value.data.walletId !== "string"
    ) {
      throw new BadRequestException("INVALID_SQS_ENVELOPE");
    }
    return value as SqsEnvelope;
  }
}
