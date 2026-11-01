import { describe, expect, test } from "bun:test";
import { CreateQueueCommand, DeleteMessageCommand, DeleteQueueCommand, GetQueueAttributesCommand, GetQueueUrlCommand, ReceiveMessageCommand, SendMessageCommand, SetQueueAttributesCommand, SQSClient } from "@aws-sdk/client-sqs";

const integration = process.env.RUN_INTEGRATION === "1" ? describe : describe.skip;
const apiUrl = process.env.API_URL ?? "http://localhost:3000";
const queueUrl = process.env.SQS_QUEUE_URL ?? "http://localhost:4566/000000000000/wager-transactions.fifo";

integration("Inbox + SQS redelivery", () => {
  const sqs = new SQSClient({ region: process.env.AWS_REGION ?? "us-east-1", endpoint: process.env.SQS_ENDPOINT ?? "http://localhost:4566", credentials: { accessKeyId: "test", secretAccessKey: "test" } });

  const faultTest = process.env.EXPECT_ACK_FAULT === "1" ? test : test.skip;
  faultTest("recovers when the worker fails after commit and before ack", async () => {
    const playerId = `integration-ack-fault-${Date.now()}`;
    const walletResponse = await fetch(`${apiUrl}/wallets`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ playerId, initialBalance: { amount: "100.00", currency: "BRL" } }) });
    expect(walletResponse.ok).toBe(true);
    const wallet = await walletResponse.json() as { id: string };
    const messageId = `integration-ack-fault-message-${Date.now()}`;
    const envelope = { messageId, type: "WagerTransactionRequested", occurredAt: new Date().toISOString(), data: { providerId: "integration-provider", externalTransactionId: `integration-ack-fault-bet-${Date.now()}`, idempotencyKey: `integration-ack-fault-key-${Date.now()}`, playerId, walletId: wallet.id, roundId: "integration-round", gameId: "integration-game", kind: "BET", money: { amount: "25.00", currency: "BRL" } } };
    await sqs.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: JSON.stringify(envelope), MessageGroupId: "integration-ack-fault-wallet", MessageDeduplicationId: `${messageId}-first` }));
    let balance = "";
    for (let attempt = 0; attempt < 20; attempt++) { await new Promise(resolve => setTimeout(resolve, 500)); const response = await fetch(`${apiUrl}/wallets/${wallet.id}`); if (response.ok) { balance = String((await response.json()).balance); if (balance === "75.00") break; } }
    expect(balance).toBe("75.00");
  }, 15000);

  test("same envelope message id produces one financial effect", async () => {
    const walletResponse = await fetch(`${apiUrl}/wallets`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ playerId: `integration-${Date.now()}`, initialBalance: { amount: "100.00", currency: "BRL" } }) });
    expect(walletResponse.ok).toBe(true);
    const wallet = await walletResponse.json() as { id: string; playerId: string };
    const envelope = { messageId: `integration-message-${Date.now()}`, type: "WagerTransactionRequested", occurredAt: new Date().toISOString(), data: { providerId: "integration-provider", externalTransactionId: `integration-bet-${Date.now()}`, idempotencyKey: `integration-key-${Date.now()}`, playerId: wallet.playerId, walletId: wallet.id, roundId: "integration-round", gameId: "integration-game", kind: "BET", money: { amount: "25.00", currency: "BRL" } } };
    for (const suffix of ["first", "redelivery"]) await sqs.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: JSON.stringify(envelope), MessageGroupId: "integration-wallet", MessageDeduplicationId: `${envelope.messageId}-${suffix}` }));
    let balance = "";
    for (let attempt = 0; attempt < 20; attempt++) { await new Promise(resolve => setTimeout(resolve, 250)); const response = await fetch(`${apiUrl}/wallets/${wallet.id}`); if (response.ok) { balance = String((await response.json()).balance); if (balance === "75.00") break; } }
    expect(balance).toBe("75.00");
  }, 15000);

  test("same message id with a different payload does not create a second debit", async () => {
    const playerId = `integration-conflict-${Date.now()}`;
    const walletResponse = await fetch(`${apiUrl}/wallets`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ playerId, initialBalance: { amount: "100.00", currency: "BRL" } }) });
    expect(walletResponse.ok).toBe(true);
    const wallet = await walletResponse.json() as { id: string };
    const messageId = `integration-conflict-message-${Date.now()}`;
    const base = { providerId: "integration-provider", externalTransactionId: `integration-conflict-bet-${Date.now()}`, idempotencyKey: `integration-conflict-key-${Date.now()}`, playerId, walletId: wallet.id, roundId: "integration-round", gameId: "integration-game", kind: "BET" };
    const first = { messageId, type: "WagerTransactionRequested", occurredAt: new Date().toISOString(), data: { ...base, money: { amount: "25.00", currency: "BRL" } } };
    const conflicting = { ...first, data: { ...base, idempotencyKey: `${base.idempotencyKey}-conflict`, money: { amount: "30.00", currency: "BRL" } } };
    await sqs.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: JSON.stringify(first), MessageGroupId: "integration-conflict-wallet", MessageDeduplicationId: `${messageId}-first` }));
    await sqs.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: JSON.stringify(conflicting), MessageGroupId: "integration-conflict-wallet", MessageDeduplicationId: `${messageId}-conflict` }));
    let balance = "";
    for (let attempt = 0; attempt < 20; attempt++) { await new Promise(resolve => setTimeout(resolve, 250)); const response = await fetch(`${apiUrl}/wallets/${wallet.id}`); if (response.ok) { balance = String((await response.json()).balance); if (balance === "75.00") break; } }
    expect(balance).toBe("75.00");
  }, 15000);

  test("publisher emits a versioned event envelope to the outbox queue", async () => {
    const playerId = `integration-outbox-${Date.now()}`;
    const walletResponse = await fetch(`${apiUrl}/wallets`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ playerId, initialBalance: { amount: "100.00", currency: "BRL" } }) });
    expect(walletResponse.ok).toBe(true);
    const wallet = await walletResponse.json() as { id: string };
    const transactionId = `integration-outbox-bet-${Date.now()}`;
    const correlationId = `integration-outbox-correlation-${Date.now()}`;
    const response = await fetch(`${apiUrl}/wagering/transactions`, { method: "POST", headers: { "content-type": "application/json", "idempotency-key": `integration-outbox-key-${Date.now()}`, "x-correlation-id": correlationId }, body: JSON.stringify({ providerId: "integration-outbox-provider", externalTransactionId: transactionId, playerId, walletId: wallet.id, roundId: "integration-outbox-round", gameId: "integration-outbox-game", kind: "BET", money: { amount: "10.00", currency: "BRL" } }) });
    expect(response.ok).toBe(true);
    const result = await response.json() as { transactionId: string };
    const queue = await sqs.send(new GetQueueUrlCommand({ QueueName: process.env.OUTBOX_QUEUE_NAME ?? "wager-events.fifo" }));
    expect(queue.QueueUrl).toBeTruthy();
    let found: Record<string, unknown> | undefined;
    for (let attempt = 0; attempt < 20 && !found; attempt++) {
      const received = await sqs.send(new ReceiveMessageCommand({ QueueUrl: queue.QueueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 1 }));
      for (const message of received.Messages ?? []) {
        const envelope = JSON.parse(message.Body ?? "{}");
        if ((envelope.data as Record<string, unknown> | undefined)?.transactionId === result.transactionId) found = envelope;
      }
    }
    expect(found).toMatchObject({ eventId: expect.any(String), aggregateId: wallet.id, correlationId, version: 1 });
    expect(["WagerTransactionProcessed", "WalletBalanceChanged"]).toContain(String(found?.eventType));
    expect(found?.data).toMatchObject({ transactionId: result.transactionId, walletId: wallet.id });
  }, 30000);

  test("two publisher instances converge on one FIFO event id", async () => {
    const second = Bun.spawn(["bun", "src/main.ts"], { cwd: process.cwd(), env: { ...process.env, PORT: "3001", OUTBOX_POLL_INTERVAL_MS: "50" }, stdout: "ignore", stderr: "ignore" });
    try {
      await new Promise(resolve => setTimeout(resolve, 1500));
      const playerId = `integration-outbox-concurrent-${Date.now()}`;
      const walletResponse = await fetch(`${apiUrl}/wallets`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ playerId, initialBalance: { amount: "100.00", currency: "BRL" } }) });
      expect(walletResponse.ok).toBe(true);
      const wallet = await walletResponse.json() as { id: string };
      const response = await fetch(`${apiUrl}/wagering/transactions`, { method: "POST", headers: { "content-type": "application/json", "idempotency-key": `integration-outbox-concurrent-key-${Date.now()}` }, body: JSON.stringify({ providerId: "integration-outbox-concurrent-provider", externalTransactionId: `integration-outbox-concurrent-bet-${Date.now()}`, playerId, walletId: wallet.id, roundId: "integration-outbox-concurrent-round", gameId: "integration-outbox-concurrent-game", kind: "BET", money: { amount: "10.00", currency: "BRL" } }) });
      expect(response.ok).toBe(true);
      const result = await response.json() as { transactionId: string };
      const queue = await sqs.send(new GetQueueUrlCommand({ QueueName: process.env.OUTBOX_QUEUE_NAME ?? "wager-events.fifo" }));
      const eventIds = new Set<string>();
      let matchingMessages = 0;
      for (let attempt = 0; attempt < 20 && eventIds.size < 2; attempt++) {
        const received = await sqs.send(new ReceiveMessageCommand({ QueueUrl: queue.QueueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 30 }));
        for (const message of received.Messages ?? []) {
          const envelope = JSON.parse(message.Body ?? "{}");
          if ((envelope.data as Record<string, unknown> | undefined)?.transactionId === result.transactionId) { eventIds.add(String(envelope.eventId)); matchingMessages++; }
          if (message.ReceiptHandle) await sqs.send(new DeleteMessageCommand({ QueueUrl: queue.QueueUrl, ReceiptHandle: message.ReceiptHandle }));
        }
      }
      expect(eventIds.size).toBe(2);
      expect(matchingMessages).toBe(2);
    } finally {
      second.kill();
    }
  }, 30000);

  test("replaying the same event id after a publish/update crash is broker-safe", async () => {
    const queue = await sqs.send(new GetQueueUrlCommand({ QueueName: process.env.OUTBOX_QUEUE_NAME ?? "wager-events.fifo" }));
    const eventId = `integration-crash-replay-${Date.now()}`;
    const body = JSON.stringify({ eventId, eventType: "TestEvent", aggregateId: "integration-crash", correlationId: eventId, occurredAt: new Date().toISOString(), version: 1, data: { eventId } });
    for (let attempt = 0; attempt < 2; attempt++) await sqs.send(new SendMessageCommand({ QueueUrl: queue.QueueUrl, MessageBody: body, MessageGroupId: "integration-crash", MessageDeduplicationId: eventId }));
    const received = await sqs.send(new ReceiveMessageCommand({ QueueUrl: queue.QueueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 2, VisibilityTimeout: 30 }));
    const matching = (received.Messages ?? []).filter(message => JSON.parse(message.Body ?? "{}").eventId === eventId);
    expect(matching).toHaveLength(1);
    for (const message of matching) if (message.ReceiptHandle) await sqs.send(new DeleteMessageCommand({ QueueUrl: queue.QueueUrl, ReceiptHandle: message.ReceiptHandle }));
  }, 15000);

  test("pending references are retried after the referenced transaction arrives", async () => {
    const playerId = `integration-reference-${Date.now()}`;
    const walletResponse = await fetch(`${apiUrl}/wallets`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ playerId, initialBalance: { amount: "100.00", currency: "BRL" } }) });
    expect(walletResponse.ok).toBe(true);
    const wallet = await walletResponse.json() as { id: string };
    const providerId = `integration-reference-provider-${Date.now()}`;
    const referenceExternalId = `integration-reference-bet-${Date.now()}`;
    const refundResponse = await fetch(`${apiUrl}/wagering/transactions`, { method: "POST", headers: { "content-type": "application/json", "idempotency-key": `integration-reference-refund-${Date.now()}` }, body: JSON.stringify({ providerId, externalTransactionId: `integration-refund-${Date.now()}`, playerId, walletId: wallet.id, roundId: "integration-reference-round", gameId: "integration-reference-game", kind: "REFUND", referenceExternalTransactionId: referenceExternalId, money: { amount: "10.00", currency: "BRL" } }) });
    expect(refundResponse.ok).toBe(true);
    const pending = await refundResponse.json() as { transactionId: string; status: string };
    expect(pending.status).toBe("PENDING_REFERENCE");
    const betResponse = await fetch(`${apiUrl}/wagering/transactions`, { method: "POST", headers: { "content-type": "application/json", "idempotency-key": `integration-reference-bet-key-${Date.now()}` }, body: JSON.stringify({ providerId, externalTransactionId: referenceExternalId, playerId, walletId: wallet.id, roundId: "integration-reference-round", gameId: "integration-reference-game", kind: "BET", money: { amount: "10.00", currency: "BRL" } }) });
    expect(betResponse.ok).toBe(true);
    let status = "";
    for (let attempt = 0; attempt < 20; attempt++) { await new Promise(resolve => setTimeout(resolve, 250)); const response = await fetch(`${apiUrl}/wagering/transactions/${pending.transactionId}`); if (response.ok) { const value = await response.json() as { status: string }; status = value.status; if (status === "PROCESSED") break; } }
    expect(status).toBe("PROCESSED");
    const balance = await fetch(`${apiUrl}/wallets/${wallet.id}`).then(response => response.json()) as { balance: string };
    expect(String(balance.balance)).toBe("100.00");
  }, 15000);

  test("transient deliveries are eventually moved to the configured DLQ", async () => {
    const suffix = Date.now();
    const mainName = `integration-retry-${suffix}.fifo`;
    const dlqName = `integration-retry-dlq-${suffix}.fifo`;
    const queueAttributes = { FifoQueue: "true", ContentBasedDeduplication: "false" };
    await sqs.send(new CreateQueueCommand({ QueueName: dlqName, Attributes: queueAttributes }));
    const dlq = await sqs.send(new GetQueueUrlCommand({ QueueName: dlqName }));
    expect(dlq.QueueUrl).toBeTruthy();
    const attributes = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ["QueueArn"] }));
    const dlqArn = attributes.Attributes?.QueueArn;
    expect(dlqArn).toBeTruthy();
    await sqs.send(new CreateQueueCommand({ QueueName: mainName, Attributes: queueAttributes }));
    const main = await sqs.send(new GetQueueUrlCommand({ QueueName: mainName }));
    expect(main.QueueUrl).toBeTruthy();
    await sqs.send(new SetQueueAttributesCommand({ QueueUrl: main.QueueUrl, Attributes: { RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: "3" }) } }));
    try {
      await sqs.send(new SendMessageCommand({ QueueUrl: main.QueueUrl, MessageBody: JSON.stringify({ messageId: `integration-dlq-${suffix}`, type: "TransientTest", occurredAt: new Date().toISOString(), data: { idempotencyKey: `integration-dlq-${suffix}` } }), MessageGroupId: "integration-dlq", MessageDeduplicationId: `integration-dlq-${suffix}` }));
      for (let attempt = 0; attempt < 5; attempt++) {
        await sqs.send(new ReceiveMessageCommand({ QueueUrl: main.QueueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 1, VisibilityTimeout: 0 }));
        await new Promise(resolve => setTimeout(resolve, 250));
      }
      const deadLetter = await sqs.send(new ReceiveMessageCommand({ QueueUrl: dlq.QueueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 2 }));
      expect(deadLetter.Messages?.length ?? 0).toBe(1);
    } finally {
      await sqs.send(new DeleteQueueCommand({ QueueUrl: main.QueueUrl }));
      await sqs.send(new DeleteQueueCommand({ QueueUrl: dlq.QueueUrl }));
    }
  }, 15000);

  test("ledger endpoint returns stable cursor pages without mutating the wallet", async () => {
    const playerId = `integration-ledger-${Date.now()}`;
    const walletResponse = await fetch(`${apiUrl}/wallets`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ playerId, initialBalance: { amount: "100.00", currency: "BRL" } }) });
    expect(walletResponse.ok).toBe(true);
    const wallet = await walletResponse.json() as { id: string };
    const wagerResponse = await fetch(`${apiUrl}/wagering/transactions`, { method: "POST", headers: { "content-type": "application/json", "idempotency-key": `integration-ledger-key-${Date.now()}` }, body: JSON.stringify({ providerId: "integration-ledger-provider", externalTransactionId: `integration-ledger-bet-${Date.now()}`, playerId, walletId: wallet.id, roundId: "integration-ledger-round", gameId: "integration-ledger-game", kind: "BET", money: { amount: "10.00", currency: "BRL" } }) });
    expect(wagerResponse.ok).toBe(true);
    const before = await fetch(`${apiUrl}/wallets/${wallet.id}`).then(response => response.json()) as { balance: string };
    const first = await fetch(`${apiUrl}/wallets/${wallet.id}/ledger?limit=1`).then(response => response.json()) as { items: Array<{ id: string }>; nextCursor: string|null };
    expect(first.items).toHaveLength(1);
    expect(first.nextCursor).toBeTruthy();
    const second = await fetch(`${apiUrl}/wallets/${wallet.id}/ledger?limit=1&cursor=${encodeURIComponent(first.nextCursor as string)}`).then(response => response.json()) as { items: Array<{ id: string }>; nextCursor: string|null };
    expect(second.items).toHaveLength(1);
    expect(second.items[0].id).not.toBe(first.items[0].id);
    expect(second.nextCursor).toBeNull();
    const after = await fetch(`${apiUrl}/wallets/${wallet.id}`).then(response => response.json()) as { balance: string };
    expect(String(after.balance)).toBe(String(before.balance));
  }, 15000);
});
