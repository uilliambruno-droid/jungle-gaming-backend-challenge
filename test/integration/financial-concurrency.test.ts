import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { createServer } from "node:net";
import { MikroORM } from "@mikro-orm/postgresql";
import config from "../../src/infrastructure/mikro-orm.config.js";

const integration = process.env.RUN_INTEGRATION === "1" ? describe : describe.skip;
const apiUrl = process.env.API_URL ?? "http://localhost:3000";
const endpoint = process.env.SQS_ENDPOINT ?? "http://localhost:4566";
const sqs = new SQSClient({
  region: process.env.AWS_REGION ?? "us-east-1",
  endpoint,
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});

interface WalletResponse {
  id: string;
  playerId: string;
}

interface WagerResponse {
  transactionId: string;
  status: string;
  failureCode?: string;
  idempotentReplay?: boolean;
  balance?: { amount: string; currency: string };
}

interface DatabaseRow {
  count: string;
}

integration("financial concurrency and persistence", () => {
  let orm: MikroORM;

  beforeAll(async () => {
    orm = await MikroORM.init(config);
  });

  afterAll(async () => {
    if (orm) await orm.close(true);
    sqs.destroy();
  });

  async function query<T extends object>(sql: string, params: unknown[] = []): Promise<T[]> {
    return orm.em.getConnection().execute<T[]>(sql, params);
  }

  async function createWallet(baseUrl: string, initialBalance = "100.00"): Promise<WalletResponse> {
    const response = await fetch(`${baseUrl}/wallets`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        playerId: `integration-financial-${crypto.randomUUID()}`,
        initialBalance: { amount: initialBalance, currency: "BRL" },
      }),
    });
    expect(response.ok).toBe(true);
    return response.json() as Promise<WalletResponse>;
  }

  async function submitBet(baseUrl: string, wallet: WalletResponse, amount: string, key: string): Promise<WagerResponse> {
    const response = await fetch(`${baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: { "content-type": "application/json", "idempotency-key": key },
      body: JSON.stringify({
        providerId: "integration-concurrency-provider",
        externalTransactionId: `external-${key}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: "integration-concurrency-round",
        gameId: "integration-concurrency-game",
        kind: "BET",
        money: { amount, currency: "BRL" },
      }),
    });
    const result = await response.json() as WagerResponse;
    expect(response.status).toBe(result.status === "REJECTED" ? 422 : result.status === "PENDING_REFERENCE" ? 202 : 200);
    return result;
  }

  async function createQueue(name: string): Promise<string> {
    await sqs.send(new CreateQueueCommand({
      QueueName: name,
      Attributes: {
        FifoQueue: "true",
        ContentBasedDeduplication: "false",
        VisibilityTimeout: "2",
      },
    }));
    const { QueueUrl } = await sqs.send(new GetQueueUrlCommand({ QueueName: name }));
    if (!QueueUrl) throw new Error(`INTEGRATION_QUEUE_URL_NOT_FOUND:${name}`);
    return QueueUrl;
  }

  async function deleteQueue(queueUrl: string): Promise<void> {
    await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
  }

  async function unusedPort(): Promise<number> {
    const server = createServer();
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("INTEGRATION_PORT_ALLOCATION_FAILED");
    const { port } = address;
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    return port;
  }

  async function startInstance(queueName: string, options: {
    consumerName: string;
    failAfterCommit?: boolean;
  }): Promise<{ process: ReturnType<typeof Bun.spawn>; url: string }> {
    const port = await unusedPort();
    const child = Bun.spawn(["bun", "src/main.ts"], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        PORT: String(port),
        SQS_QUEUE_NAME: queueName,
        SQS_VISIBILITY_TIMEOUT_SECONDS: "2",
        CONSUMER_NAME: options.consumerName,
        SQS_TEST_FAIL_AFTER_COMMIT_BEFORE_ACK: options.failAfterCommit ? "1" : "0",
      },
      stdout: "ignore",
      stderr: "inherit",
    });
    const url = `http://127.0.0.1:${port}`;
    try {
      await waitUntil(async () => {
        if (child.exitCode !== null) throw new Error(`INTEGRATION_INSTANCE_EXITED:${child.exitCode}`);
        try {
          const response = await fetch(`${url}/health/ready`);
          return response.ok;
        } catch {
          return false;
        }
      }, 20_000, "INTEGRATION_INSTANCE_NOT_READY");
    } catch (error) {
      child.kill();
      await child.exited;
      throw error;
    }
    return { process: child, url };
  }

  async function stopInstance(instance: { process: ReturnType<typeof Bun.spawn> }): Promise<void> {
    if (instance.process.exitCode === null) instance.process.kill("SIGTERM");
    await instance.process.exited;
  }

  async function waitUntil(
    condition: () => Promise<boolean>,
    timeoutMs: number,
    errorMessage: string,
  ): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (await condition()) return;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    throw new Error(errorMessage);
  }

  async function waitForQueueToDrain(queueUrl: string): Promise<void> {
    await waitUntil(async () => {
      const { Attributes } = await sqs.send(new GetQueueAttributesCommand({
        QueueUrl: queueUrl,
        AttributeNames: ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
      }));
      return Attributes?.ApproximateNumberOfMessages === "0"
        && Attributes.ApproximateNumberOfMessagesNotVisible === "0";
    }, 30_000, "INTEGRATION_QUEUE_DID_NOT_DRAIN");
  }

  async function assertLedgerMatchesWallet(
    walletId: string,
    expectedBalance: string,
    baseUrl = apiUrl,
  ): Promise<void> {
    const rows = await query<{ balance: string; ledger_balance: string }>(
      `SELECT w.balance::text AS balance,
              COALESCE(SUM(CASE WHEN l.direction = 'CREDIT' THEN l.amount ELSE -l.amount END), 0)::text AS ledger_balance
       FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
       WHERE w.id = ? GROUP BY w.id`,
      [walletId],
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].balance).toBe(expectedBalance);
    expect(rows[0].ledger_balance).toBe(expectedBalance);
    const response = await fetch(`${baseUrl}/wallets/${walletId}/reconciliation`, { method: "POST" });
    expect(response.ok).toBe(true);
    const reconciliation = await response.json() as { consistent: boolean; calculatedBalance: { amount: string } };
    expect(reconciliation.consistent).toBe(true);
    expect(reconciliation.calculatedBalance.amount).toBe(expectedBalance);
  }

  test("migrations enforce wallet, ledger, Inbox and Outbox invariants", async () => {
    const migrations = await query<{ name: string }>(
      "SELECT name FROM mikro_orm_migrations ORDER BY name",
    );
    expect(migrations.map(row => row.name)).toEqual([
      "Migration202610310001",
      "Migration202610310002",
      "Migration202611010003",
    ]);

    const constraints = await query<{ conname: string }>(
      `SELECT conname FROM pg_constraint
       WHERE conrelid IN ('wallets'::regclass, 'wallet_ledger_entries'::regclass)`,
    );
    const names = new Set(constraints.map(row => row.conname));
    expect(names.has("wallets_balance_nonnegative")).toBe(true);
    expect(names.has("wallets_player_currency_unique")).toBe(true);
    expect(names.has("wallet_ledger_entries_transaction_id_wallet_id_key")).toBe(true);
    expect(names.has("wallet_ledger_balance_matches_direction")).toBe(true);
    const reversalIndex = await query<{ indexname: string }>(
      "SELECT indexname FROM pg_indexes WHERE indexname = 'wager_processed_reversal_unique'",
    );
    expect(reversalIndex).toHaveLength(1);

    const triggers = await query<{ tgname: string }>(
      `SELECT tgname FROM pg_trigger
       WHERE tgrelid = 'wallet_ledger_entries'::regclass AND NOT tgisinternal`,
    );
    expect(triggers.map(row => row.tgname)).toContain("wallet_ledger_immutable");

    const wallet = await createWallet(apiUrl);
    const ledger = await query<{ id: string }>(
      "SELECT id FROM wallet_ledger_entries WHERE wallet_id = ?",
      [wallet.id],
    );
    expect(ledger).toHaveLength(1);

    await orm.em.transactional(async transactionalEm => {
      const connection = transactionalEm.getConnection();
      const context = transactionalEm.getTransactionContext();
      const execute = (sql: string, params: unknown[] = []) =>
        connection.execute(sql, params, "all", context);
      await execute("SAVEPOINT wallet_constraint_probe");
      let negativeBalanceRejected = false;
      try {
        await execute("UPDATE wallets SET balance = -1 WHERE id = ?", [wallet.id]);
      } catch {
        negativeBalanceRejected = true;
        await execute("ROLLBACK TO SAVEPOINT wallet_constraint_probe");
      }
      expect(negativeBalanceRejected).toBe(true);
      await execute("RELEASE SAVEPOINT wallet_constraint_probe");

      await execute("SAVEPOINT ledger_immutability_probe");
      let ledgerMutationRejected = false;
      try {
        await execute("UPDATE wallet_ledger_entries SET amount = 0 WHERE id = ?", [ledger[0].id]);
      } catch {
        ledgerMutationRejected = true;
        await execute("ROLLBACK TO SAVEPOINT ledger_immutability_probe");
      }
      expect(ledgerMutationRejected).toBe(true);
      await execute("RELEASE SAVEPOINT ledger_immutability_probe");
    });

    await assertLedgerMatchesWallet(wallet.id, "100.00");
  }, 15_000);

  test("50 simultaneous deliveries of one message make exactly one financial effect", async () => {
    const suffix = crypto.randomUUID();
    const queueName = `integration-duplicates-${suffix}.fifo`;
    const queueUrl = await createQueue(queueName);
    const instances: Array<{ process: ReturnType<typeof Bun.spawn>; url: string }> = [];
    try {
      for (let index = 0; index < 3; index++) {
        instances.push(await startInstance(queueName, { consumerName: "integration-duplicate-consumer" }));
      }

      const wallet = await createWallet(instances[0].url);
      const messageId = `integration-duplicate-message-${suffix}`;
      const idempotencyKey = `integration-duplicate-key-${suffix}`;
      const envelope = {
        messageId,
        type: "WagerTransactionRequested",
        occurredAt: new Date().toISOString(),
        data: {
          providerId: "integration-duplicate-provider",
          externalTransactionId: `integration-duplicate-bet-${suffix}`,
          idempotencyKey,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: "integration-duplicate-round",
          gameId: "integration-duplicate-game",
          kind: "BET",
          money: { amount: "25.00", currency: "BRL" },
        },
      };

      await Promise.all(Array.from({ length: 50 }, (_, index) => sqs.send(new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(envelope),
        MessageGroupId: `wallet-delivery-${index}`,
        MessageDeduplicationId: `delivery-${index}-${suffix}`,
      }))));
      await waitForQueueToDrain(queueUrl);

      const transactions = await query<{ id: string }>(
        "SELECT id FROM wager_transactions WHERE idempotency_key = ?",
        [idempotencyKey],
      );
      expect(transactions).toHaveLength(1);
      const inbox = await query<{ processed_at: Date | null }>(
        "SELECT processed_at FROM inbox_messages WHERE message_id = ? AND consumer_name = ?",
        [messageId, "integration-duplicate-consumer"],
      );
      expect(inbox).toHaveLength(1);
      expect(inbox[0].processed_at).toBeTruthy();

      const ledger = await query<DatabaseRow>(
        "SELECT COUNT(*)::text AS count FROM wallet_ledger_entries WHERE transaction_id = ?",
        [transactions[0].id],
      );
      expect(ledger[0].count).toBe("1");
      const outbox = await query<DatabaseRow>(
        "SELECT COUNT(*)::text AS count FROM outbox_messages WHERE payload->'data'->>'transactionId' = ?",
        [transactions[0].id],
      );
      expect(outbox[0].count).toBe("2");
      await assertLedgerMatchesWallet(wallet.id, "75.00", instances[0].url);
    } finally {
      await Promise.all(instances.map(stopInstance));
      await deleteQueue(queueUrl);
    }
  }, 60_000);

  test("concurrent bets across two instances serialize wallet updates atomically", async () => {
    const suffix = crypto.randomUUID();
    const queueName = `integration-bets-${suffix}.fifo`;
    const queueUrl = await createQueue(queueName);
    const instances: Array<{ process: ReturnType<typeof Bun.spawn>; url: string }> = [];
    try {
      instances.push(await startInstance(queueName, { consumerName: `integration-bets-${suffix}` }));
      instances.push(await startInstance(queueName, { consumerName: `integration-bets-${suffix}` }));
      const wallet = await createWallet(instances[0].url);
      const results = await Promise.all([
        submitBet(instances[0].url, wallet, "80.00", `integration-bet-a-${suffix}`),
        submitBet(instances[1].url, wallet, "80.00", `integration-bet-b-${suffix}`),
      ]);

      expect(results.map(result => result.status).sort()).toEqual(["PROCESSED", "REJECTED"]);
      expect(results.find(result => result.status === "REJECTED")?.failureCode).toBe("INSUFFICIENT_FUNDS");
      const transactions = await query<{ id: string; status: string }>(
        `SELECT id, status FROM wager_transactions
         WHERE idempotency_key IN (?, ?) ORDER BY status`,
        [`integration-bet-a-${suffix}`, `integration-bet-b-${suffix}`],
      );
      expect(transactions).toHaveLength(2);
      expect(transactions.filter(row => row.status === "PROCESSED")).toHaveLength(1);
      expect(transactions.filter(row => row.status === "REJECTED")).toHaveLength(1);

      const ledger = await query<DatabaseRow>(
        `SELECT COUNT(*)::text AS count FROM wallet_ledger_entries
         WHERE transaction_id IN (?, ?)`,
        transactions.map(row => row.id),
      );
      expect(ledger[0].count).toBe("1");
      const outbox = await query<DatabaseRow>(
        `SELECT COUNT(*)::text AS count FROM outbox_messages
         WHERE payload->'data'->>'transactionId' IN (?, ?)`,
        transactions.map(row => row.id),
      );
      expect(outbox[0].count).toBe("3");
      await assertLedgerMatchesWallet(wallet.id, "20.00", instances[0].url);
    } finally {
      await Promise.all(instances.map(stopInstance));
      await deleteQueue(queueUrl);
    }
  }, 45_000);

  test("three instances process distinct wallets concurrently", async () => {
    const suffix = crypto.randomUUID();
    const queueName = `integration-three-instances-${suffix}.fifo`;
    const queueUrl = await createQueue(queueName);
    const instances: Array<{ process: ReturnType<typeof Bun.spawn>; url: string }> = [];
    try {
      for (let index = 0; index < 3; index++) {
        instances.push(await startInstance(queueName, {
          consumerName: `integration-three-wallets-${suffix}`,
        }));
      }
      const wallets = await Promise.all(instances.map(instance => createWallet(instance.url)));
      const results = await Promise.all(wallets.map((wallet, index) =>
        submitBet(instances[index].url, wallet, "20.00", `integration-three-wallet-bet-${suffix}-${index}`),
      ));

      expect(results.map(result => result.status)).toEqual(["PROCESSED", "PROCESSED", "PROCESSED"]);
      for (let index = 0; index < wallets.length; index++) {
        await assertLedgerMatchesWallet(wallets[index].id, "80.00", instances[index].url);
      }
    } finally {
      await Promise.all(instances.map(stopInstance));
      await deleteQueue(queueUrl);
    }
  }, 60_000);

  test("concurrent pending refunds cannot reverse the same transaction twice", async () => {
    const suffix = crypto.randomUUID();
    const queueName = `integration-pending-reversals-${suffix}.fifo`;
    const queueUrl = await createQueue(queueName);
    const instances: Array<{ process: ReturnType<typeof Bun.spawn>; url: string }> = [];
    try {
      for (let index = 0; index < 3; index++) {
        instances.push(await startInstance(queueName, {
          consumerName: `integration-pending-reversal-${suffix}`,
        }));
      }
      const wallet = await createWallet(instances[0].url);
      const betKey = `integration-pending-reversal-bet-${suffix}`;
      const referenceExternalId = `external-${betKey}`;
      const reversalKeys = [
        `integration-pending-refund-a-${suffix}`,
        `integration-pending-refund-b-${suffix}`,
      ];
      const pendingResponses = await Promise.all(reversalKeys.map((key, index) =>
        fetch(`${instances[index].url}/wagering/transactions`, {
          method: "POST",
          headers: { "content-type": "application/json", "idempotency-key": key },
          body: JSON.stringify({
            providerId: "integration-concurrency-provider",
            externalTransactionId: `integration-pending-refund-${index}-${suffix}`,
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: "integration-concurrency-round",
            gameId: "integration-concurrency-game",
            kind: "REFUND",
            referenceExternalTransactionId: referenceExternalId,
            money: { amount: "20.00", currency: "BRL" },
          }),
        }),
      ));
      expect(pendingResponses.map(response => response.status)).toEqual([202, 202]);
      const pending = await Promise.all(pendingResponses.map(response => response.json() as Promise<WagerResponse>));
      expect(pending.map(result => result.status)).toEqual(["PENDING_REFERENCE", "PENDING_REFERENCE"]);

      const bet = await submitBet(instances[2].url, wallet, "20.00", betKey);
      expect(bet.status).toBe("PROCESSED");
      await waitUntil(async () => {
        const rows = await query<{ status: string; failure_code: string | null }>(
          "SELECT status,failure_code FROM wager_transactions WHERE idempotency_key IN (?,?) ORDER BY idempotency_key",
          reversalKeys,
        );
        return rows.length === 2 && rows.every(row => row.status !== "PENDING_REFERENCE");
      }, 20_000, "INTEGRATION_PENDING_REVERSALS_NOT_COMPLETED");

      const reversals = await query<{ status: string; failure_code: string | null }>(
        "SELECT status,failure_code FROM wager_transactions WHERE idempotency_key IN (?,?)",
        reversalKeys,
      );
      expect(reversals.filter(row => row.status === "PROCESSED")).toHaveLength(1);
      expect(reversals.filter(row => row.status === "REJECTED" && row.failure_code === "DUPLICATE_REVERSAL")).toHaveLength(1);
      const reversalLedger = await query<DatabaseRow>(
        "SELECT COUNT(*)::text AS count FROM wallet_ledger_entries WHERE transaction_id IN (?,?)",
        pending.map(result => result.transactionId),
      );
      expect(reversalLedger[0].count).toBe("1");
      await assertLedgerMatchesWallet(wallet.id, "100.00", instances[0].url);
    } finally {
      await Promise.all(instances.map(stopInstance));
      await deleteQueue(queueUrl);
    }
  }, 60_000);

  test("HTTP responses preserve replay snapshots and distinguish idempotency conflicts", async () => {
    const wallet = await createWallet(apiUrl);
    const key = `integration-replay-snapshot-${crypto.randomUUID()}`;
    const missingKey = await fetch(`${apiUrl}/wagering/transactions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        providerId: "integration-concurrency-provider",
        externalTransactionId: `external-missing-key-${key}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: "integration-concurrency-round",
        gameId: "integration-concurrency-game",
        kind: "BET",
        money: { amount: "1.00", currency: "BRL" },
      }),
    });
    expect(missingKey.status).toBe(400);

    const original = await submitBet(apiUrl, wallet, "20.00", key);
    expect(original.status).toBe("PROCESSED");
    const originalBalance = await query<{ response_balance: string }>(
      "SELECT response_balance::text AS response_balance FROM wager_transactions WHERE id=?",
      [original.transactionId],
    );
    expect(originalBalance[0].response_balance).toBe("80.00");

    const changed = await fetch(`${apiUrl}/wagering/transactions`, {
      method: "POST",
      headers: { "content-type": "application/json", "idempotency-key": key },
      body: JSON.stringify({
        providerId: "integration-concurrency-provider",
        externalTransactionId: `external-${key}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: "integration-concurrency-round",
        gameId: "integration-concurrency-game",
        kind: "BET",
        money: { amount: "25.00", currency: "BRL" },
      }),
    });
    expect(changed.status).toBe(409);

    const win = await fetch(`${apiUrl}/wagering/transactions`, {
      method: "POST",
      headers: { "content-type": "application/json", "idempotency-key": `${key}-win` },
      body: JSON.stringify({
        providerId: "integration-concurrency-provider",
        externalTransactionId: `external-${key}-win`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: "integration-concurrency-round",
        gameId: "integration-concurrency-game",
        kind: "WIN",
        money: { amount: "5.00", currency: "BRL" },
      }),
    });
    expect(win.status).toBe(200);
    expect((await win.json() as WagerResponse).balance?.amount).toBe("85.00");

    const replay = await submitBet(apiUrl, wallet, "20.00", key);
    expect(replay.transactionId).toBe(original.transactionId);
    expect(replay.idempotentReplay).toBe(true);
    expect(replay.balance?.amount).toBe("80.00");

    const metricsResponse = await fetch(`${apiUrl}/health/metrics`);
    const metrics = await metricsResponse.json() as { metrics: Record<string, number> };
    for (const name of [
      "transactions_processed_total",
      "transactions_rejected_total",
      "transactions_pending_reference_total",
      "sqs_retries_total",
      "sqs_dlq_messages",
      "lock_conflicts_total",
      "wager_submission_latency_ms_count",
      "outbox_publish_latency_ms_count",
      "outbox_pending_total",
      "outbox_lag_seconds",
    ]) {
      expect(metrics.metrics[name]).toBeDefined();
    }
  }, 30_000);

  test("reconciliation reports a real signed difference without changing the wallet", async () => {
    const wallet = await createWallet(apiUrl, "10.00");
    await orm.em.getConnection().execute("UPDATE wallets SET balance=? WHERE id=?", ["8.00", wallet.id]);
    try {
      const response = await fetch(`${apiUrl}/wallets/${wallet.id}/reconciliation`, { method: "POST" });
      expect(response.status).toBe(200);
      const result = await response.json() as {
        consistent: boolean;
        storedBalance: { amount: string };
        calculatedBalance: { amount: string };
        difference: { amount: string };
      };
      expect(result).toMatchObject({
        consistent: false,
        storedBalance: { amount: "8.00" },
        calculatedBalance: { amount: "10.00" },
        difference: { amount: "-2.00" },
      });
      const metricsResponse = await fetch(`${apiUrl}/health/metrics`);
      const metrics = await metricsResponse.json() as { metrics: Record<string, number> };
      expect(metrics.metrics.reconciliation_mismatches_total).toBeGreaterThan(0);
    } finally {
      await orm.em.getConnection().execute("UPDATE wallets SET balance=? WHERE id=?", ["10.00", wallet.id]);
    }
    await assertLedgerMatchesWallet(wallet.id, "10.00");
  }, 30_000);

  test("Inbox and financial effects survive a worker restart after commit-before-ack failure", async () => {
    const suffix = crypto.randomUUID();
    const queueName = `integration-restart-${suffix}.fifo`;
    const queueUrl = await createQueue(queueName);
    const consumerName = `integration-restart-consumer-${suffix}`;
    let first: { process: ReturnType<typeof Bun.spawn>; url: string } | undefined;
    let restarted: { process: ReturnType<typeof Bun.spawn>; url: string } | undefined;
    try {
      first = await startInstance(queueName, { consumerName, failAfterCommit: true });
      const wallet = await createWallet(first.url);
      const messageId = `integration-restart-message-${suffix}`;
      const idempotencyKey = `integration-restart-key-${suffix}`;
      await sqs.send(new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify({
          messageId,
          type: "WagerTransactionRequested",
          occurredAt: new Date().toISOString(),
          data: {
            providerId: "integration-restart-provider",
            externalTransactionId: `integration-restart-bet-${suffix}`,
            idempotencyKey,
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: "integration-restart-round",
            gameId: "integration-restart-game",
            kind: "BET",
            money: { amount: "25.00", currency: "BRL" },
          },
        }),
        MessageGroupId: "integration-restart-wallet",
        MessageDeduplicationId: `integration-restart-delivery-${suffix}`,
      }));

      await waitUntil(async () => {
        const rows = await query<DatabaseRow>(
          "SELECT COUNT(*)::text AS count FROM wager_transactions WHERE idempotency_key = ?",
          [idempotencyKey],
        );
        return rows[0].count === "1";
      }, 15_000, "INTEGRATION_COMMIT_NOT_OBSERVED");
      await stopInstance(first);
      first = undefined;

      restarted = await startInstance(queueName, { consumerName });
      await waitForQueueToDrain(queueUrl);
      const transactions = await query<{ id: string }>(
        "SELECT id FROM wager_transactions WHERE idempotency_key = ?",
        [idempotencyKey],
      );
      expect(transactions).toHaveLength(1);
      const inbox = await query<{ processed_at: Date | null }>(
        "SELECT processed_at FROM inbox_messages WHERE message_id = ? AND consumer_name = ?",
        [messageId, consumerName],
      );
      expect(inbox).toHaveLength(1);
      expect(inbox[0].processed_at).toBeTruthy();
      const ledger = await query<DatabaseRow>(
        "SELECT COUNT(*)::text AS count FROM wallet_ledger_entries WHERE transaction_id = ?",
        [transactions[0].id],
      );
      expect(ledger[0].count).toBe("1");
      const outbox = await query<DatabaseRow>(
        "SELECT COUNT(*)::text AS count FROM outbox_messages WHERE payload->'data'->>'transactionId' = ?",
        [transactions[0].id],
      );
      expect(outbox[0].count).toBe("2");
      await assertLedgerMatchesWallet(wallet.id, "75.00", restarted.url);
    } finally {
      if (first) await stopInstance(first);
      if (restarted) await stopInstance(restarted);
      await deleteQueue(queueUrl);
    }
  }, 60_000);

  test("pending-reference attempts and Outbox state survive a worker restart", async () => {
    const suffix = crypto.randomUUID();
    const queueName = `integration-reference-restart-${suffix}.fifo`;
    const queueUrl = await createQueue(queueName);
    const consumerName = `integration-reference-restart-consumer-${suffix}`;
    let first: { process: ReturnType<typeof Bun.spawn>; url: string } | undefined;
    let restarted: { process: ReturnType<typeof Bun.spawn>; url: string } | undefined;
    try {
      first = await startInstance(queueName, { consumerName });
      const wallet = await createWallet(first.url);
      const betKey = `integration-reference-bet-key-${suffix}`;
      const referenceExternalId = `external-${betKey}`;
      const refundKey = `integration-reference-refund-${suffix}`;
      const refund = await fetch(`${first.url}/wagering/transactions`, {
        method: "POST",
        headers: { "content-type": "application/json", "idempotency-key": refundKey },
        body: JSON.stringify({
          providerId: "integration-concurrency-provider",
          externalTransactionId: `integration-refund-${suffix}`,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: "integration-concurrency-round",
          gameId: "integration-concurrency-game",
          kind: "REFUND",
          referenceExternalTransactionId: referenceExternalId,
          money: { amount: "10.00", currency: "BRL" },
        }),
      });
      expect(refund.ok).toBe(true);
      expect(refund.status).toBe(202);
      const refundResult = await refund.json() as WagerResponse;
      expect(refundResult.status).toBe("PENDING_REFERENCE");

      await waitUntil(async () => {
        const rows = await query<{ reference_attempts: number }>(
          "SELECT reference_attempts FROM wager_transactions WHERE idempotency_key = ?",
          [refundKey],
        );
        return rows.length === 1 && rows[0].reference_attempts > 0;
      }, 15_000, "INTEGRATION_REFERENCE_RETRY_NOT_RECORDED");

      const beforeRestart = await query<{
        id: string;
        reference_attempts: number;
        reference_next_attempt_at: Date | null;
      }>(
        `SELECT id, reference_attempts, reference_next_attempt_at
         FROM wager_transactions WHERE idempotency_key = ?`,
        [refundKey],
      );
      expect(beforeRestart[0].reference_next_attempt_at).toBeTruthy();
      await waitUntil(async () => {
        const rows = await query<DatabaseRow>(
          `SELECT COUNT(*)::text AS count FROM outbox_messages
           WHERE payload->'data'->>'transactionId' = ? AND published_at IS NULL`,
          [refundResult.transactionId],
        );
        return rows[0].count === "0";
      }, 15_000, "INTEGRATION_PENDING_REFERENCE_OUTBOX_NOT_PUBLISHED");

      await stopInstance(first);
      first = undefined;
      restarted = await startInstance(queueName, { consumerName });

      const afterRestart = await query<{
        status: string;
        reference_attempts: number;
        reference_next_attempt_at: Date | null;
      }>(
        `SELECT status, reference_attempts, reference_next_attempt_at
         FROM wager_transactions WHERE idempotency_key = ?`,
        [refundKey],
      );
      expect(afterRestart[0].status).toBe("PENDING_REFERENCE");
      expect(afterRestart[0].reference_attempts).toBeGreaterThanOrEqual(beforeRestart[0].reference_attempts);
      expect(afterRestart[0].reference_next_attempt_at).toBeTruthy();

      const betResult = await submitBet(restarted.url, wallet, "10.00", betKey);
      expect(betResult.status).toBe("PROCESSED");
      await waitUntil(async () => {
        const rows = await query<{ status: string }>(
          "SELECT status FROM wager_transactions WHERE idempotency_key = ?",
          [refundKey],
        );
        return rows[0]?.status === "PROCESSED";
      }, 20_000, "INTEGRATION_PENDING_REFERENCE_NOT_RECOVERED");

      await waitUntil(async () => {
        const rows = await query<DatabaseRow>(
          `SELECT COUNT(*) FILTER (WHERE published_at IS NULL)::text AS count
           FROM outbox_messages
           WHERE payload->'data'->>'transactionId' IN (?, ?)`,
          [refundResult.transactionId, betResult.transactionId],
        );
        return rows[0].count === "0";
      }, 20_000, "INTEGRATION_REFERENCE_OUTBOX_NOT_PUBLISHED");

      const outbox = await query<{
        count: string;
        unpublished: string;
      }>(
        `SELECT COUNT(*)::text AS count,
                COUNT(*) FILTER (WHERE published_at IS NULL)::text AS unpublished
         FROM outbox_messages
         WHERE payload->'data'->>'transactionId' IN (?, ?)`,
        [refundResult.transactionId, betResult.transactionId],
      );
      expect(outbox[0].count).toBe("5");
      expect(outbox[0].unpublished).toBe("0");

      const walletAfter = await query<{ balance: string }>(
        "SELECT balance::text AS balance FROM wallets WHERE id = ?",
        [wallet.id],
      );
      await assertLedgerMatchesWallet(wallet.id, walletAfter[0].balance, restarted.url);
      expect(walletAfter[0].balance).toBe("100.00");
    } finally {
      if (first) await stopInstance(first);
      if (restarted) await stopInstance(restarted);
      await deleteQueue(queueUrl);
    }
  }, 60_000);
});
