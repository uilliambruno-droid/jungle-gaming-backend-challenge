import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { LedgerDirection } from "../../domain/ledger/wallet-ledger-entry.js";
import {
  EventContext,
  IntegrationEvent,
  WalletBalanceChanged,
  WagerTransactionProcessed,
  WagerTransactionRejected,
} from "../../domain/integration/integration-event.js";
import { Money } from "../../domain/money/money.js";
import {
  FailureCode,
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../domain/wager/wager-transaction.js";
import { Wallet } from "../../domain/wallet/wallet.js";
import { MetricsService } from "../observability/metrics.service.js";

interface PendingRow {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  idempotency_key: string;
  payload_hash: string;
  reference_external_transaction_id: string;
  wallet_id: string;
  player_id: string;
  round_id: string;
  game_id: string;
  amount: string;
  currency: string;
  kind: WagerTransactionKind;
  reference_attempts: number;
  correlation_id: string;
  causation_id: string | null;
  created_at: Date;
  processed_at: Date | null;
  reference_transaction_id: string | null;
  failure_code: FailureCode | null;
}

interface ReferenceRow {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  idempotency_key: string;
  payload_hash: string;
  wallet_id: string;
  player_id: string;
  round_id: string;
  game_id: string;
  kind: WagerTransactionKind;
  amount: string;
  currency: string;
  reference_external_transaction_id: string | null;
  reference_transaction_id: string | null;
  status: WagerTransactionStatus;
  failure_code: FailureCode | null;
  created_at: Date;
  processed_at: Date | null;
}

interface WalletRow {
  id: string;
  player_id: string;
  balance: string;
  currency: string;
  version: number;
  created_at: Date;
  updated_at: Date;
}

function normalizeAmount(value: string): string {
  const amount = value.trim();
  return amount.includes(".") ? amount : `${amount}.00`;
}

function wait(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

@Injectable()
export class PendingReferenceWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PendingReferenceWorker.name);
  private running = true;
  private loopPromise?: Promise<void>;

  constructor(
    private readonly em: EntityManager,
    private readonly metrics: MetricsService,
  ) {}

  onModuleInit(): void {
    this.loopPromise = this.loop();
  }

  async onModuleDestroy(): Promise<void> {
    this.running = false;
    await this.loopPromise;
  }

  private async loop(): Promise<void> {
    while (this.running) {
      try {
        const processed = await this.processOne();
        if (!processed && this.running) {
          await wait(Number(process.env.PENDING_REFERENCE_POLL_INTERVAL_MS ?? "1000"));
        }
      } catch (error) {
        const code = error && typeof error === "object" && "code" in error
          ? String((error as { code: unknown }).code)
          : "WORKER_ERROR";
        if (["40P01", "55P03", "40001"].includes(code)) this.metrics.increment("lock_conflicts_total");
        this.logger.error(JSON.stringify({
          event: "pending_reference_worker_failed",
          correlationId: "pending-reference-worker",
          messageId: null,
          transactionId: null,
          walletId: null,
          providerId: null,
          category: "transient",
          errorCode: code,
        }));
        if (this.running) await wait(Number(process.env.PENDING_REFERENCE_ERROR_DELAY_MS ?? "1000"));
      }
    }
  }

  private async processOne(): Promise<boolean> {
    const startedAt = performance.now();
    const didProcess = await this.em.transactional(async tm => {
      const candidates = await tm.execute<Array<{ id: string; wallet_id: string }>>(
        `SELECT id,wallet_id
         FROM wager_transactions
         WHERE status='PENDING_REFERENCE'
           AND (reference_next_attempt_at IS NULL OR reference_next_attempt_at<=now())
         ORDER BY created_at,id
         LIMIT 1`,
      );
      if (!candidates.length) return false;

      const walletRows = await tm.execute<WalletRow[]>(
        "SELECT id,player_id,balance,currency,version,created_at,updated_at FROM wallets WHERE id=? FOR UPDATE",
        [candidates[0].wallet_id],
      );
      if (!walletRows.length) throw new Error("PENDING_REFERENCE_WALLET_MISSING");
      const walletRow = walletRows[0];
      const wallet = Wallet.rehydrate({
        id: walletRow.id,
        playerId: walletRow.player_id,
        balance: Money.from({
          amount: normalizeAmount(walletRow.balance),
          currency: walletRow.currency.trim(),
        }),
        version: walletRow.version,
        createdAt: walletRow.created_at,
        updatedAt: walletRow.updated_at,
      });

      const rows = await tm.execute<PendingRow[]>(
        `SELECT id,provider_id,external_transaction_id,idempotency_key,payload_hash,
                reference_external_transaction_id,wallet_id,player_id,round_id,game_id,amount,currency,kind,
                reference_attempts,correlation_id,causation_id,created_at,processed_at,reference_transaction_id,failure_code
         FROM wager_transactions
         WHERE id=?
           AND status='PENDING_REFERENCE'
           AND (reference_next_attempt_at IS NULL OR reference_next_attempt_at<=now())
         FOR UPDATE`,
        [candidates[0].id],
      );
      if (!rows.length) return true;
      const row = rows[0];
      const context: EventContext = {
        correlationId: row.correlation_id,
        ...(row.causation_id ? { causationId: row.causation_id } : {}),
      };
      const amount = Money.from({ amount: normalizeAmount(row.amount), currency: row.currency.trim() });
      const transaction = WagerTransaction.rehydrate({
        id: row.id,
        providerId: row.provider_id,
        externalTransactionId: row.external_transaction_id,
        idempotencyKey: row.idempotency_key,
        payloadHash: row.payload_hash,
        walletId: row.wallet_id,
        playerId: row.player_id,
        roundId: row.round_id,
        gameId: row.game_id,
        kind: row.kind,
        money: amount,
        referenceExternalTransactionId: row.reference_external_transaction_id,
        status: WagerTransactionStatus.PendingReference,
        referenceTransactionId: row.reference_transaction_id ?? undefined,
        failureCode: row.failure_code ?? undefined,
        createdAt: row.created_at,
        processedAt: row.processed_at ?? undefined,
      });
      const attempts = row.reference_attempts + 1;
      const maxAttempts = Number(process.env.PENDING_REFERENCE_MAX_ATTEMPTS ?? "8");
      const enqueue = async (event: IntegrationEvent<unknown>): Promise<void> => {
        await tm.execute(
          "INSERT INTO outbox_messages(id,aggregate_id,event_type,payload,occurred_at) VALUES(?,?,?,?,?)",
          [event.eventId, event.aggregateId, event.eventType, JSON.stringify(event.toJSON()), event.occurredAt],
        );
      };
      const updateTerminal = async (code: FailureCode): Promise<void> => {
        transaction.reject(code);
        await tm.execute(
          `UPDATE wager_transactions
           SET status=?,failure_code=?,reference_attempts=?,reference_next_attempt_at=NULL,processed_at=?
           WHERE id=?`,
          [transaction.status, code, attempts, new Date(), transaction.id],
        );
        await enqueue(WagerTransactionRejected.create(row.wallet_id, context, {
          transactionId: row.id,
          walletId: row.wallet_id,
          providerId: row.provider_id,
          status: WagerTransactionStatus.Rejected,
          failureCode: code,
        }));
        this.metrics.increment("transactions_rejected_total");
        if (code === "REFERENCE_NOT_FOUND") this.metrics.increment("pending_reference_expired_total");
        this.logger.warn(JSON.stringify({
          event: "pending_reference_rejected",
          correlationId: context.correlationId,
          messageId: null,
          transactionId: row.id,
          walletId: row.wallet_id,
          providerId: row.provider_id,
          failureCode: code,
        }));
      };

      const references = await tm.execute<ReferenceRow[]>(
        `SELECT id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,
                game_id,kind,amount,currency,reference_external_transaction_id,reference_transaction_id,status,
                failure_code,created_at,processed_at
         FROM wager_transactions
         WHERE provider_id=? AND external_transaction_id=?
         FOR UPDATE`,
        [row.provider_id, row.reference_external_transaction_id],
      );
      if (!references.length) {
        if (attempts >= maxAttempts) {
          await updateTerminal("REFERENCE_NOT_FOUND");
        } else {
          await tm.execute(
            `UPDATE wager_transactions
             SET reference_attempts=?,
                 reference_next_attempt_at=now()+(POWER(2,LEAST(?,8))*interval '1 second')
             WHERE id=?`,
            [attempts, attempts, row.id],
          );
          this.metrics.increment("pending_reference_retries_total");
          this.logger.log(JSON.stringify({
            event: "pending_reference_retry",
            correlationId: context.correlationId,
            messageId: null,
            transactionId: row.id,
            walletId: row.wallet_id,
            providerId: row.provider_id,
            attempts,
          }));
        }
        return true;
      }

      const referenceRow = references[0];
      const reference = WagerTransaction.rehydrate({
        id: referenceRow.id,
        providerId: referenceRow.provider_id,
        externalTransactionId: referenceRow.external_transaction_id,
        idempotencyKey: referenceRow.idempotency_key,
        payloadHash: referenceRow.payload_hash,
        walletId: referenceRow.wallet_id,
        playerId: referenceRow.player_id,
        roundId: referenceRow.round_id,
        gameId: referenceRow.game_id,
        kind: referenceRow.kind,
        money: Money.from({
          amount: normalizeAmount(referenceRow.amount),
          currency: referenceRow.currency.trim(),
        }),
        referenceExternalTransactionId: referenceRow.reference_external_transaction_id ?? undefined,
        status: referenceRow.status,
        referenceTransactionId: referenceRow.reference_transaction_id ?? undefined,
        failureCode: referenceRow.failure_code ?? undefined,
        createdAt: referenceRow.created_at,
        processedAt: referenceRow.processed_at ?? undefined,
      });
      const sameContext = reference.providerId === row.provider_id
        && reference.playerId === row.player_id
        && reference.walletId === row.wallet_id
        && reference.roundId === row.round_id
        && reference.money.currency === amount.currency
        && reference.status === WagerTransactionStatus.Processed;
      const valid = sameContext && (
        transaction.kind === WagerTransactionKind.Win
          ? reference.kind === WagerTransactionKind.Bet
          : reference.money.equals(amount) && (transaction.kind === WagerTransactionKind.Refund
            ? reference.kind === WagerTransactionKind.Bet
            : [WagerTransactionKind.Bet, WagerTransactionKind.Win, WagerTransactionKind.Refund].includes(reference.kind))
      );
      if (!valid) {
        await updateTerminal("INVALID_REFERENCE");
        return true;
      }

      if (transaction.requiresReference()) {
        const priorReversals = await tm.execute<Array<{ id: string }>>(
          `SELECT id FROM wager_transactions
           WHERE reference_transaction_id=? AND kind=? AND status='PROCESSED'`,
          [reference.id, transaction.kind],
        );
        if (priorReversals.length) {
          await updateTerminal("DUPLICATE_REVERSAL");
          return true;
        }
      }

      const direction = transaction.ledgerDirectionFor(reference);
      if (direction === LedgerDirection.Debit && wallet.balance.isLessThan(amount)) {
        await updateTerminal("REVERSAL_WOULD_OVERDRAW");
        return true;
      }

      const now = new Date();
      const entry = direction === LedgerDirection.Debit
        ? wallet.debit(amount, { id: crypto.randomUUID(), transactionId: row.id, at: now })
        : wallet.credit(amount, { id: crypto.randomUUID(), transactionId: row.id, at: now });
      transaction.markProcessed(reference.id, now);
      await tm.execute(
        "UPDATE wallets SET balance=?,version=?,updated_at=? WHERE id=?",
        [wallet.balance.toJSON().amount, wallet.version, wallet.updatedAt, wallet.id],
      );
      await tm.execute(
        `INSERT INTO wallet_ledger_entries(
           id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after,created_at
         ) VALUES(?,?,?,?,?,?,?,?,?)`,
        [
          entry.id, entry.walletId, entry.transactionId, entry.direction, entry.money.toJSON().amount,
          entry.money.currency, entry.balanceBefore.toJSON().amount, entry.balanceAfter.toJSON().amount, entry.createdAt,
        ],
      );
      await tm.execute(
        `UPDATE wager_transactions
         SET status=?,reference_transaction_id=?,reference_attempts=?,reference_next_attempt_at=NULL,processed_at=?
         WHERE id=?`,
        [transaction.status, reference.id, attempts, now, row.id],
      );
      await enqueue(WagerTransactionProcessed.create(row.wallet_id, context, {
        transactionId: row.id,
        walletId: row.wallet_id,
        providerId: row.provider_id,
        status: WagerTransactionStatus.Processed,
      }, now));
      await enqueue(WalletBalanceChanged.from(wallet, entry, context, row.provider_id));
      this.metrics.increment("transactions_processed_total");
      this.metrics.increment("pending_reference_processed_total");
      this.logger.log(JSON.stringify({
        event: "pending_reference_processed",
        correlationId: context.correlationId,
        messageId: null,
        transactionId: row.id,
        walletId: row.wallet_id,
        providerId: row.provider_id,
        referenceTransactionId: reference.id,
      }));
      return true;
    });
    if (didProcess) {
      this.metrics.observe("pending_reference_processing_latency_ms", performance.now() - startedAt);
    }
    return didProcess;
  }
}
