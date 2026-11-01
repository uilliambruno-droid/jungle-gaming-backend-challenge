import {
  BadRequestException,
  ConflictException,
  HttpException,
  Injectable,
  Logger,
  NotFoundException,
  ServiceUnavailableException,
} from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { randomUUID } from "node:crypto";
import { LedgerDirection, WalletLedgerEntry } from "../domain/ledger/wallet-ledger-entry.js";
import {
  EventContext,
  IntegrationEvent,
  WalletBalanceChanged,
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
} from "../domain/integration/integration-event.js";
import { Money, MoneyProps } from "../domain/money/money.js";
import {
  FailureCode,
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../domain/wager/wager-transaction.js";
import { Wallet } from "../domain/wallet/wallet.js";
import { payloadHash } from "./canonical-json.js";
import { MetricsService } from "../infrastructure/observability/metrics.service.js";

export interface WagerInput {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
  idempotencyKey: string;
}

export interface WagerResult {
  transactionId: string;
  status: WagerTransactionStatus;
  balance: MoneyProps;
  idempotentReplay: boolean;
  failureCode?: string;
}

interface SubmissionContext extends EventContext {
  readonly messageId?: string;
  readonly consumerName?: string;
}

interface TransactionResponseRow {
  id: string;
  status: WagerTransactionStatus;
  payload_hash: string;
  response_status: WagerTransactionStatus;
  response_balance: string;
  response_currency: string;
  response_failure_code: string | null;
}

interface WalletRow {
  id: string;
  player_id: string;
  currency: string;
  balance: string;
  version: number;
  created_at: Date;
  updated_at: Date;
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

interface PostgresErrorDetails {
  readonly code?: string;
  readonly constraint?: string;
}

function postgresErrorDetails(error: unknown): PostgresErrorDetails {
  const pending: unknown[] = [error];
  const visited = new Set<object>();
  while (pending.length) {
    const current = pending.pop();
    if (!current || typeof current !== "object" || visited.has(current)) continue;
    visited.add(current);
    const value = current as Record<string, unknown>;
    if (typeof value.code === "string" || typeof value.constraint === "string") {
      return {
        code: typeof value.code === "string" ? value.code : undefined,
        constraint: typeof value.constraint === "string" ? value.constraint : undefined,
      };
    }
    pending.push(value.cause, value.driverException, value.originalError);
  }
  return {};
}

function normalizeAmount(value: string): string {
  const amount = String(value).trim();
  return amount.includes(".") ? amount : `${amount}.00`;
}

function moneyProps(amount: string, currency: string): MoneyProps {
  return Money.from({ amount: normalizeAmount(amount), currency: currency.trim() }).toJSON();
}

function encodeCursor(value: { createdAt: string; id: string }): string {
  return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

function decodeCursor(value: string): { createdAt: Date; id: string } {
  try {
    const parsed = JSON.parse(Buffer.from(value, "base64url").toString("utf8")) as {
      createdAt?: string;
      id?: string;
    };
    const createdAt = new Date(parsed.createdAt ?? "");
    if (!parsed.id || Number.isNaN(createdAt.getTime())) throw new Error("INVALID_CURSOR");
    return { createdAt, id: parsed.id };
  } catch {
    throw new BadRequestException("INVALID_LEDGER_CURSOR");
  }
}

function isDomainInputError(error: unknown): boolean {
  return error instanceof Error && [
    "INVALID_AMOUNT",
    "INVALID_CURRENCY",
    "CURRENCY_MISMATCH",
    "INVALID_PAYLOAD",
    "INVALID_REFERENCE",
    "INITIAL_BALANCE_CANNOT_BE_NEGATIVE",
  ].includes(error.message);
}

function exceptionCode(error: unknown): string {
  if (error instanceof HttpException) {
    const response = error.getResponse();
    if (typeof response === "string") return response;
    if (response && typeof response === "object" && "message" in response) {
      const message = (response as { message: unknown }).message;
      if (typeof message === "string") return message;
    }
  }
  return error instanceof Error ? error.name : "UNKNOWN_ERROR";
}

@Injectable()
export class WagerService {
  private readonly logger = new Logger(WagerService.name);

  constructor(
    private readonly em: EntityManager,
    private readonly metrics: MetricsService,
  ) {}

  async createWallet(input: { playerId: string; initialBalance: MoneyProps }) {
    let initialBalance: Money;
    try {
      initialBalance = Money.from(input.initialBalance);
    } catch (error) {
      if (isDomainInputError(error)) throw new BadRequestException((error as Error).message);
      throw error;
    }
    const now = new Date();
    const wallet = Wallet.open({ id: randomUUID(), playerId: input.playerId, initialBalance, now });

    try {
      await this.em.transactional(async tm => {
        await tm.execute(
          "INSERT INTO wallets(id,player_id,currency,balance,version,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
          [wallet.id, wallet.playerId, wallet.currency, wallet.balance.toJSON().amount, wallet.version, now, now],
        );
        if (!initialBalance.isPositive()) return;

        const transactionId = randomUUID();
        const opening = WagerTransaction.rehydrate({
          id: transactionId,
          providerId: "__internal__",
          externalTransactionId: `OPENING:${wallet.id}`,
          idempotencyKey: `OPENING:${wallet.id}`,
          payloadHash: payloadHash({ walletId: wallet.id, opening: initialBalance.toJSON() }),
          walletId: wallet.id,
          playerId: wallet.playerId,
          roundId: "__opening__",
          gameId: "__internal__",
          kind: WagerTransactionKind.Opening,
          money: initialBalance,
          status: WagerTransactionStatus.Processed,
          createdAt: now,
          processedAt: now,
        });
        const entry = WalletLedgerEntry.create({
          id: randomUUID(),
          walletId: wallet.id,
          transactionId: opening.id,
          direction: LedgerDirection.Credit,
          money: initialBalance,
          balanceBefore: Money.zero(wallet.currency),
          createdAt: now,
        });
        await tm.execute(
          `INSERT INTO wager_transactions(
             id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,
             kind,amount,currency,status,created_at,processed_at,response_status,response_balance,response_currency,correlation_id
           ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
          [
            opening.id, opening.providerId, opening.externalTransactionId, opening.idempotencyKey, opening.payloadHash,
            opening.walletId, opening.playerId, opening.roundId, opening.gameId, opening.kind, initialBalance.toJSON().amount,
            wallet.currency, opening.status, now, now, opening.status, initialBalance.toJSON().amount, wallet.currency,
            opening.id,
          ],
        );
        await tm.execute(
          `INSERT INTO wallet_ledger_entries(
             id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after,created_at
           ) VALUES(?,?,?,?,?,?,?,?,?)`,
          [
            entry.id, entry.walletId, entry.transactionId, entry.direction, entry.money.toJSON().amount, wallet.currency,
            entry.balanceBefore.toJSON().amount, entry.balanceAfter.toJSON().amount, entry.createdAt,
          ],
        );
        const openingContext = { correlationId: opening.id };
        const events: IntegrationEvent<unknown>[] = [
          WagerTransactionProcessed.create(wallet.id, openingContext, {
            transactionId: opening.id,
            walletId: wallet.id,
            providerId: opening.providerId,
            status: WagerTransactionStatus.Processed,
          }, now),
          WalletBalanceChanged.from(wallet, entry, openingContext, opening.providerId),
        ];
        for (const event of events) {
          await tm.execute(
            "INSERT INTO outbox_messages(id,aggregate_id,event_type,payload,occurred_at) VALUES(?,?,?,?,?)",
            [event.eventId, event.aggregateId, event.eventType, JSON.stringify(event.toJSON()), event.occurredAt],
          );
        }
        this.metrics.increment("transactions_processed_total");
      });
    } catch (error) {
      const details = postgresErrorDetails(error);
      if (details.code === "23505" && details.constraint === "wallets_player_currency_unique") {
        throw new ConflictException("WALLET_ALREADY_EXISTS");
      }
      if (isDomainInputError(error)) throw new BadRequestException((error as Error).message);
      this.logger.error(JSON.stringify({
        event: "wallet_creation_failed",
        providerId: null,
        transactionId: null,
        walletId: wallet.id,
        correlationId: wallet.id,
        errorCode: details.code ?? "DATABASE_ERROR",
      }));
      throw new ServiceUnavailableException("WALLET_CREATION_UNAVAILABLE");
    }

    return {
      id: wallet.id,
      playerId: wallet.playerId,
      balance: wallet.balance.toJSON(),
      version: wallet.version,
    };
  }

  async submit(
    input: WagerInput,
    inbox?: { messageId: string; consumerName: string },
    context: SubmissionContext = { correlationId: input.idempotencyKey },
  ): Promise<WagerResult> {
    const startedAt = performance.now();
    let transactionId: string | null = null;
    try {
      const money = Money.from(input.money);
      if (!input.idempotencyKey.trim()) throw new BadRequestException("IDEMPOTENCY_KEY_REQUIRED");
      if (input.kind === WagerTransactionKind.Opening) throw new BadRequestException("INVALID_TRANSACTION_KIND");
      const businessPayload = {
        providerId: input.providerId,
        externalTransactionId: input.externalTransactionId,
        playerId: input.playerId,
        walletId: input.walletId,
        roundId: input.roundId,
        gameId: input.gameId,
        kind: input.kind,
        money: money.toJSON(),
        referenceExternalTransactionId: input.referenceExternalTransactionId,
      };
      const hash = payloadHash(businessPayload);
      const result = await this.em.transactional(async tm => {
        const complete = async (value: WagerResult): Promise<WagerResult> => {
          if (inbox) {
            await tm.execute(
              "UPDATE inbox_messages SET processed_at=?,transaction_id=? WHERE message_id=? AND consumer_name=?",
              [new Date(), value.transactionId, inbox.messageId, inbox.consumerName],
            );
          }
          return value;
        };
        const enqueue = async (event: IntegrationEvent<unknown>): Promise<void> => {
          await tm.execute(
            "INSERT INTO outbox_messages(id,aggregate_id,event_type,payload,occurred_at) VALUES(?,?,?,?,?)",
            [event.eventId, event.aggregateId, event.eventType, JSON.stringify(event.toJSON()), event.occurredAt],
          );
        };

        if (inbox) {
          await tm.execute(
            `INSERT INTO inbox_messages(message_id,consumer_name,payload_hash,received_at)
             VALUES(?,?,?,?) ON CONFLICT(message_id,consumer_name) DO NOTHING`,
            [inbox.messageId, inbox.consumerName, hash, new Date()],
          );
          const seen = await tm.execute<Array<{
            payload_hash: string;
            processed_at: Date | null;
            transaction_id: string | null;
          }>>(
            "SELECT payload_hash,processed_at,transaction_id FROM inbox_messages WHERE message_id=? AND consumer_name=? FOR UPDATE",
            [inbox.messageId, inbox.consumerName],
          );
          if (seen[0]?.payload_hash !== hash) {
            this.metrics.increment("inbox_payload_conflicts_total");
            throw new ConflictException("INBOX_PAYLOAD_CONFLICT");
          }
          if (seen[0]?.processed_at) {
            this.metrics.increment("inbox_duplicates_total");
            if (seen[0].transaction_id) {
              const prior = await tm.execute<TransactionResponseRow[]>(
                `SELECT id,status,payload_hash,response_status,response_balance,response_currency,response_failure_code
                 FROM wager_transactions WHERE id=?`,
                [seen[0].transaction_id],
              );
              if (!prior.length) throw new ConflictException("INBOX_TRANSACTION_NOT_FOUND");
              this.metrics.increment("idempotent_replays_total");
              return complete(this.result(prior[0], true));
            }
          }
        }

        const existing = await tm.execute<TransactionResponseRow[]>(
          `SELECT id,status,payload_hash,response_status,response_balance,response_currency,response_failure_code
           FROM wager_transactions WHERE idempotency_key=? FOR UPDATE`,
          [input.idempotencyKey],
        );
        if (existing.length) {
          const row = existing[0];
          if (row.payload_hash !== hash) throw new ConflictException("IDEMPOTENCY_KEY_PAYLOAD_CONFLICT");
          this.metrics.increment("idempotent_replays_total");
          return complete(this.result(row, true));
        }

        const wallets = await tm.execute<WalletRow[]>(
          "SELECT * FROM wallets WHERE id=? FOR UPDATE",
          [input.walletId],
        );
        if (!wallets.length) throw new NotFoundException("WALLET_NOT_FOUND");
        const walletRow = wallets[0];
        const currentBalance = Money.from({
          amount: normalizeAmount(walletRow.balance),
          currency: walletRow.currency.trim(),
        });
        const wallet = Wallet.rehydrate({
          id: walletRow.id,
          playerId: walletRow.player_id,
          balance: currentBalance,
          version: walletRow.version,
          createdAt: walletRow.created_at,
          updatedAt: walletRow.updated_at,
        });

        const id = randomUUID();
        transactionId = id;
        const now = new Date();
        const transaction = WagerTransaction.create({
          id,
          providerId: input.providerId,
          externalTransactionId: input.externalTransactionId,
          idempotencyKey: input.idempotencyKey,
          payloadHash: hash,
          walletId: input.walletId,
          playerId: input.playerId,
          roundId: input.roundId,
          gameId: input.gameId,
          kind: input.kind,
          money,
          referenceExternalTransactionId: input.referenceExternalTransactionId,
          now,
        });
        await tm.execute(
          `INSERT INTO wager_transactions(
             id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,
             kind,amount,currency,status,reference_external_transaction_id,created_at,response_status,response_balance,
             response_currency,correlation_id,causation_id
           ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
          [
            id, input.providerId, input.externalTransactionId, input.idempotencyKey, hash, input.walletId, input.playerId,
            input.roundId, input.gameId, input.kind, money.toJSON().amount, money.currency, transaction.status,
            input.referenceExternalTransactionId ?? null, now, transaction.status, currentBalance.toJSON().amount,
            currentBalance.currency, context.correlationId, context.causationId ?? null,
          ],
        );

        const finish = async (status: WagerTransactionStatus, failureCode?: FailureCode): Promise<WagerResult> => {
          const responseBalance = wallet.balance.toJSON();
          await tm.execute(
            `UPDATE wager_transactions
             SET status=?,failure_code=?,response_status=?,response_balance=?,response_currency=?,response_failure_code=?
             WHERE id=?`,
            [
              transaction.status,
              failureCode ?? null,
              status,
              responseBalance.amount,
              responseBalance.currency,
              failureCode ?? null,
              id,
            ],
          );
          if (status === WagerTransactionStatus.Processed) {
            await enqueue(WagerTransactionProcessed.create(input.walletId, context, {
              transactionId: id,
              walletId: input.walletId,
              providerId: input.providerId,
              status,
            }, now));
            this.metrics.increment("transactions_processed_total");
          } else if (status === WagerTransactionStatus.PendingReference) {
            await enqueue(WagerTransactionPendingReference.create(input.walletId, context, {
              transactionId: id,
              walletId: input.walletId,
              providerId: input.providerId,
              status,
              referenceExternalTransactionId: input.referenceExternalTransactionId!,
            }, now));
            this.metrics.increment("transactions_pending_reference_total");
          } else {
            await enqueue(WagerTransactionRejected.create(input.walletId, context, {
              transactionId: id,
              walletId: input.walletId,
              providerId: input.providerId,
              status,
              failureCode: failureCode ?? "INVALID_PAYLOAD",
            }, now));
            this.metrics.increment("transactions_rejected_total");
          }
          const result: WagerResult = {
            transactionId: id,
            status,
            balance: responseBalance,
            idempotentReplay: false,
            ...(failureCode ? { failureCode } : {}),
          };
          this.logger.log(JSON.stringify({
            event: "wager_transaction_completed",
            correlationId: context.correlationId,
            messageId: context.messageId ?? null,
            transactionId: id,
            walletId: input.walletId,
            providerId: input.providerId,
            status,
            failureCode: failureCode ?? null,
          }));
          return complete(result);
        };

        if (wallet.playerId !== input.playerId || wallet.currency !== money.currency) {
          transaction.reject("WALLET_MISMATCH");
          return finish(WagerTransactionStatus.Rejected, "WALLET_MISMATCH");
        }

        let reference: WagerTransaction | undefined;
        const hasOptionalWinReference = transaction.kind === WagerTransactionKind.Win
          && Boolean(transaction.referenceExternalTransactionId);
        if (transaction.requiresReference() || hasOptionalWinReference) {
          const references = await tm.execute<ReferenceRow[]>(
            `SELECT id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,
                    game_id,kind,amount,currency,reference_external_transaction_id,reference_transaction_id,status,
                    failure_code,created_at,processed_at
             FROM wager_transactions
             WHERE provider_id=? AND external_transaction_id=? FOR UPDATE`,
            [input.providerId, input.referenceExternalTransactionId],
          );
          if (!references.length) {
            transaction.markPendingReference();
            return finish(WagerTransactionStatus.PendingReference);
          }

          const row = references[0];
          reference = WagerTransaction.rehydrate({
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
            money: Money.from({ amount: normalizeAmount(row.amount), currency: row.currency.trim() }),
            referenceExternalTransactionId: row.reference_external_transaction_id ?? undefined,
            status: row.status,
            referenceTransactionId: row.reference_transaction_id ?? undefined,
            failureCode: row.failure_code ?? undefined,
            createdAt: row.created_at,
            processedAt: row.processed_at ?? undefined,
          });
          const sameContext = row.player_id === input.playerId
            && row.wallet_id === input.walletId
            && row.round_id === input.roundId
            && row.currency.trim() === money.currency
            && reference.status === WagerTransactionStatus.Processed;
          const valid = sameContext && (
            input.kind === WagerTransactionKind.Win
              ? reference.kind === WagerTransactionKind.Bet
              : reference.money.equals(money) && (input.kind === WagerTransactionKind.Refund
                ? reference.kind === WagerTransactionKind.Bet
                : [WagerTransactionKind.Bet, WagerTransactionKind.Win, WagerTransactionKind.Refund].includes(reference.kind))
          );
          if (!valid) {
            transaction.reject("INVALID_REFERENCE");
            return finish(WagerTransactionStatus.Rejected, "INVALID_REFERENCE");
          }

          if (transaction.requiresReference()) {
            const reversals = await tm.execute<Array<{ id: string }>>(
              `SELECT id FROM wager_transactions
               WHERE reference_transaction_id=? AND kind=? AND status='PROCESSED'`,
              [reference.id, transaction.kind],
            );
            if (reversals.length) {
              transaction.reject("DUPLICATE_REVERSAL");
              return finish(WagerTransactionStatus.Rejected, "DUPLICATE_REVERSAL");
            }
          }
        }

        if (transaction.kind === WagerTransactionKind.Bet && wallet.balance.isLessThan(money)) {
          transaction.reject("INSUFFICIENT_FUNDS");
          return finish(WagerTransactionStatus.Rejected, "INSUFFICIENT_FUNDS");
        }

        let entry: WalletLedgerEntry | undefined;
        if (transaction.affectsBalance()) {
          const direction = transaction.ledgerDirectionFor(reference);
          if (direction === LedgerDirection.Debit && wallet.balance.isLessThan(money)) {
            transaction.reject(transaction.requiresReference() ? "REVERSAL_WOULD_OVERDRAW" : "INSUFFICIENT_FUNDS");
            return finish(
              WagerTransactionStatus.Rejected,
              transaction.requiresReference() ? "REVERSAL_WOULD_OVERDRAW" : "INSUFFICIENT_FUNDS",
            );
          }
          entry = direction === LedgerDirection.Debit
            ? wallet.debit(money, { id: randomUUID(), transactionId: id, at: now })
            : wallet.credit(money, { id: randomUUID(), transactionId: id, at: now });
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
        }

        transaction.markProcessed(reference?.id, now);
        await tm.execute(
          "UPDATE wager_transactions SET status=?,reference_transaction_id=?,processed_at=? WHERE id=?",
          [transaction.status, reference?.id ?? null, transaction.processedAt, transaction.id],
        );
        const response = await finish(WagerTransactionStatus.Processed);
        if (entry) {
          await enqueue(WalletBalanceChanged.from(wallet, entry, context, input.providerId));
        }
        return response;
      });
      return result;
    } catch (error) {
      if (error instanceof ConflictException || error instanceof BadRequestException || error instanceof NotFoundException) {
        this.logger.warn(JSON.stringify({
          event: error instanceof ConflictException ? "wager_transaction_conflict" : "wager_request_rejected",
          correlationId: context.correlationId,
          messageId: context.messageId ?? null,
          transactionId,
          walletId: input.walletId,
          providerId: input.providerId,
          errorCode: exceptionCode(error),
        }));
        throw error;
      }
      if (isDomainInputError(error)) throw new BadRequestException((error as Error).message);

      const details = postgresErrorDetails(error);
      if (details.code === "23505") {
        if (details.constraint === "wager_transactions_idempotency_key_key") {
          let existing: TransactionResponseRow | undefined;
          try {
            existing = await this.findByIdempotencyKey(input.idempotencyKey);
          } catch (replayError) {
            const replayErrorDetails = postgresErrorDetails(replayError);
            this.logger.error(JSON.stringify({
              event: "idempotency_replay_lookup_failed",
              correlationId: context.correlationId,
              messageId: context.messageId ?? null,
              transactionId,
              walletId: input.walletId,
              providerId: input.providerId,
              category: "transient",
              errorCode: replayErrorDetails.code ?? "DATABASE_ERROR",
            }));
            throw new ServiceUnavailableException("IDEMPOTENCY_REPLAY_UNAVAILABLE");
          }
          if (existing) {
            const hash = payloadHash({
              providerId: input.providerId,
              externalTransactionId: input.externalTransactionId,
              playerId: input.playerId,
              walletId: input.walletId,
              roundId: input.roundId,
              gameId: input.gameId,
              kind: input.kind,
              money: Money.from(input.money).toJSON(),
              referenceExternalTransactionId: input.referenceExternalTransactionId,
            });
            if (existing.payload_hash !== hash) throw new ConflictException("IDEMPOTENCY_KEY_PAYLOAD_CONFLICT");
            this.metrics.increment("idempotent_replays_total");
            return this.result(existing, true);
          }
          throw new ConflictException("IDEMPOTENCY_KEY_CONFLICT");
        }
        if (details.constraint === "wager_processed_reversal_unique") {
          throw new ConflictException("DUPLICATE_REVERSAL");
        }
        if (details.constraint === "wager_transactions_provider_id_external_transaction_id_key") {
          throw new ConflictException("EXTERNAL_TRANSACTION_ID_CONFLICT");
        }
        throw new ConflictException("PERSISTENCE_UNIQUE_CONFLICT");
      }

      if (["40P01", "55P03", "40001"].includes(details.code ?? "")) {
        this.metrics.increment("lock_conflicts_total");
      }
      this.logger.error(JSON.stringify({
        event: "wager_transaction_failed",
        correlationId: context.correlationId,
        messageId: context.messageId ?? null,
        transactionId,
        walletId: input.walletId,
        providerId: input.providerId,
        category: "transient",
        errorCode: details.code ?? "DATABASE_ERROR",
      }));
      throw new ServiceUnavailableException("WAGER_PROCESSING_UNAVAILABLE");
    } finally {
      this.metrics.observe("wager_submission_latency_ms", performance.now() - startedAt);
    }
  }

  private async findByIdempotencyKey(key: string): Promise<TransactionResponseRow | undefined> {
    const rows = await this.em.getConnection().execute<TransactionResponseRow[]>(
      `SELECT id,status,payload_hash,response_status,response_balance,response_currency,response_failure_code
       FROM wager_transactions WHERE idempotency_key=?`,
      [key],
    );
    return rows[0];
  }

  private result(row: TransactionResponseRow, replay: boolean): WagerResult {
    return {
      transactionId: row.id,
      status: row.response_status ?? row.status,
      balance: moneyProps(row.response_balance, row.response_currency),
      idempotentReplay: replay,
      ...(row.response_failure_code ? { failureCode: row.response_failure_code } : {}),
    };
  }

  async getWallet(id: string) {
    const rows = await this.em.getConnection().execute<Array<{
      id: string;
      player_id: string;
      balance: string;
      currency: string;
      version: number;
      created_at: Date;
      updated_at: Date;
    }>>(
      "SELECT id,player_id,balance,currency,version,created_at,updated_at FROM wallets WHERE id=?",
      [id],
    );
    if (!rows.length) throw new NotFoundException("WALLET_NOT_FOUND");
    return rows[0];
  }

  async getTransaction(id: string) {
    const rows = await this.em.getConnection().execute<Array<Record<string, unknown>>>(
      "SELECT * FROM wager_transactions WHERE id=?",
      [id],
    );
    if (!rows.length) throw new NotFoundException("TRANSACTION_NOT_FOUND");
    return rows[0];
  }

  async getTransactionByExternal(providerId: string, externalId: string) {
    const rows = await this.em.getConnection().execute<Array<Record<string, unknown>>>(
      "SELECT * FROM wager_transactions WHERE provider_id=? AND external_transaction_id=?",
      [providerId, externalId],
    );
    if (!rows.length) throw new NotFoundException("TRANSACTION_NOT_FOUND");
    return rows[0];
  }

  async getLedger(walletId: string, limitInput: string | number = "50", cursor?: string, direction = "forward") {
    await this.getWallet(walletId);
    const limit = Number(limitInput);
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) throw new BadRequestException("INVALID_LEDGER_LIMIT");
    if (direction !== "forward" && direction !== "backward") throw new BadRequestException("INVALID_LEDGER_DIRECTION");
    const decoded = cursor ? decodeCursor(cursor) : undefined;
    const order = direction === "forward" ? "ASC" : "DESC";
    const comparison = direction === "forward" ? ">" : "<";
    const params: [string, ...unknown[]] = [walletId];
    let where = "wallet_id=?";
    if (decoded) {
      where += ` AND (created_at ${comparison} ? OR (created_at = ? AND id ${comparison} ?))`;
      params.push(decoded.createdAt.toISOString(), decoded.createdAt.toISOString(), decoded.id);
    }
    const rows = await this.em.getConnection().execute<Array<{
      id: string;
      wallet_id: string;
      transaction_id: string;
      direction: LedgerDirection;
      amount: string;
      currency: string;
      balance_before: string;
      balance_after: string;
      created_at: Date;
    }>>(
      `SELECT id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after,created_at
       FROM wallet_ledger_entries WHERE ${where}
       ORDER BY created_at ${order},id ${order} LIMIT ${limit + 1}`,
      params,
    );
    const hasNext = rows.length > limit;
    const items = rows.slice(0, limit).map(row => ({
      id: row.id,
      walletId: row.wallet_id,
      transactionId: row.transaction_id,
      direction: row.direction,
      amount: moneyProps(row.amount, row.currency),
      balanceBefore: moneyProps(row.balance_before, row.currency),
      balanceAfter: moneyProps(row.balance_after, row.currency),
      createdAt: new Date(row.created_at).toISOString(),
    }));
    const last = rows[limit - 1];
    const wallet = await this.getWallet(walletId);
    return {
      walletId,
      items,
      nextCursor: hasNext && last
        ? encodeCursor({ createdAt: new Date(last.created_at).toISOString(), id: last.id })
        : null,
      limit,
      direction,
      balance: moneyProps(wallet.balance, wallet.currency),
    };
  }

  async reconcile(id: string) {
    const rows = await this.em.getConnection().execute<Array<{
      stored_balance: string;
      calculated_balance: string;
      currency: string;
      checked_entries: string;
    }>>(
      `SELECT       w.balance::text AS stored_balance,
      COALESCE(SUM(CASE WHEN l.direction='CREDIT' THEN l.amount ELSE -l.amount END),0)::text AS calculated_balance,
      w.currency,
              COUNT(l.id)::text AS checked_entries
       FROM wallets w
       LEFT JOIN wallet_ledger_entries l ON l.wallet_id=w.id
       WHERE w.id=?
       GROUP BY w.id`,
      [id],
    );
    if (!rows.length) throw new NotFoundException("WALLET_NOT_FOUND");
    const row = rows[0];
    const storedBalance = Money.from({ amount: normalizeAmount(row.stored_balance), currency: row.currency.trim() });
    const calculatedBalance = Money.from({ amount: normalizeAmount(row.calculated_balance), currency: row.currency.trim() });
    const difference = storedBalance.subtract(calculatedBalance).toJSON();
    const consistent = storedBalance.equals(calculatedBalance);
    if (!consistent) this.metrics.increment("reconciliation_mismatches_total");
    return {
      walletId: id,
      storedBalance: storedBalance.toJSON(),
      calculatedBalance: calculatedBalance.toJSON(),
      difference,
      consistent,
      checkedEntries: Number(row.checked_entries),
    };
  }
}
