import { randomUUID } from "node:crypto";
import { LedgerDirection, WalletLedgerEntry } from "../ledger/wallet-ledger-entry.js";
import { MoneyProps } from "../money/money.js";
import { WagerTransactionStatus } from "../wager/wager-transaction.js";
import { Wallet } from "../wallet/wallet.js";

export interface EventContext {
  readonly correlationId: string;
  readonly causationId?: string;
}

export interface IntegrationEventProps<T> extends EventContext {
  readonly eventId: string;
  readonly aggregateId: string;
  readonly occurredAt: Date;
  readonly data: T;
}

export interface IntegrationEventEnvelope<T> {
  eventId: string;
  eventType: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: string;
  version: number;
  data: T;
}

export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;
  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly occurredAt: Date;
  readonly data: Readonly<T>;

  protected constructor(props: IntegrationEventProps<T>) {
    this.eventId = props.eventId;
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    this.causationId = props.causationId;
    this.occurredAt = new Date(props.occurredAt);
    this.data = Object.freeze({ ...props.data });
  }

  toJSON(): IntegrationEventEnvelope<T> {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      ...(this.causationId ? { causationId: this.causationId } : {}),
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: this.data as T,
    };
  }
}

export interface TransactionEventData {
  transactionId: string;
  walletId: string;
  providerId: string;
  status: WagerTransactionStatus;
}

export class WagerTransactionProcessed extends IntegrationEvent<TransactionEventData> {
  readonly eventType = "WagerTransactionProcessed";
  readonly version = 1;

  static create(aggregateId: string, context: EventContext, data: TransactionEventData, occurredAt = new Date()) {
    return new WagerTransactionProcessed({ eventId: randomUUID(), aggregateId, ...context, occurredAt, data });
  }
}

export interface WagerTransactionRejectedData extends TransactionEventData {
  failureCode: string;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = "WagerTransactionRejected";
  readonly version = 1;

  static create(aggregateId: string, context: EventContext, data: WagerTransactionRejectedData, occurredAt = new Date()) {
    return new WagerTransactionRejected({ eventId: randomUUID(), aggregateId, ...context, occurredAt, data });
  }
}

export interface WagerTransactionPendingReferenceData extends TransactionEventData {
  referenceExternalTransactionId: string;
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = "WagerTransactionPendingReference";
  readonly version = 1;

  static create(aggregateId: string, context: EventContext, data: WagerTransactionPendingReferenceData, occurredAt = new Date()) {
    return new WagerTransactionPendingReference({ eventId: randomUUID(), aggregateId, ...context, occurredAt, data });
  }
}

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  providerId?: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = "WalletBalanceChanged";
  readonly version = 1;

  static from(
    wallet: Wallet,
    entry: WalletLedgerEntry,
    context: EventContext,
    providerId?: string,
  ): WalletBalanceChanged {
    return new WalletBalanceChanged({
      eventId: randomUUID(),
      aggregateId: wallet.id,
      ...context,
      occurredAt: entry.createdAt,
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        ...(providerId ? { providerId } : {}),
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: wallet.version,
      },
    });
  }
}
