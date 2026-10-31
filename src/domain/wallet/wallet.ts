import { WalletLedgerEntry, LedgerDirection } from "../ledger/wallet-ledger-entry.js";
import { Money } from "../money/money.js";

export interface WalletState {
  readonly id: string;
  readonly playerId: string;
  readonly balance: Money;
  readonly version: number;
  readonly createdAt: Date;
  readonly updatedAt: Date;
}

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: { id: string; playerId: string; initialBalance: Money; now?: Date }): Wallet {
    if (props.initialBalance.isNegative()) throw new Error("INITIAL_BALANCE_CANNOT_BE_NEGATIVE");
    const now = new Date(props.now ?? new Date());
    return new Wallet(props.id, props.playerId, props.initialBalance.currency,
      props.initialBalance, 1, now, now);
  }

  static rehydrate(state: WalletState): Wallet {
    return new Wallet(state.id, state.playerId, state.balance.currency, state.balance, state.version,
      new Date(state.createdAt), new Date(state.updatedAt));
  }

  get balance(): Money { return this._balance; }
  get version(): number { return this._version; }
  get updatedAt(): Date { return new Date(this._updatedAt); }

  debit(amount: Money, entry: { id: string; transactionId: string; at: Date }): WalletLedgerEntry {
    return this.apply(amount, LedgerDirection.Debit, entry);
  }

  credit(amount: Money, entry: { id: string; transactionId: string; at: Date }): WalletLedgerEntry {
    return this.apply(amount, LedgerDirection.Credit, entry);
  }

  private apply(amount: Money, direction: LedgerDirection, meta: { id: string; transactionId: string; at: Date }): WalletLedgerEntry {
    if (amount.currency !== this.currency) throw new Error("CURRENCY_MISMATCH");
    const entry = WalletLedgerEntry.create({ id: meta.id, walletId: this.id, transactionId: meta.transactionId,
      direction, money: amount, balanceBefore: this._balance, createdAt: meta.at });
    if (!entry.balanceAfter.equals(this._balance)) {
      this._balance = entry.balanceAfter;
      this._version += 1;
      this._updatedAt = new Date(meta.at);
    }
    return entry;
  }
}
