import { Money } from "../money/money.js";

export enum LedgerDirection { Debit = "DEBIT", Credit = "CREDIT" }

export interface CreateLedgerEntryProps {
  readonly id: string;
  readonly walletId: string;
  readonly transactionId: string;
  readonly direction: LedgerDirection;
  readonly money: Money;
  readonly balanceBefore: Money;
  readonly createdAt: Date;
}

export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {}

  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    if (!props.money.isPositive()) throw new Error("LEDGER_AMOUNT_MUST_BE_POSITIVE");
    const after = props.direction === LedgerDirection.Credit
      ? props.balanceBefore.add(props.money)
      : props.balanceBefore.subtract(props.money);
    if (after.isNegative()) throw new Error("LEDGER_BALANCE_CANNOT_BE_NEGATIVE");
    return new WalletLedgerEntry(props.id, props.walletId, props.transactionId, props.direction,
      props.money, props.balanceBefore, after, new Date(props.createdAt));
  }

  static rehydrate(state: WalletLedgerEntry): WalletLedgerEntry {
    return new WalletLedgerEntry(state.id, state.walletId, state.transactionId, state.direction,
      state.money, state.balanceBefore, state.balanceAfter, new Date(state.createdAt));
  }

  isBalanced(): boolean {
    const expected = this.direction === LedgerDirection.Credit
      ? this.balanceBefore.add(this.money)
      : this.balanceBefore.subtract(this.money);
    return expected.equals(this.balanceAfter);
  }
}
