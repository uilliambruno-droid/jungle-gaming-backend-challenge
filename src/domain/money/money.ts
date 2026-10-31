export interface MoneyProps {
  readonly amount: string;
  readonly currency: string;
}

/** Exact fixed-scale money backed by integer minor units. */
export class Money {
  private constructor(private readonly minorUnits: bigint, public readonly currency: string) {}

  static from({ amount, currency }: MoneyProps): Money {
    if (!/^[A-Z]{3}$/.test(currency)) throw new Error("INVALID_CURRENCY");
    if (!/^(0|[1-9]\d*)\.\d{2}$/.test(amount)) throw new Error("INVALID_AMOUNT");
    return new Money(BigInt(amount.replace(".", "")), currency);
  }

  static zero(currency: string): Money { return Money.from({ amount: "0.00", currency }); }

  add(other: Money): Money { this.assertSameCurrency(other); return new Money(this.minorUnits + other.minorUnits, this.currency); }
  subtract(other: Money): Money { this.assertSameCurrency(other); return new Money(this.minorUnits - other.minorUnits, this.currency); }
  negate(): Money { return new Money(-this.minorUnits, this.currency); }
  isZero(): boolean { return this.minorUnits === 0n; }
  isPositive(): boolean { return this.minorUnits > 0n; }
  isNegative(): boolean { return this.minorUnits < 0n; }
  isLessThan(other: Money): boolean { this.assertSameCurrency(other); return this.minorUnits < other.minorUnits; }
  equals(other: Money): boolean { this.assertSameCurrency(other); return this.minorUnits === other.minorUnits; }
  toJSON(): MoneyProps { return { amount: this.toString(), currency: this.currency }; }
  toString(): string {
    const absolute = this.minorUnits < 0n ? -this.minorUnits : this.minorUnits;
    const digits = absolute.toString().padStart(3, "0");
    return `${this.minorUnits < 0n ? "-" : ""}${digits.slice(0, -2)}.${digits.slice(-2)}`;
  }
  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) throw new Error("CURRENCY_MISMATCH");
  }
}
