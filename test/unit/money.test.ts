import { describe, expect, test } from "bun:test";
import { Money } from "../../src/domain/money/money.js";

describe("Money", () => {
  test("does exact arithmetic with fixed two-place strings", () => {
    expect(Money.from({ amount: "100000000000000000000.01", currency: "BRL" })
      .subtract(Money.from({ amount: "0.02", currency: "BRL" })).toJSON())
      .toEqual({ amount: "99999999999999999999.99", currency: "BRL" });
  });
  test("rejects malformed amounts and currencies", () => {
    for (const amount of ["", "1", "1.234", "1e2", "-1.00", "NaN", "Infinity"]) {
      expect(() => Money.from({ amount, currency: "BRL" })).toThrow();
    }
    expect(() => Money.from({ amount: "1.00", currency: "US" })).toThrow();
  });
  test("rejects arithmetic across currencies", () => {
    expect(() => Money.from({ amount: "1.00", currency: "BRL" })
      .add(Money.from({ amount: "1.00", currency: "USD" }))).toThrow("CURRENCY_MISMATCH");
  });
  test("serializes a signed internal difference without accepting negative input", () => {
    const difference = Money.from({ amount: "1.00", currency: "BRL" })
      .subtract(Money.from({ amount: "2.50", currency: "BRL" }));
    expect(difference.toJSON()).toEqual({ amount: "-1.50", currency: "BRL" });
    expect(difference.isNegative()).toBe(true);
    expect(() => Money.from(difference.toJSON())).toThrow("INVALID_AMOUNT");
  });
});
