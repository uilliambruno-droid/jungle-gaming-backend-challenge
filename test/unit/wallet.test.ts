import { describe, expect, test } from "bun:test";
import { Money } from "../../src/domain/money/money.js";
import { Wallet } from "../../src/domain/wallet/wallet.js";

describe("Wallet and ledger", () => {
  const money = (amount: string) => Money.from({ amount, currency: "BRL" });
  test("creates balanced debit and increments version only when balance changes", () => {
    const wallet = Wallet.open({ id: "w1", playerId: "p1", initialBalance: money("100.00"), now: new Date(0) });
    const entry = wallet.debit(money("25.00"), { id: "l1", transactionId: "t1", at: new Date(1) });
    expect(entry.isBalanced()).toBe(true);
    expect(wallet.balance.toString()).toBe("75.00");
    expect(wallet.version).toBe(2);
  });
  test("rejects overdraft and currency mismatch without changing state", () => {
    const wallet = Wallet.open({ id: "w1", playerId: "p1", initialBalance: money("10.00") });
    expect(() => wallet.debit(money("11.00"), { id: "l", transactionId: "t", at: new Date() })).toThrow("LEDGER_BALANCE_CANNOT_BE_NEGATIVE");
    expect(() => wallet.credit(Money.from({ amount: "1.00", currency: "USD" }), { id: "l2", transactionId: "t2", at: new Date() })).toThrow("CURRENCY_MISMATCH");
    expect(wallet.balance.toString()).toBe("10.00");
    expect(wallet.version).toBe(1);
  });

  test("does not expose mutable date references", () => {
    const createdAt = new Date(0);
    const wallet = Wallet.open({ id: "w-date", playerId: "p-date", initialBalance: money("1.00"), now: createdAt });
    createdAt.setTime(10);
    const exposedCreatedAt = wallet.createdAt;
    exposedCreatedAt.setTime(20);
    expect(wallet.createdAt.getTime()).toBe(0);

    const entry = wallet.credit(money("1.00"), {
      id: "l-date",
      transactionId: "t-date",
      at: new Date(30),
    });
    const exposedEntryDate = entry.createdAt;
    exposedEntryDate.setTime(40);
    expect(entry.createdAt.getTime()).toBe(30);
  });
});
