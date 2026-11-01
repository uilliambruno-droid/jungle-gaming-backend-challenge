import { describe,expect,test } from "bun:test";
import { Money } from "../../src/domain/money/money.js";
import { LedgerDirection } from "../../src/domain/ledger/wallet-ledger-entry.js";
import { WagerTransaction,WagerTransactionKind,WagerTransactionStatus } from "../../src/domain/wager/wager-transaction.js";
describe("WagerTransaction",()=>{const money=Money.from({amount:"10.00",currency:"BRL"});const base={id:"t",providerId:"p",externalTransactionId:"e",idempotencyKey:"p:e",payloadHash:"h",walletId:"w",playerId:"pl",roundId:"r",gameId:"g",kind:WagerTransactionKind.Bet,money};
 test("starts pending and has terminal transitions",()=>{const tx=WagerTransaction.create(base);expect(tx.status).toBe(WagerTransactionStatus.Pending);tx.markProcessed(undefined,new Date(0));expect(tx.status).toBe(WagerTransactionStatus.Processed);expect(()=>tx.reject("INVALID_PAYLOAD")).toThrow("INVALID_TRANSACTION_STATE")});
 test("requires a reference for reversals",()=>{expect(()=>WagerTransaction.create({...base,kind:WagerTransactionKind.Refund})).toThrow("INVALID_REFERENCE")});
 test("derives reversal ledger direction from the referenced operation",()=>{
  const bet=WagerTransaction.rehydrate({...base,status:WagerTransactionStatus.Processed,createdAt:new Date(0)});
  const rollback=WagerTransaction.create({...base,kind:WagerTransactionKind.Rollback,referenceExternalTransactionId:"bet-external"});
  expect(rollback.ledgerDirectionFor(bet)).toBe(LedgerDirection.Credit);
  const win=WagerTransaction.rehydrate({...base,kind:WagerTransactionKind.Win,status:WagerTransactionStatus.Processed,createdAt:new Date(0)});
  expect(rollback.ledgerDirectionFor(win)).toBe(LedgerDirection.Debit);
 });
});
