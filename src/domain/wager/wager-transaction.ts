import { Money } from "../money/money.js";
import { LedgerDirection } from "../ledger/wallet-ledger-entry.js";

export enum WagerTransactionKind { Opening="OPENING", Bet="BET", Win="WIN", Loss="LOSS", Refund="REFUND", Rollback="ROLLBACK" }
export enum WagerTransactionStatus { Pending="PENDING", PendingReference="PENDING_REFERENCE", Processed="PROCESSED", Rejected="REJECTED", Failed="FAILED" }
export type FailureCode = "INSUFFICIENT_FUNDS" | "REVERSAL_WOULD_OVERDRAW" | "REFERENCE_NOT_FOUND" | "INVALID_REFERENCE" | "DUPLICATE_REVERSAL" | "WALLET_MISMATCH" | "INVALID_PAYLOAD";

export interface WagerState { id:string; providerId:string; externalTransactionId:string; idempotencyKey:string; payloadHash:string; walletId:string; playerId:string; roundId:string; gameId:string; kind:WagerTransactionKind; money:Money; referenceExternalTransactionId?:string; status:WagerTransactionStatus; referenceTransactionId?:string; failureCode?:FailureCode; createdAt:Date; processedAt?:Date; }
export interface CreateWagerProps extends Omit<WagerState,"id"|"status"|"createdAt"> { id:string; now?:Date; }

export class WagerTransaction {
  private constructor(private readonly state: WagerState) {}
  static create(p: CreateWagerProps): WagerTransaction {
    const needsRef = p.kind === WagerTransactionKind.Refund || p.kind === WagerTransactionKind.Rollback;
    if (p.kind === WagerTransactionKind.Opening || (needsRef && !p.referenceExternalTransactionId)) throw new Error("INVALID_REFERENCE");
    if (!p.money.isPositive()) throw new Error("INVALID_PAYLOAD");
    return new WagerTransaction({ ...p, status:WagerTransactionStatus.Pending, createdAt:new Date(p.now ?? Date.now()) });
  }
  static rehydrate(state: WagerState): WagerTransaction { return new WagerTransaction({ ...state, createdAt:new Date(state.createdAt), processedAt:state.processedAt ? new Date(state.processedAt):undefined }); }
  get id(){return this.state.id} get providerId(){return this.state.providerId} get externalTransactionId(){return this.state.externalTransactionId}
  get idempotencyKey(){return this.state.idempotencyKey} get payloadHash(){return this.state.payloadHash} get walletId(){return this.state.walletId}
  get playerId(){return this.state.playerId} get roundId(){return this.state.roundId} get gameId(){return this.state.gameId} get kind(){return this.state.kind}
  get money(){return this.state.money} get referenceExternalTransactionId(){return this.state.referenceExternalTransactionId} get status(){return this.state.status}
  get referenceTransactionId(){return this.state.referenceTransactionId} get failureCode(){return this.state.failureCode} get createdAt(){return new Date(this.state.createdAt)} get processedAt(){return this.state.processedAt ? new Date(this.state.processedAt):undefined}
  isTerminal(){return [WagerTransactionStatus.Processed,WagerTransactionStatus.Rejected,WagerTransactionStatus.Failed].includes(this.status)}
  affectsBalance(){return this.kind !== WagerTransactionKind.Loss}
  requiresReference(){return this.kind === WagerTransactionKind.Refund || this.kind === WagerTransactionKind.Rollback}
  matchesPayload(hash:string){return this.payloadHash===hash}
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    if (this.kind === WagerTransactionKind.Bet) return LedgerDirection.Debit;
    if (this.kind === WagerTransactionKind.Win || this.kind === WagerTransactionKind.Refund) return LedgerDirection.Credit;
    if (this.kind === WagerTransactionKind.Rollback && reference) {
      return reference.kind === WagerTransactionKind.Bet ? LedgerDirection.Credit : LedgerDirection.Debit;
    }
    throw new Error("INVALID_LEDGER_DIRECTION");
  }
  markPendingReference(){this.assertOpen(); this.state.status=WagerTransactionStatus.PendingReference}
  markProcessed(referenceTransactionId:string|undefined, at:Date){this.assertOpen(); this.state.status=WagerTransactionStatus.Processed; this.state.referenceTransactionId=referenceTransactionId; this.state.processedAt=new Date(at)}
  reject(code:FailureCode){this.assertOpen(); this.state.status=WagerTransactionStatus.Rejected; this.state.failureCode=code}
  fail(code:FailureCode){this.assertOpen(); this.state.status=WagerTransactionStatus.Failed; this.state.failureCode=code}
  private assertOpen(){if(this.isTerminal()) throw new Error("INVALID_TRANSACTION_STATE")}
  toState():WagerState{return {...this.state,createdAt:new Date(this.state.createdAt),processedAt:this.state.processedAt?new Date(this.state.processedAt):undefined}}
}
