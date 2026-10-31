import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { MetricsService } from "../observability/metrics.service.js";

function minor(value: string): bigint { const [whole, fraction = "00"] = String(value).split("."); return BigInt(whole) * 100n + BigInt(fraction.padEnd(2, "0")); }
function amount(value: bigint): string { return `${value / 100n}.${(value % 100n).toString().padStart(2, "0")}`; }

interface PendingRow { id:string; provider_id:string; external_transaction_id:string; reference_external_transaction_id:string; wallet_id:string; player_id:string; round_id:string; amount:string; currency:string; kind:string; reference_attempts:number; }

@Injectable()
export class PendingReferenceWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PendingReferenceWorker.name);
  private running = true;
  private loopPromise?: Promise<void>;

  constructor(private readonly em: EntityManager, private readonly metrics: MetricsService) {}

  onModuleInit(): void { this.loopPromise = this.loop(); }
  async onModuleDestroy(): Promise<void> { this.running = false; await this.loopPromise; }

  private async loop(): Promise<void> {
    while (this.running) {
      try {
        const processed = await this.processOne();
        if (!processed) await new Promise(resolve => setTimeout(resolve, Number(process.env.PENDING_REFERENCE_POLL_INTERVAL_MS ?? "1000")));
      } catch (error) {
        this.logger.error(JSON.stringify({ event: "pending_reference_worker_failed", category: "transient", error: String(error) }));
        await new Promise(resolve => setTimeout(resolve, Number(process.env.PENDING_REFERENCE_ERROR_DELAY_MS ?? "1000")));
      }
    }
  }

  private async processOne(): Promise<boolean> {
    return this.em.transactional(async tm => {
      const c = tm.getConnection();
      const rows = await c.execute<PendingRow[]>("SELECT id,provider_id,external_transaction_id,reference_external_transaction_id,wallet_id,player_id,round_id,amount,currency,kind,reference_attempts FROM wager_transactions WHERE status='PENDING_REFERENCE' AND (reference_next_attempt_at IS NULL OR reference_next_attempt_at <= now()) ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1");
      if (!rows.length) return false;
      const row = rows[0];
      const refs = await c.execute<any[]>("SELECT * FROM wager_transactions WHERE provider_id=? AND external_transaction_id=? FOR UPDATE", [row.provider_id, row.reference_external_transaction_id]);
      const max = Number(process.env.PENDING_REFERENCE_MAX_ATTEMPTS ?? "8");
      const nextAttempt = row.reference_attempts + 1;
      if (!refs.length) {
        if (nextAttempt >= max) {
          await c.execute("UPDATE wager_transactions SET status='REJECTED',failure_code='REFERENCE_NOT_FOUND',reference_attempts=?,reference_next_attempt_at=NULL,processed_at=? WHERE id=?", [nextAttempt, new Date(), row.id]);
          await this.event(c, row, "WagerTransactionRejected", { transactionId: row.id, walletId: row.wallet_id, failureCode: "REFERENCE_NOT_FOUND" });
          this.logger.warn(JSON.stringify({ event: "pending_reference_expired", transactionId: row.id, attempts: nextAttempt }));
          this.metrics.increment("pending_reference_expired_total");
        } else {
          await c.execute("UPDATE wager_transactions SET reference_attempts=?,reference_next_attempt_at=now() + (POWER(2, LEAST(?, 8)) * interval '1 second') WHERE id=?", [nextAttempt, nextAttempt, row.id]);
          this.logger.log(JSON.stringify({ event: "pending_reference_retry", transactionId: row.id, attempts: nextAttempt }));
          this.metrics.increment("pending_reference_retries_total");
        }
        return true;
      }
      const ref = refs[0];
      const valid = ref.player_id === row.player_id && ref.wallet_id === row.wallet_id && ref.round_id === row.round_id && String(ref.currency).trim() === String(row.currency).trim() && minor(String(ref.amount)) === minor(String(row.amount)) && ref.status === "PROCESSED" && (row.kind === "REFUND" ? ref.kind === "BET" : ["BET", "WIN", "REFUND"].includes(ref.kind));
      if (!valid) {
        await c.execute("UPDATE wager_transactions SET status='REJECTED',failure_code='INVALID_REFERENCE',reference_attempts=?,processed_at=? WHERE id=?", [nextAttempt, new Date(), row.id]);
        await this.event(c, row, "WagerTransactionRejected", { transactionId: row.id, walletId: row.wallet_id, failureCode: "INVALID_REFERENCE" });
        return true;
      }
      const walletRows = await c.execute<any[]>("SELECT * FROM wallets WHERE id=? FOR UPDATE", [row.wallet_id]);
      const wallet = walletRows[0];
      const delta = ref.kind === "BET" || ref.kind === "REFUND" ? minor(String(row.amount)) : -minor(String(row.amount));
      const before = minor(String(wallet.balance));
      const after = before + delta;
      if (after < 0n) {
        await c.execute("UPDATE wager_transactions SET status='REJECTED',failure_code='REVERSAL_WOULD_OVERDRAW',reference_attempts=?,processed_at=? WHERE id=?", [nextAttempt, new Date(), row.id]);
        await this.event(c, row, "WagerTransactionRejected", { transactionId: row.id, walletId: row.wallet_id, failureCode: "REVERSAL_WOULD_OVERDRAW" });
        return true;
      }
      const now = new Date();
      await c.execute("UPDATE wallets SET balance=?,version=version+1,updated_at=? WHERE id=?", [amount(after), now, row.wallet_id]);
      await c.execute("INSERT INTO wallet_ledger_entries(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after,created_at) VALUES(gen_random_uuid(),?,?,?,?,?,?,?,?)", [row.wallet_id, row.id, delta > 0n ? "CREDIT" : "DEBIT", row.amount, row.currency, amount(before), amount(after), now]);
      await c.execute("UPDATE wager_transactions SET status='PROCESSED',reference_transaction_id=?,reference_attempts=?,reference_next_attempt_at=NULL,processed_at=? WHERE id=?", [ref.id, nextAttempt, now, row.id]);
      await this.event(c, row, "WagerTransactionProcessed", { transactionId: row.id, walletId: row.wallet_id, status: "PROCESSED" });
      await this.event(c, row, "WalletBalanceChanged", { transactionId: row.id, walletId: row.wallet_id, balanceBefore: amount(before), balanceAfter: amount(after) });
      this.logger.log(JSON.stringify({ event: "pending_reference_processed", transactionId: row.id, referenceTransactionId: ref.id }));
      this.metrics.increment("pending_reference_processed_total");
      return true;
    });
  }

  private async event(c: ReturnType<EntityManager["getConnection"]>, row: PendingRow, eventType: string, payload: Record<string, unknown>): Promise<void> {
    await c.execute("INSERT INTO outbox_messages(id,aggregate_id,event_type,payload,occurred_at) VALUES(gen_random_uuid(),?,?,?,?)", [row.wallet_id, eventType, JSON.stringify(payload), new Date()]);
  }
}
