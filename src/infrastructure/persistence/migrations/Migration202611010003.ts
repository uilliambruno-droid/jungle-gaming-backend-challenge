import { Migration } from "@mikro-orm/migrations";

export class Migration202611010003 extends Migration {
  async up(): Promise<void> {
    this.addSql(`
      ALTER TABLE wager_transactions
        ADD COLUMN response_status text,
        ADD COLUMN response_balance numeric(30,2),
        ADD COLUMN response_currency char(3),
        ADD COLUMN response_failure_code text,
        ADD COLUMN correlation_id text,
        ADD COLUMN causation_id text;

      UPDATE wager_transactions t
      SET response_status = t.status,
          response_balance = COALESCE(
            (SELECT l.balance_after FROM wallet_ledger_entries l WHERE l.transaction_id = t.id LIMIT 1),
            (SELECT l.balance_after
             FROM wallet_ledger_entries l
             WHERE l.wallet_id = t.wallet_id
               AND l.created_at <= COALESCE(t.processed_at, t.created_at)
             ORDER BY l.created_at DESC, l.id DESC
             LIMIT 1),
            0
          ),
          response_currency = w.currency,
          response_failure_code = t.failure_code,
          correlation_id = t.id::text
      FROM wallets w
      WHERE w.id = t.wallet_id;

      ALTER TABLE wager_transactions
        ALTER COLUMN response_status SET NOT NULL,
        ALTER COLUMN response_balance SET NOT NULL,
        ALTER COLUMN response_currency SET NOT NULL,
        ALTER COLUMN correlation_id SET NOT NULL,
        ADD CONSTRAINT wager_response_status_valid
          CHECK (response_status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        ADD CONSTRAINT wager_response_balance_nonnegative CHECK (response_balance >= 0);

      ALTER TABLE inbox_messages
        ADD COLUMN transaction_id uuid REFERENCES wager_transactions(id);
      UPDATE inbox_messages i
      SET transaction_id = (
        SELECT t.id FROM wager_transactions t
        WHERE t.payload_hash = i.payload_hash
        ORDER BY t.processed_at NULLS LAST, t.id
        LIMIT 1
      )
      WHERE i.processed_at IS NOT NULL;

      ALTER TABLE wallet_ledger_entries
        ADD CONSTRAINT wallet_ledger_balance_matches_direction
        CHECK (
          balance_after = CASE
            WHEN direction = 'CREDIT' THEN balance_before + amount
            ELSE balance_before - amount
          END
        );

      CREATE UNIQUE INDEX wager_processed_reversal_unique
        ON wager_transactions(reference_transaction_id, kind)
        WHERE reference_transaction_id IS NOT NULL
          AND kind IN ('REFUND','ROLLBACK')
          AND status = 'PROCESSED';
    `);
  }

  async down(): Promise<void> {
    this.addSql(`
      DROP INDEX wager_processed_reversal_unique;
      ALTER TABLE wallet_ledger_entries DROP CONSTRAINT wallet_ledger_balance_matches_direction;
      ALTER TABLE inbox_messages DROP COLUMN transaction_id;
      ALTER TABLE wager_transactions
        DROP CONSTRAINT wager_response_balance_nonnegative,
        DROP CONSTRAINT wager_response_status_valid,
        DROP COLUMN causation_id,
        DROP COLUMN correlation_id,
        DROP COLUMN response_failure_code,
        DROP COLUMN response_currency,
        DROP COLUMN response_balance,
        DROP COLUMN response_status;
    `);
  }
}
