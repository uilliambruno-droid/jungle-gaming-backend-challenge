import { Migration } from "@mikro-orm/migrations";

export class Migration202611010004 extends Migration {
  async up(): Promise<void> {
    this.addSql(`
      CREATE FUNCTION enforce_wallet_ledger_consistency() RETURNS trigger
      LANGUAGE plpgsql AS $$
      DECLARE
        target_wallet_id uuid;
        stored_balance numeric(30,2);
        ledger_balance numeric(30,2);
        wallet_currency char(3);
      BEGIN
        IF TG_TABLE_NAME = 'wallets' THEN
          target_wallet_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END;
        ELSE
          target_wallet_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.wallet_id ELSE NEW.wallet_id END;
        END IF;

        SELECT balance, currency INTO stored_balance, wallet_currency
        FROM wallets WHERE id = target_wallet_id;
        IF NOT FOUND THEN
          RETURN NULL;
        END IF;

        SELECT COALESCE(SUM(
          CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END
        ), 0) INTO ledger_balance
        FROM wallet_ledger_entries
        WHERE wallet_id = target_wallet_id;

        IF stored_balance IS DISTINCT FROM ledger_balance
           OR EXISTS (
             SELECT 1 FROM wallet_ledger_entries
             WHERE wallet_id = target_wallet_id AND currency <> wallet_currency
           ) THEN
          RAISE EXCEPTION 'wallet_ledger_balance_mismatch'
            USING ERRCODE = '23514', CONSTRAINT = 'wallet_ledger_balance_consistency';
        END IF;
        RETURN NULL;
      END;
      $$;

      CREATE CONSTRAINT TRIGGER wallet_ledger_consistency_on_wallet
        AFTER INSERT OR UPDATE ON wallets
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION enforce_wallet_ledger_consistency();

      CREATE CONSTRAINT TRIGGER wallet_ledger_consistency_on_entry
        AFTER INSERT ON wallet_ledger_entries
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION enforce_wallet_ledger_consistency();
    `);
  }

  async down(): Promise<void> {
    this.addSql(`
      DROP TRIGGER wallet_ledger_consistency_on_entry ON wallet_ledger_entries;
      DROP TRIGGER wallet_ledger_consistency_on_wallet ON wallets;
      DROP FUNCTION enforce_wallet_ledger_consistency();
    `);
  }
}
