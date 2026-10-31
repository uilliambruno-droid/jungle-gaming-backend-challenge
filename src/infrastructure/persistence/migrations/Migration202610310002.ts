import { Migration } from "@mikro-orm/migrations";

export class Migration202610310002 extends Migration {
  async up(): Promise<void> {
    this.addSql("ALTER TABLE wager_transactions ADD COLUMN reference_attempts integer NOT NULL DEFAULT 0;");
    this.addSql("ALTER TABLE wager_transactions ADD COLUMN reference_next_attempt_at timestamptz;");
    this.addSql("CREATE INDEX wager_pending_reference_idx ON wager_transactions(status, reference_next_attempt_at, created_at) WHERE status = 'PENDING_REFERENCE';");
  }

  async down(): Promise<void> {
    this.addSql("DROP INDEX wager_pending_reference_idx;");
    this.addSql("ALTER TABLE wager_transactions DROP COLUMN reference_next_attempt_at;");
    this.addSql("ALTER TABLE wager_transactions DROP COLUMN reference_attempts;");
  }
}
