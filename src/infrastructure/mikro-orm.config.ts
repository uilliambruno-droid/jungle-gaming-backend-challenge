import { defineConfig } from "@mikro-orm/postgresql";
import { Migrator } from "@mikro-orm/migrations";
import { WalletEntity,WagerTransactionEntity,WalletLedgerEntryEntity,InboxMessageEntity,OutboxMessageEntity } from "./persistence/entities.js";
export default defineConfig({ clientUrl: process.env.DATABASE_URL ?? "postgresql://wager:wager@localhost:5433/wagering", entities:[WalletEntity,WagerTransactionEntity,WalletLedgerEntryEntity,InboxMessageEntity,OutboxMessageEntity], extensions:[Migrator], migrations:{path:"./src/infrastructure/persistence/migrations",pathTs:"./src/infrastructure/persistence/migrations",glob:"!*.[jt]s.map"} });
