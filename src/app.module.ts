import { Module } from "@nestjs/common";
import { MikroOrmModule } from "@mikro-orm/nestjs";
import config from "./infrastructure/mikro-orm.config.js";
import { WalletEntity,WagerTransactionEntity,WalletLedgerEntryEntity,InboxMessageEntity,OutboxMessageEntity } from "./infrastructure/persistence/entities.js";
import { WagerService } from "./application/wager.service.js";
import { SqsConsumer } from "./infrastructure/messaging/sqs.consumer.js";
import { OutboxPublisher } from "./infrastructure/messaging/outbox.publisher.js";
import { PendingReferenceWorker } from "./infrastructure/messaging/pending-reference.worker.js";
import { HealthService } from "./infrastructure/observability/health.service.js";
import { MetricsService } from "./infrastructure/observability/metrics.service.js";
import { WalletController,WagerController,ProviderController,HealthController } from "./api/controllers.js";
@Module({imports:[MikroOrmModule.forRoot(config),MikroOrmModule.forFeature([WalletEntity,WagerTransactionEntity,WalletLedgerEntryEntity,InboxMessageEntity,OutboxMessageEntity])],controllers:[WalletController,WagerController,ProviderController,HealthController],providers:[WagerService,SqsConsumer,OutboxPublisher,PendingReferenceWorker,HealthService,MetricsService],exports:[WagerService]}) export class AppModule{}
