import { Body,Controller,Get,Headers,Param,Post,Query,ConflictException,ServiceUnavailableException } from "@nestjs/common";
import { CreateWalletDto,WagerDto } from "./dtos.js";
import { WagerService } from "../application/wager.service.js";
import { HealthService } from "../infrastructure/observability/health.service.js";
import { MetricsService } from "../infrastructure/observability/metrics.service.js";
@Controller("wallets") export class WalletController { constructor(private readonly service:WagerService){}
  @Post() create(@Body() dto:CreateWalletDto){return this.service.createWallet(dto)}
  @Get(":id") get(@Param("id") id:string){return this.service.getWallet(id)}
  @Post(":id/reconciliation") reconcile(@Param("id") id:string){return this.service.reconcile(id)}
  @Get(":id/ledger") ledger(@Param("id") id:string,@Query("limit") limit="50",@Query("cursor") cursor?:string,@Query("direction") direction="forward"){return this.service.getLedger(id,limit,cursor,direction)}
}
@Controller("wagering/transactions") export class WagerController { constructor(private readonly service:WagerService){}
  @Post() submit(@Body() dto:WagerDto,@Headers("idempotency-key") key?:string){if(!key)throw new ConflictException("IDEMPOTENCY_KEY_REQUIRED");return this.service.submit({...dto,kind:dto.kind as any,idempotencyKey:key})}
  @Get(":id") get(@Param("id") id:string){return this.service.getTransaction(id)}
}
@Controller("providers") export class ProviderController { constructor(private readonly service:WagerService){} @Get(":providerId/wagering/transactions/:externalId") get(@Param("providerId") p:string,@Param("externalId") e:string){return this.service.getTransactionByExternal(p,e)} }
@Controller("health") export class HealthController { constructor(private readonly health:HealthService,private readonly metrics:MetricsService){} @Get("live") live(){return {status:"ok"}} @Get("ready") async ready(){try{return await this.health.readiness()}catch(error){throw new ServiceUnavailableException({status:"not_ready",error:String(error)})}} @Get("metrics") metricsSnapshot(){return {status:"ok",metrics:this.metrics.snapshot()}} }
