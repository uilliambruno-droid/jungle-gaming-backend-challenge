import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  Param,
  Post,
  Query,
  Res,
  ServiceUnavailableException,
} from "@nestjs/common";
import type { Response } from "express";
import { randomUUID } from "node:crypto";
import { WagerTransactionStatus } from "../domain/wager/wager-transaction.js";
import { CreateWalletDto,WagerDto } from "./dtos.js";
import { WagerService } from "../application/wager.service.js";
import { HealthService } from "../infrastructure/observability/health.service.js";
import { MetricsService } from "../infrastructure/observability/metrics.service.js";
@Controller("wallets") export class WalletController { constructor(private readonly service:WagerService){}
  @Post() create(@Body() dto:CreateWalletDto){return this.service.createWallet(dto)}
  @Get(":id") get(@Param("id") id:string){return this.service.getWallet(id)}
  @Post(":id/reconciliation") @HttpCode(200) reconcile(@Param("id") id:string){return this.service.reconcile(id)}
  @Get(":id/ledger") ledger(@Param("id") id:string,@Query("limit") limit="50",@Query("cursor") cursor?:string,@Query("direction") direction="forward"){return this.service.getLedger(id,limit,cursor,direction)}
}
@Controller("wagering/transactions") export class WagerController { constructor(private readonly service:WagerService){}
  @Post() async submit(
    @Body() dto:WagerDto,
    @Headers("idempotency-key") key:string|undefined,
    @Headers("x-correlation-id") correlationId:string|undefined,
    @Res({passthrough:true}) response:Response,
  ){
    if(!key)throw new BadRequestException("IDEMPOTENCY_KEY_REQUIRED");
    const result=await this.service.submit(
      {...dto,kind:dto.kind,idempotencyKey:key},
      undefined,
      {correlationId:correlationId?.trim()||randomUUID()},
    );
    if(result.status===WagerTransactionStatus.PendingReference)response.status(202);
    else if(result.status===WagerTransactionStatus.Rejected)response.status(422);
    else if(result.status===WagerTransactionStatus.Failed)response.status(503);
    else response.status(200);
    return result;
  }
  @Get(":id") get(@Param("id") id:string){return this.service.getTransaction(id)}
}
@Controller("providers") export class ProviderController { constructor(private readonly service:WagerService){} @Get(":providerId/wagering/transactions/:externalId") get(@Param("providerId") p:string,@Param("externalId") e:string){return this.service.getTransactionByExternal(p,e)} }
@Controller("health") export class HealthController { constructor(private readonly health:HealthService,private readonly metrics:MetricsService){} @Get("live") live(){return {status:"ok"}} @Get("ready") async ready(){try{return await this.health.readiness()}catch{throw new ServiceUnavailableException({status:"not_ready",error:"DEPENDENCY_UNAVAILABLE"})}} @Get("metrics") metricsSnapshot(){return {status:"ok",metrics:this.metrics.snapshot()}} }
