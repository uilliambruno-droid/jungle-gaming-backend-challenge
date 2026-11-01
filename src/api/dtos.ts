import { IsIn, IsOptional, IsString, Matches, ValidateNested, IsUUID } from "class-validator";
import { Type } from "class-transformer";
import { WagerTransactionKind } from "../domain/wager/wager-transaction.js";
export class MoneyDto { @IsString() @Matches(/^(0|[1-9]\d*)\.\d{2}$/) amount!:string; @IsString() @Matches(/^[A-Z]{3}$/) currency!:string; }
export class CreateWalletDto { @IsString() playerId!:string; @ValidateNested() @Type(()=>MoneyDto) initialBalance!:MoneyDto; }
export class WagerDto { @IsString() providerId!:string; @IsString() externalTransactionId!:string; @IsString() playerId!:string; @IsUUID() walletId!:string; @IsString() roundId!:string; @IsString() gameId!:string; @IsIn(["BET","WIN","LOSS","REFUND","ROLLBACK"]) kind!:WagerTransactionKind; @ValidateNested() @Type(()=>MoneyDto) money!:MoneyDto; @IsOptional() @IsString() referenceExternalTransactionId?:string; }
