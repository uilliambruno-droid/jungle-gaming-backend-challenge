import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import { ValidationPipe } from "@nestjs/common";
import { AppModule } from "./app.module.js";
async function bootstrap(){const app=await NestFactory.create(AppModule);app.useGlobalPipes(new ValidationPipe({whitelist:true,transform:true}));await app.listen(Number(process.env.PORT??3000));}
bootstrap().catch(error=>{console.error(JSON.stringify({level:"error",message:"bootstrap_failed",error:String(error)}));process.exit(1)});
