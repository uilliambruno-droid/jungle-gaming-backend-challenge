import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import { ValidationPipe } from "@nestjs/common";
import { AppModule } from "./app.module.js";

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  app.enableShutdownHooks(["SIGTERM", "SIGINT"]);
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  await app.listen(Number(process.env.PORT ?? 3000));
}

bootstrap().catch(error => {
  console.error(JSON.stringify({
    level: "error",
    message: "bootstrap_failed",
    errorCode: error instanceof Error ? error.name : "UNKNOWN_ERROR",
  }));
  process.exit(1);
});
