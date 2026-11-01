# Processador distribuído de transações de apostas

Serviço NestJS/TypeScript executado com Bun para processar `BET`, `WIN`, `LOSS`, `REFUND` e `ROLLBACK` com saldo exato, ledger imutável, idempotência persistente e eventos via SQS/Outbox.

## Requisitos

- Bun 1.x
- Docker Desktop com Docker Compose v2
- Git

## Subir localmente

No PowerShell:

```powershell
Copy-Item .env.example .env
bun install
docker compose up -d postgres localstack
bun run db:migrate
bun run sqs:setup
bun run dev
```

O PostgreSQL fica em `localhost:5433`, o LocalStack em `localhost:4566` e a API em `localhost:3000`. As filas são `wager-transactions.fifo`, `wager-transactions-dlq.fifo` e `wager-events.fifo`.

Para verificar as dependências:

```powershell
Invoke-RestMethod http://localhost:3000/health/ready
```

## API principal

| Endpoint | Uso |
|---|---|
| `POST /wallets` | Cria wallet; saldo inicial positivo gera abertura no ledger |
| `GET /wallets/:id` | Consulta saldo e versão |
| `GET /wallets/:id/ledger?limit=50&cursor=...` | Consulta ledger com cursor estável |
| `POST /wallets/:id/reconciliation` | Compara saldo armazenado ao ledger, sem reparo silencioso |
| `POST /wagering/transactions` | Processa operação com `Idempotency-Key` obrigatório |
| `GET /wagering/transactions/:id` | Consulta transação |
| `GET /providers/:providerId/wagering/transactions/:externalId` | Consulta por referência do provedor |
| `GET /health/live` | Liveness |
| `GET /health/ready` | Readiness de PostgreSQL, SQS e DLQ |
| `GET /health/metrics` | Métricas locais |

O corpo de uma transação usa valores monetários como strings decimais:

```json
{
  "providerId": "provider-a",
  "externalTransactionId": "transaction-123",
  "playerId": "player-123",
  "walletId": "wallet-uuid",
  "roundId": "round-987",
  "gameId": "fortune-chimp",
  "kind": "BET",
  "money": { "amount": "25.00", "currency": "BRL" }
}
```

Envie `Idempotency-Key: provider-a:transaction-123`. Opcionalmente, envie `X-Correlation-Id` para propagar a correlação aos eventos da Outbox.

| HTTP | Significado |
|---:|---|
| `200` | Operação processada ou replay |
| `202` | Aceita e aguardando referência |
| `400` | Payload inválido ou `Idempotency-Key` ausente |
| `404` | Wallet/transação inexistente |
| `409` | Chave idempotente conflitante ou conflito estrutural de persistência |
| `422` | Rejeição de regra de negócio, com `failureCode` |
| `503` | Falha transitória de infraestrutura; pode reenviar a mesma chave |

Uma reversão duplicada rejeitada pela regra de negócio retorna `422` com `failureCode: "DUPLICATE_REVERSAL"`.

## Validação

Validações locais:

```powershell
bun run typecheck
bun run build
bun test
```

A suíte que usa PostgreSQL e LocalStack reais requer ambos ativos, migrations aplicadas e filas criadas. Mantenha a API na porta 3000 para os testes de integração SQS e rode:

```powershell
$env:RUN_INTEGRATION = "1"
bun test
```

A suíte de concorrência também inicia múltiplas instâncias locais. Para exercitar o fault injection opcional de crash depois do commit e antes do ack, inicie a API com `SQS_TEST_FAIL_AFTER_COMMIT_BEFORE_ACK=1` e `SQS_VISIBILITY_TIMEOUT_SECONDS=2`, então execute:

```powershell
$env:RUN_INTEGRATION = "1"
$env:EXPECT_ACK_FAULT = "1"
bun test test/integration
```

Resultados mais recentes: typecheck/build concluídos; suite completa com PostgreSQL 16 e LocalStack 3.8: 26 testes aprovados, 1 teste opcional ignorado e nenhuma falha.

## Operação e arquitetura

Para parar os containers sem apagar os dados:

```powershell
docker compose down
```

`docker compose down -v` apaga os volumes locais; use apenas se realmente quiser resetar os dados de desenvolvimento.

O serviço atende `SIGTERM` e `SIGINT` com shutdown gracioso. Wallet, transação, ledger, Inbox e Outbox são persistidos atomicamente; o publisher publica os envelopes tipados após commit. Consulte [ARCHITECTURE.md](./ARCHITECTURE.md) para locks, idempotência, contratos de erro, eventos, métricas e trade-offs; veja [RETRY_DLQ_ARCHITECTURE.md](./RETRY_DLQ_ARCHITECTURE.md) para retries e DLQ.

Limitações conhecidas: métricas são mantidas em memória e não há autenticação externa. Não registre valores ou payloads financeiros em logs.
