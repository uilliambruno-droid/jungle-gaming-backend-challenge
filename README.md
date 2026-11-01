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

O replay sempre preserva a resposta da primeira submissão. Se a resposta inicial foi `202 PENDING_REFERENCE`, repeti-la continua retornando o mesmo `202` e o mesmo saldo original, ainda que o worker já tenha concluído a operação. Consulte `GET /wagering/transactions/:id` para conhecer o estado atual.

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

A suíte de concorrência também inicia múltiplas instâncias locais. Para exercitar o fault injection opcional de crash depois do commit e antes do ack, siga a execução isolada em dois terminais descrita em [RETRY_DLQ_ARCHITECTURE.md](./RETRY_DLQ_ARCHITECTURE.md). Rodar somente o teste específico garante que a falha única afete a mensagem correta.

Validação mais recente (01/11/2026): com Bun 1.4.4, PostgreSQL 16 e LocalStack 3.8 ativos, migrations/setup SQS, `bun run typecheck`, `bun run build`, `bun test` (11 aprovados; 22 integrações ignoradas por ausência de `RUN_INTEGRATION`) e `$env:RUN_INTEGRATION="1"; bun test` (30 aprovados, 1 fault injection ignorada, 0 falhas) passaram. A fault injection também foi executada separadamente e passou: `1 pass, 8 filtered out, 0 fail`. Procedimento exato em [RETRY_DLQ_ARCHITECTURE.md](./RETRY_DLQ_ARCHITECTURE.md).

## Operação e arquitetura

Para parar os containers sem apagar os dados:

```powershell
docker compose down
```

`docker compose down -v` apaga os volumes locais; use apenas se realmente quiser resetar os dados de desenvolvimento.

O serviço atende `SIGTERM` e `SIGINT` com shutdown gracioso. Wallet, transação, ledger, Inbox e Outbox são persistidos atomicamente; o publisher publica os envelopes tipados após commit. Consulte [ARCHITECTURE.md](./ARCHITECTURE.md) para locks, idempotência, contratos de erro, eventos, métricas e trade-offs; veja [RETRY_DLQ_ARCHITECTURE.md](./RETRY_DLQ_ARCHITECTURE.md) para retries e DLQ.

As migrations protegem a consistência wallet/ledger também no banco: uma constraint trigger diferida compara o saldo materializado à soma do ledger no commit. `ProviderIdentityPort` existe como interface de extensão para futura autenticação OIDC no limite HTTP; não tem implementação nem está conectada aos controllers, então autenticação não está ativa neste challenge. A proposta e os limites de confiança estão em [ARCHITECTURE.md](./ARCHITECTURE.md).

Limitações conhecidas: métricas são mantidas em memória e não há autenticação externa. Eventos da Outbox podem ser publicados mais de uma vez; consumidores devem deduplicar persistentemente por `eventId`. Não registre valores ou payloads financeiros em logs.
