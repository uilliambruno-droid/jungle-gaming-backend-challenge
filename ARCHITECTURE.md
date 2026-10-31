# Decisões de arquitetura

## Visão geral

O sistema usa Bun, NestJS, TypeScript strict, PostgreSQL, MikroORM, Docker Compose e SQS via LocalStack. O domínio não conhece NestJS, MikroORM ou SQS.

## Dinheiro, wallet e ledger

`Money` usa `bigint` em centavos e serializa valores como strings com duas casas. Cada alteração de saldo bloqueia a wallet com `SELECT ... FOR UPDATE`, valida saldo e moeda, atualiza saldo/version e grava um lançamento imutável no ledger na mesma transação PostgreSQL.

O endpoint de ledger consulta o PostgreSQL usando cursor opaco e ordenação estável por `(created_at, id)`. É somente leitura.

## Inbox e idempotência

Mensagens SQS são registradas em `inbox_messages` por `(consumer_name, message_id)`. O hash do payload detecta conflitos. O ack só acontece depois do commit SQL. Falhas antes do commit são revertidas; falhas depois do commit permitem redelivery sem novo efeito financeiro.

Erros permanentes de envelope, conflito ou validação fazem ack e rejeição. Falhas de infraestrutura não fazem delete e permanecem elegíveis para redelivery.

## Outbox e publishers

Wallet, transação, ledger, Inbox e Outbox participam da mesma transação financeira. O `OutboxPublisher` usa `FOR UPDATE SKIP LOCKED`, permitindo múltiplas instâncias concorrentes.

Eventos são publicados na fila `wager-events.fifo` com envelope versionado. O ID persistido da Outbox é usado como `eventId` e `MessageDeduplicationId`. Falhas incrementam `attempts` e calculam `next_attempt_at` com backoff exponencial.

## Retries, DLQ e referências fora de ordem

`PENDING_REFERENCE` representa uma transação recebida antes da referência. O `PendingReferenceWorker` usa `FOR UPDATE SKIP LOCKED` e persiste `reference_attempts` e `reference_next_attempt_at`.

Quando a referência aparece, o worker valida provider, player, wallet, rodada, moeda, valor e tipo. Wallet, ledger, status e eventos da Outbox são atualizados atomicamente. Após o limite, a transação é rejeitada com `REFERENCE_NOT_FOUND`.

A fila `wager-transactions.fifo` possui redrive policy para `wager-transactions-dlq.fifo`, configurada por `bun run sqs:setup`.

## Readiness e observabilidade

- `/health/live` verifica se o processo está ativo.
- `/health/ready` verifica PostgreSQL e SQS e retorna `503` quando uma dependência falha.
- `/health/metrics` expõe contadores operacionais.

Os logs são JSON estruturados e não registram payloads financeiros completos. As métricas são mantidas em memória para desenvolvimento local; produção deve usar Prometheus, OpenTelemetry ou equivalente.

## Desenvolvimento local

Use `docker compose up -d postgres localstack`, `bun run db:migrate`, `bun run sqs:setup` e `bun run dev`. Consulte [RETRY_DLQ_ARCHITECTURE.md](./RETRY_DLQ_ARCHITECTURE.md) para detalhes de retry e DLQ.

## Limitações antes da entrega

- Ainda falta executar a suíte final com 50 mensagens idênticas e duas apostas concorrentes na mesma wallet.
- A validação final deve ser repetida a partir de ambiente limpo.
- As métricas não possuem armazenamento externo.
- Autenticação externa não foi implementada porque não é pontuada no desafio.
