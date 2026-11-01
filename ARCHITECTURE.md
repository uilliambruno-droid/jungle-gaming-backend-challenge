# Decisões de arquitetura

## Limites e fluxo

O serviço usa Bun, TypeScript strict, NestJS, MikroORM, PostgreSQL e SQS via LocalStack. Controllers e workers chamam o mesmo caso de uso (`WagerService.submit`); as classes de domínio (`Money`, `Wallet`, `WagerTransaction` e `WalletLedgerEntry`) validam representação monetária e transições antes da persistência. O domínio não depende de NestJS, MikroORM ou SQS.

As escritas financeiras, Inbox e Outbox passam pelo `EntityManager` recebido em `EntityManager.transactional()` (`tm.execute`). Usar uma conexão fora do contexto transacional pode quebrar atomicidade, locks e consistência.

## Dinheiro, wallet e ledger

`Money` mantém centavos em `bigint`; contratos e colunas PostgreSQL usam decimal com escala 2 (`numeric(30,2)`). Não há conversões monetárias para `number`. `Wallet.debit`/`credit` constroem `WalletLedgerEntry` balanceado, atualizam a versão e impedem saldo negativo.

`SELECT ... FOR UPDATE` na wallet é o ponto de serialização por `walletId`. O fluxo HTTP bloqueia a wallet antes da transação de referência; o worker de referência bloqueia a transação pendente antes da wallet. Essa ordem não é idêntica: uma operação concorrente que referencie a transação pendente pode provocar deadlock. O PostgreSQL aborta uma das transações, o conflito é contabilizado e a chamada HTTP recebe `503`, podendo ser reenviada com a mesma chave idempotente. É uma oportunidade de melhoria antes de operar com carga alta. A consulta de reversões ocorre sob o lock da wallet e uma unique partial index em `(reference_transaction_id, kind)` para reversões processadas é a última proteção do banco.

Uma reversão duplicada observada pelo fluxo normal é uma rejeição auditável (`422`, `DUPLICATE_REVERSAL`); `409` fica reservado para conflito de idempotência ou colisão estrutural inesperada.

Constraints PostgreSQL cobrem wallet não negativa e única por player/moeda, transação externa e chave idempotente únicas, ledger balanceado/imutável e no máximo uma reversão processada por tipo e referência. Migrations têm `up` e `down`; a migration 003 registra o snapshot de resposta e contexto de correlação.

Reconciliação lê saldo e ledger em uma única instrução SQL (um único snapshot MVCC), calcula `stored - calculated` usando `Money` e não corrige divergências. O endpoint sinaliza `consistent`, diferença e quantidade de entradas; divergências incrementam métrica.

## Idempotência e respostas

O `Idempotency-Key` HTTP é obrigatório. O SHA-256 de JSON canônico usa apenas campos de negócio, sem chave de idempotência nem metadados de transporte. A transação persiste status, saldo, moeda e código de falha originais da resposta; replay não consulta o saldo atual da wallet.

Inbox é única por `(consumer_name, message_id)`, compara o hash e guarda a transação associada. Inbox, transação, saldo, ledger e Outbox são confirmados juntos. O consumer apaga a mensagem somente após o commit; a redelivery depois de commit-before-ack retorna a transação associada sem repetir efeito. Hash divergente é conflito auditável.

O tratamento de `23505` é explícito e limitado: colisão de chave idempotente consulta a transação já confirmada uma vez e retorna replay ou conflito; outros índices únicos viram conflito de persistência/reversão. Não há retry recursivo.

Status HTTP:

| Resultado | HTTP |
|---|---:|
| Transação processada/replay processado | `200` |
| Referência ainda ausente | `202` |
| Rejeição de regra de negócio | `422` |
| Conflito de idempotência ou conflito estrutural de persistência | `409` |
| Payload inválido ou chave ausente | `400` |
| Recurso inexistente | `404` |
| Falha transitória de infraestrutura | `503` |

## Inbox, SQS e Outbox

O consumidor reaproveita o caso de uso HTTP. Conflitos terminais são auditados e reconhecidos; mensagens malformadas/permanentes não são apagadas e seguem a redrive policy para DLQ; falhas transitórias ficam sem ack e voltam após expirar a visibilidade. Em `SIGTERM`, a aplicação para de receber, espera o processamento atual e aguarda os workers terminarem; mensagens recebidas que ainda não começaram ficam elegíveis para redelivery.

Cada evento de integração é uma classe concreta de `IntegrationEvent`, com `eventType`, versão e envelope persistido completo (`eventId`, `aggregateId`, `correlationId`, `causationId`, `occurredAt`, `version`, `data`). `correlationId`/`causationId` são propagados da requisição ou mensagem SQS. O evento não inclui instâncias de `Money`; valores financeiros são strings decimais.

Os eventos de transação, alteração de saldo e referência pendente são gravados atomicamente com as alterações financeiras. `OutboxPublisher` usa `FOR UPDATE SKIP LOCKED`, publica o envelope persistido após o commit e marca `published_at`; `eventId` também é o `MessageDeduplicationId`. A janela entre publicação e atualização SQL pode duplicar publicação, portanto o consumidor de eventos deve deduplicar por `eventId`.

Referências fora de ordem ficam em `PENDING_REFERENCE`, com tentativas e backoff persistidos. O worker trava wallet e referência, revalida todas as invariantes, aplica a operação usando classes de domínio e persiste status, ledger e eventos atomicamente. Uma reversão duplicada é rejeitada também no worker.

## Observabilidade e ciclo de vida

- `/health/live` verifica liveness; `/health/ready` verifica PostgreSQL, fila principal e DLQ.
- `/health/metrics` expõe contadores por status, duplicatas, retries, DLQ, conflitos de lock, latência e quantidade/atraso da Outbox.
- Logs JSON incluem correlation/message/transaction/wallet/provider ids nos fluxos de processamento; não registram payloads nem valores financeiros.
- `SIGTERM` e `SIGINT` acionam os hooks de shutdown do NestJS.

As métricas são em memória para este challenge local. Os nomes `transactions_*_total` contam transições/eventos de processamento, não um snapshot do número atual de linhas em cada status. A profundidade da DLQ só é atualizada durante readiness e é uma aproximação eventualmente consistente. Latência é exposta como contagem, soma e máximo; não há histogramas/percentis. Em produção, exportar para Prometheus/OpenTelemetry, usar histogramas e medir a DLQ por um coletor dedicado.

## Desenvolvimento e validação

Setup, endpoints e comandos reproduzíveis estão no [README.md](./README.md). O desenho de retries e DLQ está em [RETRY_DLQ_ARCHITECTURE.md](./RETRY_DLQ_ARCHITECTURE.md).

Validação final: Bun 1.4.4, PostgreSQL 16 e LocalStack 3.8. A suite completa com dependências reais aprovou 26 testes; 1 fault-injection opcional foi ignorado por depender de `EXPECT_ACK_FAULT=1`. Inclui 50 redeliveries, duas e três instâncias, wallets distintas, reversões pendentes concorrentes, replay com snapshot, restart, Outbox concorrente, DLQ e reconciliação ledger/saldo, inclusive diferença assinada.

## Trade-offs e limitações

- Lock pessimista no banco é simples e correto para operações financeiras; operações em uma wallet hot são serializadas, enquanto wallets distintas avançam em paralelo.
- A Outbox garante publicação eventual, não exactly-once na fronteira PostgreSQL/SQS. A deduplicação por `eventId` é responsabilidade do consumidor.
- As métricas em memória reiniciam com o processo; a profundidade da DLQ e a contagem SQS são aproximadas.
- O backfill de snapshots na migration 003 deriva respostas históricas do ledger disponível; para dados legados sem lançamento ou com timestamps ambíguos, o saldo exato da resposta original não pode ser recuperado. Operações novas persistem o snapshot exato no commit.
- Autenticação externa não foi implementada por não ser pontuada no challenge; os endpoints de health permanecem abertos.
- O teste de falha artificial após commit e antes do ack é opt-in com `EXPECT_ACK_FAULT=1`; o teste de reinício e redelivery da suíte normal não depende dele.
