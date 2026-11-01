# Decisões de arquitetura

## Limites e fluxo

O serviço usa Bun, TypeScript strict, NestJS, MikroORM, PostgreSQL e SQS via LocalStack. Controllers e workers chamam o mesmo caso de uso (`WagerService.submit`); as classes de domínio (`Money`, `Wallet`, `WagerTransaction` e `WalletLedgerEntry`) validam representação monetária e transições antes da persistência. O domínio não depende de NestJS, MikroORM ou SQS.

As escritas financeiras, Inbox e Outbox passam pelo `EntityManager` recebido em `EntityManager.transactional()` (`tm.execute`). Usar uma conexão fora do contexto transacional pode quebrar atomicidade, locks e consistência.

## Dinheiro, wallet e ledger

`Money` mantém centavos em `bigint`; contratos e colunas PostgreSQL usam decimal com escala 2 (`numeric(30,2)`). Não há conversões monetárias para `number`. `Wallet.debit`/`credit` constroem `WalletLedgerEntry` balanceado, atualizam a versão e impedem saldo negativo.

`SELECT ... FOR UPDATE` na wallet é o ponto de serialização por `walletId`. HTTP e worker agora seguem a ordem comum: primeiro lock da wallet, depois lock da transação de referência/pendente. O worker apenas seleciona um candidato sem lock, bloqueia sua wallet e então relê e bloqueia a transação pendente, validando novamente status e vencimento do retry. Assim, candidatos concorrentes podem esperar na wallet sem manter um lock de transação que inverta a ordem. A consulta de reversões ocorre sob o lock da wallet e uma unique partial index em `(reference_transaction_id, kind)` para reversões processadas é a última proteção do banco.

Uma reversão duplicada observada pelo fluxo normal é uma rejeição auditável (`422`, `DUPLICATE_REVERSAL`); `409` fica reservado para conflito de idempotência ou colisão estrutural inesperada.

Constraints PostgreSQL cobrem wallet não negativa e única por player/moeda, transação externa e chave idempotente únicas, ledger balanceado/imutável e no máximo uma reversão processada por tipo e referência. Uma constraint trigger diferida valida no commit que o saldo materializado da wallet é igual à soma assinada de seu ledger e que as moedas coincidem. Essa regra é diferida porque wallet e lançamento precisam ser gravados na mesma transação; a trigger verifica o estado final, não os estados intermediários. Migrations têm `up` e `down`; a migration 003 registra o snapshot de resposta e contexto de correlação, e a 004 adiciona a validação cruzada wallet/ledger.

Reconciliação lê saldo e ledger em uma única instrução SQL (um único snapshot MVCC), calcula `stored - calculated` usando `Money` e não corrige divergências. O endpoint sinaliza `consistent`, diferença e quantidade de entradas; divergências incrementam métrica.

## Idempotência e respostas

O `Idempotency-Key` HTTP é obrigatório. O SHA-256 de JSON canônico usa apenas campos de negócio, sem chave de idempotência nem metadados de transporte. A transação persiste status, saldo, moeda e código de falha originais da resposta; replay não consulta o saldo atual da wallet. Para operações que começam em `PENDING_REFERENCE`, isso significa que o replay sempre reproduz a resposta inicial (`202` e status pendente), mesmo se o worker posteriormente processar ou rejeitar a transação. O estado atual continua disponível em `GET /wagering/transactions/:id`; replay não é uma consulta de status atualizado.

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

Os eventos de transação, alteração de saldo e referência pendente são gravados atomicamente com as alterações financeiras. `OutboxPublisher` usa `FOR UPDATE SKIP LOCKED`, publica o envelope persistido após o commit e marca `published_at`; `eventId` também é o `MessageDeduplicationId`. A janela entre publicação e atualização SQL pode duplicar publicação. SQS FIFO reduz duplicatas dentro de sua janela de deduplicação, mas não elimina a possibilidade de uma nova publicação após crash ou após essa janela; consumidores de eventos devem manter deduplicação persistente por `eventId` e tornar o handler idempotente.

Referências fora de ordem ficam em `PENDING_REFERENCE`, com tentativas e backoff persistidos. O worker trava wallet e referência, revalida todas as invariantes, aplica a operação usando classes de domínio e persiste status, ledger e eventos atomicamente. Uma reversão duplicada é rejeitada também no worker.

## Observabilidade e ciclo de vida

- `/health/live` verifica liveness; `/health/ready` verifica PostgreSQL, fila principal e DLQ.
- `/health/metrics` expõe contadores por status, duplicatas, retries, DLQ, conflitos de lock, latência e quantidade/atraso da Outbox.
- Logs JSON incluem correlation/message/transaction/wallet/provider ids nos fluxos de processamento; não registram payloads nem valores financeiros.
- `SIGTERM` e `SIGINT` acionam os hooks de shutdown do NestJS.
- Datas expostas pelo domínio são cópias defensivas; snapshots de `WagerTransaction.toState()` também copiam `createdAt` e `processedAt` para evitar mutação externa do estado interno.

As métricas são em memória para este challenge local. Os nomes `transactions_*_total` contam transições/eventos de processamento, não um snapshot do número atual de linhas em cada status. A profundidade da DLQ só é atualizada durante readiness e é uma aproximação eventualmente consistente. Latência é exposta como contagem, soma e máximo; não há histogramas/percentis. Em produção, exportar para Prometheus/OpenTelemetry, usar histogramas e medir a DLQ por um coletor dedicado.

## Desenvolvimento e validação

Setup, endpoints e comandos reproduzíveis estão no [README.md](./README.md). O desenho de retries e DLQ está em [RETRY_DLQ_ARCHITECTURE.md](./RETRY_DLQ_ARCHITECTURE.md).

Validação desta revisão: Bun 1.4.4; typecheck, build e suíte completa passaram a partir de checkout temporário limpo, com PostgreSQL 16 e LocalStack 3.8 reais. Resultado: 30 testes aprovados, 1 teste opcional de fault injection ignorado (`EXPECT_ACK_FAULT=1`), 0 falhas. A cobertura inclui 50 redeliveries, duas e três instâncias, wallets distintas, reversões pendentes concorrentes, disputa HTTP/worker pela wallet, replay pendente após conclusão assíncrona, restart, Outbox concorrente, DLQ, constraints wallet/ledger e reconciliação com diferença assinada.

## Trade-offs e limitações

- Lock pessimista no banco é simples e correto para operações financeiras; operações em uma wallet hot são serializadas, enquanto wallets distintas avançam em paralelo. O worker seleciona primeiro um candidato sem lock, então usa a mesma ordem wallet → transação; não se mantém lock global entre wallets.
- A Outbox garante publicação eventual, não exactly-once na fronteira PostgreSQL/SQS. A deduplicação por `eventId` é responsabilidade do consumidor.
- As métricas em memória reiniciam com o processo; a profundidade da DLQ e a contagem SQS são aproximadas.
- O backfill de snapshots na migration 003 deriva respostas históricas do ledger disponível; para dados legados sem lançamento ou com timestamps ambíguos, o saldo exato da resposta original não pode ser recuperado. Operações novas persistem o snapshot exato no commit.
- Autenticação externa não foi implementada porque não vale pontos no challenge. `ProviderIdentityPort` marca a extensão no limite HTTP: uma implementação futura pode validar access tokens de um IdP OIDC (por exemplo Keycloak/Zitadel), mapear claims/roles a `providerId` e proteger os controllers de integração. O guard/adapter deve ficar desativado até configurado; não há autenticação artesanal nem se deve confiar em `providerId` fornecido livremente em produção. Health permanece aberto conforme o escopo do challenge, e a fila é considerada canal interno confiável.
- A integração de autenticação ainda não foi injetada nos controllers. Ao habilitá-la, valide assinatura, `issuer`, `audience`, expiração e scopes do token pelo IdP; derive o `providerId` de claims confiáveis em vez do corpo da requisição.
- O teste de falha artificial após commit e antes do ack é opt-in com `EXPECT_ACK_FAULT=1`; o teste de reinício e redelivery da suíte normal não depende dele.
