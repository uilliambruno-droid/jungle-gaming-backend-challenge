# Plano de implementação e validação

Registro de fechamento dos pontos do challenge. Itens marcados como concluídos foram implementados e cobertos pela suíte indicada na seção final.

## 1. Correção financeira e idempotência — concluído

- [x] Reconciliação calcula `storedBalance - calculatedBalance` com `Money`, em uma única leitura SQL consistente; informa divergência sem corrigir saldo.
- [x] Salvar status, saldo, moeda e falha da resposta original para replay mesmo após mudanças posteriores na wallet.
- [x] Associar cada Inbox processada à transação para impedir efeitos duplicados mesmo em redelivery com metadados de transporte diferentes.
- [x] Serializar reversões pela wallet e referência; verificar duplicidade no caso de uso HTTP e no worker; manter unique partial index como proteção do PostgreSQL.
- [x] Substituir retry recursivo de `23505` por tratamento explícito e limitado: replay idempotente uma vez ou conflito terminal.

## 2. Concorrência e contrato HTTP — concluído

- [x] Testar três instâncias simultâneas processando wallets distintas.
- [x] Testar duas reversões `PENDING_REFERENCE` concorrentes para uma referência; só uma pode ser aplicada.
- [x] Testar disputa por saldo, 50 redeliveries e reconstrução do saldo pelo ledger.
- [x] Responder `200` para processamento/replay, `202` para referência pendente, `422` para rejeição de negócio, `409` para conflito, `400` para entrada inválida e `503` para indisponibilidade transitória.

## 3. Ciclo de vida e eventos — concluído

- [x] Ativar hooks de shutdown NestJS para `SIGTERM` e `SIGINT`; aguardar operações em andamento e não iniciar novos receives após shutdown.
- [x] Usar subclasses tipadas para `WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference` e `WalletBalanceChanged`.
- [x] Persistir o envelope completo e versionado na Outbox, propagar correlation/causation e publicar o mesmo envelope após commit.
- [x] Manter `eventId` como chave de deduplicação na publicação SQS.

## 4. Observabilidade — concluído

- [x] Expor contadores por status, duplicatas, retries, conflitos de lock, DLQ e Outbox pendente/atrasada.
- [x] Registrar contagem, soma e máximo de latência de submissão e worker.
- [x] Propagar correlation/message/transaction/wallet/provider ids nos logs operacionais.
- [x] Não serializar payload nem valores financeiros em logs.

Limitação conhecida: as métricas são em memória para desenvolvimento local; em produção devem ser exportadas para Prometheus/OpenTelemetry. A contagem da SQS é aproximada.

## 5. Domínio e persistência — concluído

- [x] Aplicar movimentos por `Wallet`, `WagerTransaction` e `WalletLedgerEntry`, preservando aritmética exata.
- [x] Reforçar no PostgreSQL saldo não negativo, ledger balanceado/imutável, unicidade de wallet/transação/idempotência e reversão única.
- [x] Adicionar migration reversível para snapshots de resposta, correlação e unicidade das reversões.
- [x] Validar a reconciliação consistente, Inbox/Outbox atômicas e o worker com PostgreSQL/LocalStack reais.

## 6. Documentação e validação final

- [x] Criar README em português com setup, endpoints, status HTTP, comandos e limitações.
- [x] Expandir `ARCHITECTURE.md` com invariantes, locks, eventos, contratos, observabilidade e trade-offs.
- [x] Atualizar este plano após execução final.
- [x] Executar typecheck, build, testes locais e suíte de integração real.

### Execução registrada

Ambiente: Bun 1.4.4, PostgreSQL 16 e LocalStack 3.8. Migration 003 aplicada sem reset dos volumes; filas da aplicação verificadas. A suíte de integração executa contra serviços reais e inicia múltiplas instâncias do backend.

Cobertura: 50 redeliveries em três workers, disputa de saldo em duas instâncias, três instâncias e wallets distintas, reversões pendentes concorrentes, replay do saldo original, Outbox concorrente, DLQ, reinício após commit e reconciliação com o ledger.

Resultado histórico anterior: typecheck/build sem erros; suíte completa com 26 testes aprovados e 1 teste opcional de fault injection ignorado. Este resultado pertence à revisão anterior e não é a validação atual.

## 7. Revisão adicional e correções — concluído

### Prioridade alta

- [x] Fronteira monetária: adaptadores de dinheiro recebem apenas strings decimais; domínio continua usando `bigint`. Conversões numéricas restantes são de configuração, paginação ou métricas, nunca de dinheiro.
- [x] Ordem dos locks: API e worker agora bloqueiam wallet antes de transação de referência/pendente. O worker escolhe um candidato sem lock, bloqueia a wallet e então relê/valida o candidato sob `FOR UPDATE`.
- [x] Replay pendente: contrato definido e testado. Uma repetição retorna a resposta inicial armazenada (`202`, `PENDING_REFERENCE` e saldo original), mesmo após processamento assíncrono; `GET /wagering/transactions/:id` informa o estado atual.
- [x] Autenticação futura: documentado OIDC e criado `ProviderIdentityPort` como extensão no limite HTTP. Autenticação não é ativada neste challenge nem vale pontos; não confiar em `providerId` não autenticado em produção.

### Prioridade média

- [x] Consistência no banco: migration 004 adiciona constraint triggers diferidas para exigir saldo da wallet igual à soma assinada do ledger e moeda consistente no commit. A suíte comprova rejeição de divergência. O teste de reconciliação injeta corrupção com `session_replication_role=replica` para simular legado/falha fora do fluxo normal.
- [x] Outbox: documentada publicação at-least-once, janela crash entre publish e marcar como enviado, deduplicação FIFO limitada à janela do SQS e necessidade de deduplicação persistente por `eventId` no consumidor.
- [x] Datas do domínio: getters e snapshots agora devolvem cópias defensivas; testes verificam que mutações externas não alteram estado interno.
- [x] Clone limpo: dependências foram instaladas com `bun install --frozen-lockfile` em checkout temporário isolado; migrations/setup SQS, typecheck, build e suíte completa foram executados com PostgreSQL 16 e LocalStack 3.8.

### Resultado de validação desta revisão

Validação histórica da revisão anterior: `bun run typecheck`, `bun run build` e `RUN_INTEGRATION=1 bun test` passaram no checkout temporário. Resultado: **30 testes aprovados, 1 teste opcional ignorado, 0 falhas**. O teste ignorado valida fault injection commit-before-ack; o resultado histórico não afirma que essa falha foi injetada com sucesso.

### Reavaliação estimada pela seção 14

Estimativa técnica, não nota oficial:

| Critério | Pontos | Evidências e ressalvas |
|---|---:|---|
| Correção financeira | 20/20 | `bigint`, ledger, reversões, replay e trigger de consistência no banco; integração real e reconciliação assinada. |
| Concorrência | 18/20 | múltiplas instâncias, wallets distintas, hot wallet e disputa HTTP/worker testadas; ainda há custo/espera em wallet muito concorrida. |
| Idempotência | 15/15 | unicidade persistente, hash de payload, snapshots originais e replay pendente testados. |
| Mensageria e falhas | 14/15 | Inbox/Outbox, retries, DLQ, restart e shutdown; fault injection específico permanece opt-in. |
| Modelagem e arquitetura | 9/10 | domínio encapsulado, boundaries e port de identidade; autenticação externa é uma extensão documentada, não implementada. |
| Testes | 9/10 | PostgreSQL/SQS reais, race tests e checkout limpo; 1 cenário de fault injection foi ignorado nesta execução. |
| Observabilidade | 4/5 | logs, métricas e health presentes; métricas em memória, sem percentis, e DLQ aproximada. |
| Documentação | 5/5 | setup, contrato, locks, replay, OIDC futuro, Outbox, limitações e plano registrados. |
| **Total estimado** | **94/100** | **Sem falha eliminatória identificada nesta revisão.** |

## 8. Alinhamento documental e validação final

### Correções documentais

- [x] `ProviderIdentityPort` existe como interface, mas não possui implementação nem integração nos controllers; documentado sem sugerir que autenticação esteja ativa.
- [x] Documentada a sequência real do worker: selecionar candidato sem lock, bloquear wallet, reler/bloquear a transação pendente e revalidar status/vencimento antes de processar.
- [x] Marcados explicitamente como históricos os resultados anteriores de 26 e 30 testes para não confundi-los com a validação atual.
- [x] Documentada a execução isolada da fault injection em dois terminais; usar filtro de nome evita que a falha única atinja outra mensagem primeiro.

### Validação da revisão documental anterior — 01/11/2026

Ambiente local: Bun 1.4.4, Docker Compose com PostgreSQL 16 e LocalStack 3.8 saudáveis.

| Comando | Resultado |
|---|---|
| `bun run db:migrate` | Sucesso; schema já estava na migration mais recente. |
| `bun run sqs:setup` | Sucesso; três filas esperadas resolvidas no LocalStack. |
| `bun run typecheck` | Sucesso. |
| `bun run build` | Sucesso. |
| `bun test` | Sucesso: 11 testes passaram, 22 testes de integração foram ignorados porque `RUN_INTEGRATION` não estava habilitado. |
| `$env:RUN_INTEGRATION="1"; bun test` | Histórico da revisão documental: 30 passaram, 1 teste opcional de fault injection foi ignorado, 0 falharam. |
| Teste filtrado commit-before-ack com `EXPECT_ACK_FAULT=1` e API iniciada com `SQS_TEST_FAIL_AFTER_COMMIT_BEFORE_ACK=1`, `SQS_VISIBILITY_TIMEOUT_SECONDS=2` | Histórico da revisão documental: 1 passou, 8 filtrados, 0 falharam. |

O teste opcional foi executado isoladamente depois da suíte completa, reiniciando a API com a fault injection habilitada, para garantir que a falha de uso único atingisse a mensagem do cenário. Instruções reproduzíveis em `RETRY_DLQ_ARCHITECTURE.md`.

## 9. WIN referenciado e cobertura das operações — concluído

- [x] `WIN` continua aceito sem referência; quando `referenceExternalTransactionId` é informado, deve resolver para uma `BET` processada da mesma provider/player/wallet/moeda/rodada. O valor do `WIN` pode diferir do valor da aposta.
- [x] Referência de `WIN` ainda ausente é persistida em `PENDING_REFERENCE`; o worker valida e processa o caso depois que a `BET` chega. Referências inválidas são rejeitadas com `INVALID_REFERENCE`.
- [x] Testes de integração cobrem `WIN` referenciado direto e pendente, rejeição de referência a `LOSS`, `LOSS` sem lançamento de ledger e `ROLLBACK` processado de um `WIN`.
- [x] Teste unitário cobre a regra de `LOSS` sem efeito no saldo e a direção de débito ao reverter um `WIN`.
- [x] A abertura positiva da wallet gera `WagerTransactionProcessed` e `WalletBalanceChanged` atomicamente na Outbox; integração valida o envelope e os valores do crédito inicial.
- [x] Documentada a distinção do consumidor entre rejeição/conflito reconhecidos e envelope inválido que segue para DLQ; README explicita que o replay preserva status HTTP e saldo originais.

Validação anterior a esta correção: `bun run typecheck`, `bun run build`, 12 testes unitários, suíte PostgreSQL/LocalStack com 33 aprovados e 1 teste opcional ignorado; fault injection isolada commit-before-ack passou (1 teste). Nenhuma falha.

### Validação final após alinhamento de abertura e DLQ — 01/11/2026

| Comando | Resultado |
|---|---|
| `bun run typecheck` | Passou. |
| `bun run build` | Passou. |
| `bun test` | 12 passaram, 24 integrações ignoradas por ausência de `RUN_INTEGRATION`, 0 falhas. |
| `$env:RUN_INTEGRATION="1"; bun test` | 33 passaram, 1 teste opcional de fault injection ignorado, 0 falhas; PostgreSQL 16 e LocalStack 3.8 ativos. |
| Fault injection isolada com `EXPECT_ACK_FAULT=1`, `SQS_TEST_FAIL_AFTER_COMMIT_BEFORE_ACK=1` e `SQS_VISIBILITY_TIMEOUT_SECONDS=2` | 1 passou, 8 filtrados, 0 falhas. |
| `git diff --check` | Sem erros de whitespace. |

A integração também confirmou os envelopes `WagerTransactionProcessed` e `WalletBalanceChanged` da abertura positiva na Outbox. A migration mais recente e as filas SQS já estavam aplicadas/verificadas no ambiente local.

## 10. Fechamento para entrega — 01/11/2026

- [x] Adicionado ao `ARCHITECTURE.md` um diagrama Mermaid cobrindo entrada HTTP/SQS, idempotência, locks e domínio, persistência atômica, referências pendentes, Outbox, redelivery e DLQ.
- [x] Reavaliados os critérios do tópico 14 do challenge: estimativa técnica de **94/100**, sem falha eliminatória identificada. A nota não é oficial; os descontos considerados são contenção de hot wallet, autenticação OIDC não implementada (opcional no challenge), métricas locais/aproximadas e variabilidade no startup de instâncias auxiliares durante testes sob carga.
- [x] Repetidos typecheck, build, testes unitários, migrations e configuração SQS; todos passaram.
- [x] Suíte completa final com PostgreSQL 16 e LocalStack 3.8: **33 passaram, 1 teste opcional de fault injection ignorado e 0 falhas**.
- [x] Teste de fault injection commit-before-ack executado isoladamente com as variáveis documentadas: **1 passou, 8 filtrados e 0 falhas**.
- [x] Uma tentativa anterior da suíte completa teve falhas de readiness em três processos auxiliares após execução prolongada; os três testes passaram isoladamente, e a execução completa subsequente terminou sem falhas. Registrar essa ocorrência como instabilidade transitória do ambiente, não ocultá-la.
- [x] `.env` confirmado fora do Git e `git diff --check` sem erros.

Comandos finais executados: `bun run typecheck`, `bun run build`, `bun test`, `bun run db:migrate`, `bun run sqs:setup`, `$env:RUN_INTEGRATION="1"; bun test`, o teste isolado descrito em `RETRY_DLQ_ARCHITECTURE.md` e `git diff --check`.
