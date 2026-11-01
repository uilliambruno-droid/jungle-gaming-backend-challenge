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

Resultado final: typecheck/build sem erros; suíte completa com 26 testes aprovados, 1 teste opcional de fault injection ignorado (requer `EXPECT_ACK_FAULT=1`) e nenhuma falha.
