# Plano de implementação do desafio

Este arquivo acompanha as pendências técnicas do desafio. Cada item deve ser fechado somente após implementação, testes e documentação correspondentes.

## 1. Inbox + SQS — concluído

- [x] Resolver a URL da fila pelo AWS SDK, sem assumir um account id fixo.
- [x] Receber `messageId` do envelope ou do atributo SQS e preservar o `consumerName`.
- [x] Registrar Inbox por `(consumer_name, message_id)` dentro da mesma transação da operação financeira.
- [x] Comparar o `payloadHash` em redelivery; payload diferente para o mesmo message id deve ser conflito auditável.
- [x] Marcar Inbox como processado somente quando o caso de uso terminar com sucesso de negócio.
- [x] Fazer delete/ack somente depois do commit SQL.
- [x] Deixar falhas de infraestrutura sem `processedAt`, permitindo redelivery.
- [x] Garantir shutdown gracioso e não iniciar novos receives após `SIGTERM`.
- [x] Executar teste real de redelivery, mensagem duplicada e conflito de payload.
- [x] Simular falha depois do commit e antes do ack, verificando redelivery sem novo lançamento.
- [x] Documentar a sequência normal, duplicada e de falha.

Critério de aceite: a mesma mensagem SQS não pode produzir dois efeitos financeiros; se o processo morrer depois do commit e antes do ack, a redelivery deve ser reconhecida pelo Inbox/idempotência sem novo lançamento.

Validação concluída com PostgreSQL 16 e LocalStack 3.8 em Docker: migrations aplicadas, filas criadas e testes de integração aprovados.

## 2. Publisher da Outbox — concluído

- [x] Criar worker com seleção concorrente usando `FOR UPDATE SKIP LOCKED` ou claim equivalente.
- [x] Publicar envelope versionado no SQS somente depois do commit financeiro.
- [x] Registrar tentativas, `next_attempt_at` e `published_at`.
- [x] Tornar duplicação após crash segura por `eventId`.
- [x] Testar dois publishers concorrentes e replay do mesmo `eventId` na fronteira publish/update.

## 3. Retries, DLQ e referências pendentes — concluído

- [x] Configurar redrive policy da fila principal para `wager-transactions-dlq.fifo`.
- [x] Classificar erros de negócio, transitórios e permanentes.
- [x] Implementar backoff e limite de tentativas.
- [x] Criar worker agendado para `PENDING_REFERENCE`.
- [x] Rejeitar referências expiradas com `REFERENCE_NOT_FOUND` e publicar evento.
- [x] Testar retry, DLQ, redelivery e reinício.

## 4. Endpoint de ledger — concluído

- [x] Consultar entradas reais do PostgreSQL.
- [x] Implementar cursor opaco estável por `(created_at, id)`.
- [x] Validar limite e direção da paginação.
- [x] Garantir que o endpoint não altera dados.

## 5. Readiness e observabilidade — concluído para desenvolvimento local

- [x] Fazer `/health/ready` verificar PostgreSQL e SQS.
- [x] Adicionar logs JSON com correlation/message/transaction/wallet/provider ids.
- [x] Adicionar métricas básicas de status, duplicatas, retries, DLQ, referências e outbox pendente.
- [x] Evitar payload financeiro completo em logs.

Limitação: as métricas são mantidas em memória para desenvolvimento local; produção deve usar Prometheus/OpenTelemetry ou equivalente.

## 6. Testes reais — pendente

- [ ] Criar suíte de integração com PostgreSQL e LocalStack em Docker.
- [ ] Testar migrations, constraints, atomicidade e Inbox/Outbox.
- [ ] Testar 50 mensagens idênticas em paralelo.
- [ ] Testar duas apostas concorrentes e múltiplas instâncias.
- [ ] Testar crash após commit, publishers concorrentes e reinicialização.
- [ ] Verificar sempre `wallet.balance == saldo reconstruído pelo ledger`.

Observação: já existe uma suíte de integração real cobrindo Inbox, Outbox, publishers concorrentes, DLQ, referências pendentes e ledger. Permanecem pendentes os cenários ampliados de concorrência financeira e validação limpa final.

## 7. Validação final — pendente

- [ ] Executar Bun install, typecheck, build e testes.
- [ ] Subir PostgreSQL e LocalStack com Compose.
- [ ] Aplicar migrations e criar filas.
- [ ] Executar testes de integração e concorrência.
- [ ] Atualizar README e ARCHITECTURE com comandos, resultados e limitações.

---

## Lembrete da sessão

Data: 31/10/2026  
Horário local: 21:59:29 (+04:00)  

O projeto está com o núcleo financeiro, mensageria, retries, DLQ, ledger, readiness e observabilidade local implementados. Na próxima sessão, continuar pelos testes ampliados de concorrência, validação do saldo contra o ledger e fechamento da entrega.

## Plano da próxima sessão — domingo, 01/11/2026

### Objetivo do dia

Fechar os riscos que ainda podem afetar a avaliação: concorrência financeira, consistência entre saldo e ledger, execução em ambiente limpo e validação final da documentação. Isso será feito porque os componentes funcionais principais já estão implementados; agora precisamos provar o comportamento sob carga concorrente e garantir que o projeto possa ser entregue de forma reproduzível.

### Ordem de execução

1. **Preparar ambiente limpo**
   - executar `bun install`;
   - executar `docker compose up -d postgres localstack`;
   - executar `bun run db:migrate`;
   - executar `bun run sqs:setup`;
   - confirmar `docker compose ps` com PostgreSQL e LocalStack saudáveis.

   Motivo: eliminar dependência de estado residual e validar que outra pessoa consegue reproduzir o ambiente.

2. **Testar 50 mensagens idênticas em paralelo**
   - enviar 50 mensagens com o mesmo `messageId` e payload;
   - aguardar o consumidor;
   - verificar que existe somente um efeito financeiro;
   - conferir Inbox, wallet, ledger e Outbox.

   Critério de aceite: uma única alteração de saldo e um único lançamento financeiro.

3. **Testar apostas concorrentes na mesma wallet**
   - criar uma wallet com saldo conhecido;
   - disparar duas ou mais apostas simultaneamente;
   - verificar que não existe lost update ou saldo negativo;
   - conferir que cada efeito aprovado possui lançamento correspondente.

   Critério de aceite: `wallet.balance` deve ser igual ao saldo reconstruído a partir do ledger.

4. **Testar múltiplas instâncias**
   - iniciar duas instâncias da API/worker;
   - enviar mensagens para a mesma wallet;
   - verificar locks PostgreSQL, Inbox, Outbox e ausência de duplicidade.

   Motivo: confirmar que a correção depende do banco e não de locks em memória.

5. **Validar recuperação e reinicialização**
   - criar mensagens pendentes e registros de Outbox;
   - reiniciar uma instância durante o processamento;
   - confirmar que retries, `published_at`, `reference_attempts` e Inbox persistem;
   - verificar redelivery e DLQ quando aplicável.

6. **Executar validação final**
   - `bun run typecheck`;
   - `bun run build`;
   - `bun test`;
   - `RUN_INTEGRATION=1 bun test test/integration`;
   - revisar README, ARCHITECTURE e este plano;
   - registrar resultados, limitações e comandos de entrega.

### Resultado esperado

Ao final de domingo, o projeto deve ter evidência automatizada dos cenários concorrentes, validação limpa com PostgreSQL e LocalStack, documentação consistente e uma lista pequena de limitações conhecidas, sem pendências críticas de correção financeira.
