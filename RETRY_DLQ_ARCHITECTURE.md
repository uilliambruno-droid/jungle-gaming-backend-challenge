# Retries, DLQ e referências fora de ordem

## Referências pendentes

O `PendingReferenceWorker` adquire locks nesta ordem, dentro da transação:

1. Seleciona um candidato pendente sem bloquear nem manter lock sobre a transação.
2. Adquire `FOR UPDATE` na wallet candidata.
3. Relê a transação pendente por ID com `FOR UPDATE`, confirmando que ela ainda está pendente e que o retry continua vencido.
4. Bloqueia a referência e valida provider, player, wallet, rodada, moeda, valor e tipo.

Se outro worker alterar o candidato antes do terceiro passo, a validação de status/vencimento faz esta execução não processá-lo. Wallet, ledger, status e outbox são atualizados na mesma transação. A ordem wallet → transação de referência é compartilhada com o fluxo HTTP.

Enquanto a referência não aparece, `reference_attempts` e `reference_next_attempt_at` persistem o backoff. Após `PENDING_REFERENCE_MAX_ATTEMPTS`, a transação é rejeitada com `REFERENCE_NOT_FOUND` e um evento de rejeição é gravado na outbox.

## Classificação de falhas

- Conflitos (`409`) e rejeições de negócio (`422`): o consumidor reconhece a mensagem e registra o conflito; não há retry.
- Envelope malformado ou payload que falha validação (`400`): o consumidor não apaga a mensagem; ela volta após expirar a visibilidade e segue a redrive policy até a DLQ.
- Falhas de infraestrutura: o consumidor não faz delete; a visibilidade expira e o SQS redelivera.
- Depois de `SQS_MAX_RECEIVE_COUNT`, a fila principal encaminha a mensagem para `wager-transactions-dlq.fifo`.

## Configuração local

`bun run sqs:setup` cria as filas FIFO e aplica a redrive policy. O worker e o consumidor usam PostgreSQL para persistir tentativas, então reinícios não perdem o estado do retry.

## Validar commit antes do ack (fault injection)

Este teste é opcional e precisa ser executado isoladamente para garantir que a fault injection de uso único atinja a mensagem enviada pelo próprio teste. As dependências, migrations e filas devem estar prontas (`docker compose up -d postgres localstack`, `bun run db:migrate`, `bun run sqs:setup`).

No PowerShell, Terminal 1, habilite a falha no consumidor antes de iniciar a API:

```powershell
$env:SQS_TEST_FAIL_AFTER_COMMIT_BEFORE_ACK = "1"
$env:SQS_VISIBILITY_TIMEOUT_SECONDS = "2"
bun run dev
```

Em Terminal 2, rode somente o teste de recuperação:

```powershell
$env:RUN_INTEGRATION = "1"
$env:EXPECT_ACK_FAULT = "1"
bun test test/integration/inbox-sqs.test.ts --test-name-pattern "recovers when the worker fails after commit and before ack"
```

O consumidor confirma a transação no PostgreSQL, falha uma única vez antes de apagar a mensagem e a deixa visível para redelivery. A segunda entrega deve usar Inbox/idempotência e resultar em somente um efeito financeiro. O teste só é considerado validado se o comando acima executar e passar; a suíte completa padrão não ativa esta fault injection.
