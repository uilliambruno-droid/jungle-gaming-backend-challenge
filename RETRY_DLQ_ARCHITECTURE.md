# Retries, DLQ e referências fora de ordem

## Referências pendentes

O `PendingReferenceWorker` seleciona transações `PENDING_REFERENCE` com `FOR UPDATE SKIP LOCKED`. A busca valida provider, player, wallet, rodada, moeda, valor e tipo da referência. Quando a referência aparece, wallet, ledger, status e outbox são atualizados na mesma transação.

Enquanto a referência não aparece, `reference_attempts` e `reference_next_attempt_at` persistem o backoff. Após `PENDING_REFERENCE_MAX_ATTEMPTS`, a transação é rejeitada com `REFERENCE_NOT_FOUND` e um evento de rejeição é gravado na outbox.

## Classificação de falhas

- Erros permanentes de validação, conflito e envelope inválido: o consumidor reconhece a mensagem e registra rejeição.
- Falhas de infraestrutura: o consumidor não faz delete; a visibilidade expira e o SQS redelivera.
- Depois de `SQS_MAX_RECEIVE_COUNT`, a fila principal encaminha a mensagem para `wager-transactions-dlq.fifo`.

## Configuração local

`bun run sqs:setup` cria as filas FIFO e aplica a redrive policy. O worker e o consumidor usam PostgreSQL para persistir tentativas, então reinícios não perdem o estado do retry.
