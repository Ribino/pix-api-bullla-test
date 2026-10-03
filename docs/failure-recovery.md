# Testes de Falha e Recuperação

Validação manual de resiliência. Nenhum código ou arquitetura foi alterado
para estes testes — só execução, observação de métricas e documentação.

Ambiente por cenário: volumes Docker frescos salvo nota em contrário,
`pix-api` + `pix-worker` + `pix-partner-mock` no ar, latência do parceiro
configurada por cenário via `/partner/admin/scenario`. `transactionId` único
por cenário (`tx-f1`…).

| Cenário | Resultado | Retry | DLQ | SUCCESS | Duplicação | Observação |
|---|---|---:|---:|---:|---:|---|
| Worker antes do processamento | OK | — | — | sim | não | redelivery após start; 1 efeito |
| Worker durante processamento | OK | — | — | sim | não | kill -9 mid-call; replay idempotente |
| Partner temporariamente indisponível | OK | local + async | não | sim | não | 2 ciclos async e recuperou |
| Partner permanentemente indisponível | OK | 12 tentativas | sim | n/a (PROCESSING) | não | sem loop; headers completos |
| Reinício API | OK | — | — | sim | não | health UP; 1 outbox row |
| Reinício Worker | OK | — | — | 3/3 | não | health UP; backlog drenado |
| Reprocessamento DLQ | OK | — | origem | sim | não | manual via console-producer |

## 1. Worker reinicia antes de processar

1. Procedimento: API + mock no ar, worker parado. `POST tx-f1` → 202.
2. Comportamento: outbox chegou a `PUBLISHED`, transação ficou `PROCESSING`,
   mock com 0 chamadas. Após subir o worker: `SUCCESS`, mock com exatamente
   1 request e 1 efeito (`processedTransactionIds: ["tx-f1"]`).
3. Métricas: lag `pix-processing` > 0 antes do start, zerado depois.
4. Resultado: OK. Conclusão: mensagem nunca perdida; redelivery funcionou;
   efeito único.

## 2. Worker reinicia durante processamento

1. Procedimento: mock com latência 2000ms. `POST tx-f2` → ao detectar a
   chamada em voo no mock (`requestCount==1`), `kill -9` no worker.
2. Comportamento: o mock concluiu e gravou o outcome (parceiro processou),
   mas o worker morreu antes do commit — transação ficou `PROCESSING`. Após
   restart: redelivery, replay idempotente no parceiro, `SUCCESS`.
3. Métricas: `processedCount` final = 1 (um único efeito financeiro); a
   segunda chamada HTTP retornou o resultado gravado sem reprocessar.
4. Resultado: OK. Conclusão: o par `transactionId` (idempotência no parceiro)
   + escrita condicional absorveu exatamente o cenário "crash após SUCCESS do
   parceiro". Sem transação distribuída, sem duplicata.

## 3. Partner temporariamente indisponível

1. Procedimento: mock em `ALWAYS_500`. `POST tx-f3` → 3 tentativas locais,
   roteamento async (ciclos com `asyncRetry=1` e `=2` visíveis nos logs),
   depois mock trocado para `SUCCESS`.
2. Comportamento: `SUCCESS` final; mock com 1 efeito gravado.
3. Métricas: `pix_partner_retries_total` subiu nos 500s; `pix_async_retries_total`
   nos roteamentos; `pix_processing_total{success}` no final.
4. Resultado: OK. Conclusão: retry local + assíncrono encadeiam corretamente e
   a recuperação converge sem intervenção.

## 4. Partner permanentemente indisponível

1. Procedimento: mock em `ALWAYS_500`. `POST tx-f4`.
2. Comportamento: 12 tentativas (3 locais × 4 ciclos: inicial + 3 async) e a
   mensagem chegou à `pix.dlq` com headers completos —
   `pix-retry-count:3, pix-failure-reason:RETRY_EXHAUSTED,
   pix-first-failure-at, pix-original-topic, traceparent` — key `tx-f4` e
   payload original preservados. Transação permanece `PROCESSING`.
   `requestCount` estabilizou em 12 (sem loop).
3. Métricas: `pix_dlq_messages_total{reason=retry_exhausted}` = 1.
4. Resultado: OK. Conclusão: quantidade correta de retries, DLQ preserva
   contexto para investigação/replay, sem hot loop.

## 5. Reinício da aplicação

### API

1. Procedimento: `POST tx-f5a` → `kill -9` na API → restart.
2. Comportamento: health `/actuator/health/liveness` voltou `UP`; outbox
   publicou após o restart; `SUCCESS`; exatamente 1 linha em `outbox_event`
   para a transação (publisher não duplicou o evento).
3. Resultado: OK. Conclusão: como outbox + transação nascem no mesmo commit
   do POST, matar a API depois do 202 é sempre seguro.

### Worker

1. Procedimento: 3 POSTs, `kill -9` no worker, restart.
2. Comportamento: health `UP`; backlog drenado; 3/3 `SUCCESS`; 3 efeitos no
   mock.
3. Resultado: OK. Conclusão: offsets não confirmados voltam via redelivery;
   idempotência mantém o resto.

## 6. Reprocessamento da DLQ

1. Procedimento: com a mensagem de `tx-f4` na DLQ (cenário 4) e o mock em
   `SUCCESS`, o payload foi republicado em `pix.requested` com a mesma key
   via `kafka-console-producer` (operação manual/operacional — não há replay
   automático nesta versão).
2. Comportamento: `SUCCESS`; mock registrou `tx-f4` exatamente 1 vez no total.
3. Resultado: OK. Conclusão: reprocessamento manual funciona e continua
   idempotente; nenhuma ferramenta nova foi criada para isso.

## Garantias comprovadas

- Nenhuma mensagem perdida em nenhum cenário (incl. kill -9).
- Retries locais (3) e assíncronos (3) respeitados; DLQ ao esgotar.
- Efeito financeiro único em todos os cenários, inclusive crash após sucesso
  do parceiro.
- Reinícios recuperam estado normalmente; idempotência válida em todo o
  fluxo; sem loops infinitos.
- Métricas (`pix_*`, lag, mock stats) + logs por `transactionId` + trace
  permitiram reconstruir cada cenário.

## Limitações conhecidas (não corrigidas nesta etapa, por escopo)

- Backoff entre ciclos async é fixo pela agenda de delays (5s/15s/30s);
  sem jitter entre ciclos e sem circuit breaker.
- DLQ sem replay automático nem UI — só investigação e repost manual.
- Estatísticas do mock são resetadas a cada troca de cenário (detalhe do
  harness de teste, não do produto).
