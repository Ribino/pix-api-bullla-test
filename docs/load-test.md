# Teste de Carga (k6)

Medição de baseline da arquitetura atual. Sem tuning ainda — o objetivo é
observar o comportamento, não corrigi-lo.

## Como executar

Requisitos: infraestrutura no ar (`docker compose -f infrastructure/docker-compose.yml up -d`)
mais `pix-api`, um `pix-worker` e `pix-partner-mock` rodando.

```bash
# baseline conservador: 50 req/s por 60s, 1 worker
BASE_URL=http://localhost:8080 \
RATE=50 \
DURATION=60s \
k6 run load-test/pix.js
```

| Variável   | Padrão                | Significado              |
|------------|-----------------------|--------------------------|
| `BASE_URL` | `http://localhost:8080` | URL base da API        |
| `RATE`     | `50`                  | taxa de chegada constante (req/s) |
| `DURATION` | `60s`                 | duração do teste         |

Cada iteração posta um `transactionId` único e valida HTTP 202 com o
mesmo id devolvido como `PROCESSING`.

## Métricas a observar

O sumário do k6 (stdout) dá, para o `POST /pix`:

- `http_reqs` — throughput total (deve ser ≈ RATE × DURATION).
- `http_req_duration` — avg/med/p90/p95/p99 e `max`.
- `http_req_failed` — deve ficar ~0 (threshold `rate<0.01`).
- `checks` — as asserções de 202/echo; falhas significam quebra funcional.

Métricas do projeto (Prometheus em `http://localhost:9090`, Jaeger em
`http://localhost:16686`):

- API: `rate(pix_requests_total[1m])`, `histogram_quantile(0.95, …http_server_requests…)`.
- Processamento: `pix_processing_total`, `pix_processing_duration_*`.
- Parceiro: `pix_partner_requests_total`, `pix_partner_request_duration_*`.
- Backlog: `pix_outbox_pending`, `kafka_consumer_lag_records`.
- Falhas: `pix_partner_retries_total`, `pix_async_retries_total`, `pix_dlq_messages_total`.
- JVM/CPU/GC automáticas para análise de gargalo.

## O que significam P95/P99

P95 = 95% das requisições terminaram em até essa duração; P99 o mesmo para
99%. Eles descrevem a latência de cauda — a experiência dos usuários mais
lentos — que a média esconde. Para comparações no k6 entre quantidades de
workers, compare P95/P99 (e max), não só a média.

## Consultando as métricas do projeto

```promql
# taxa de entrada
sum(rate(pix_requests_total[1m]))
# P95 da latência do POST
histogram_quantile(0.95, sum(rate(http_server_requests_seconds_bucket{uri="/pix"}[5m])) by (le))
# throughput do worker
sum(rate(pix_processing_total[1m]))
# backlog
pix_outbox_pending
sum(kafka_consumer_lag_records) by (group)
```

## Comparando quantidades de workers

Rode o mesmo `RATE`/`DURATION` com 1, 2 e 3 workers e compare:

1. Taxa de crescimento de `kafka_consumer_lag_records{group="pix-processing"}` —
   ela achata com mais workers?
2. Taxa de `pix_processing_total` vs taxa de entrada — quando se encontram?
3. `pix_outbox_pending` — backlog do lado do publisher (independente dos workers).
4. P95/P99 da API — devem ficar planos (a API nunca espera o processamento).
5. `pix_partner_request_duration` — o piso de latência do parceiro (~2s) limita
   qualquer worker single-thread a ~0,5 msg/s; paralelismo vem das partições.

Baseline esperado com 1 worker: API rápida, outbox e lag do Kafka crescem
linearmente, o worker converge muito depois do fim do teste. Essa lacuna é o
que as execuções multi-worker devem fechar.

## Resultados: 1 vs 2 vs 3 workers (50 req/s × 60s cada, volumes frescos)

| Cenário | Workers | throughput k6 | falhas k6 | P95 API | P99 API | Throughput worker | SUCCESS (fim do teste) | Chamadas parceiro | Lag Kafka (fim) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Baseline | 1 | 50,0 req/s | 0% | ~6ms | 11,7ms | ~0,5 msg/s | ~35 | ~35 | crescendo |
| Escala | 2 | 50,0 req/s | 0% | 11,0ms | 29,7ms | ~1,0 msg/s | 62 | 64 | crescendo (376/391/366) |
| Escala | 3 | 50,0 req/s | 0% | 13,0ms | 46,7ms | ~1,5 msg/s | 90 | 93 | crescendo (413/323/381) |

Notas:

- Throughput do worker ≈ 0,5 msg/s por worker (piso de latência do parceiro ~2s,
  consumer single-thread). Escala é linear: 1 → 2 → 3 workers.
- Assignment de partições observado via `kafka-consumer-groups.sh`: 1 worker
  segura as 3 partições; 2 workers dividem 2+1; 3 workers seguram 1 cada com
  split parelho de 32/32/32 mensagens. Um 4º worker ficaria ocioso nesse tópico
  (não testado, fora do escopo).
- Contagem processada bate com `processedCount` do parceiro em todas as
  execuções (sem duplicatas, sem perdas).
- P95/P99 da API sem tendência de degradação com a quantidade de workers
  (variações pequenas são ruído); a API nunca espera o processamento.
- Chegada (50/s) ainda anã o processamento (≤1,5/s), então o lag continua
  crescendo em todas as execuções. Depois dos workers, os próximos gargalos na
  fila são as 3 partições (teto de paralelismo) e o outbox publisher (~20/s:
  batch 100 a cada 5s).

## Capacidade do outbox publisher (50 req/s × 60s, 1 worker, volumes frescos)

Só config do publisher mudou (`PIX_OUTBOX_PUBLISHER_BATCH_SIZE`,
`PIX_OUTBOX_PUBLISHER_FIXED_DELAY_MS`). Sem mudanças de código, schema ou worker.

| Configuração | Entrada/s | Publicação/s | PENDING final | Lag Kafka (fim) | Falhas |
|---|---:|---:|---:|---:|---:|
| Atual (batch 100, delay 5000ms) | 50 | ~18 | 1989 crescendo | crescendo | 0 |
| Ajustada (batch 500, delay 1000ms) | 50 | ~50 sustentados (3001/3001 drenados) | 0 | crescendo (limitado pelo worker) | 0 |

Notas:

- Baseline publica ~18/s (teórico 20/s menos custo do send Kafka por mensagem)
  — PENDING cresce ~30/s.
- Config ajustada drena tudo: `PUBLISHED` 3001/3001, `PENDING` 0,
  `pix_outbox_publish_failures_total` 0, k6 0 erros, P95 8,9ms / P99 13,1ms.
- Lag Kafka `pix-processing` continua crescendo (~868): esperado — o worker
  único downstream ainda processa ~0,5 msg/s. Isso é capacidade do worker, não
  do outbox, e está fora do escopo desta medição.
- Sem duplicatas ou perdas em ambas as execuções (processado == gravado no parceiro).
