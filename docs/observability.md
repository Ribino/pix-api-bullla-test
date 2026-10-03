# Observabilidade

Logs carregam `transactionId` (mais `event`, `result`, `error`, `retryCount`,
`reason` quando relevante) para acompanhar uma operação entre serviços.
Métricas (Micrometer/Prometheus) descrevem comportamento agregado; tracing
OpenTelemetry acompanha uma operação entre serviços.

## Logs

`transactionId` investiga uma operação individual. Payloads nunca são logados
por completo e `pixKey` é omitido salvo necessidade de diagnóstico.

## Métricas

| Métrica | Significado |
|---|---|
| `http_server_requests` (auto) | RPS da API, status, duração (P50/P95/P99 no Prometheus) |
| `pix_requests_total{result}` | Entrada PIX: `accepted`, `duplicate`, `conflict` |
| `pix_processing_total{result}` | transições terminais: `success`, `failed` |
| `pix_processing_duration{result}` | Kafka event recebido → estado terminal persistido |
| `pix_partner_requests_total{result}` | chamadas ao parceiro: `success`, `declined`, `5xx`, `timeout`, `connection_error` |
| `pix_partner_request_duration{result}` | latência do parceiro por tentativa |
| `pix_partner_retries_total{reason}` | retries locais do Resilience4j |
| `pix_async_retries_total` | eventos roteados para `pix.retry` |
| `pix_dlq_messages_total{reason}` | eventos movidos para `pix.dlq` |
| `pix_outbox_published_total` / `pix_outbox_publish_failures_total` | resultados do outbox publisher |
| `pix_outbox_pending` | linhas pendentes no outbox (atualizado a cada 15s, nunca por request) |
| `kafka_consumer_lag_records{group,topic}` | end − offset commitado por grupo/tópico |
| JVM/CPU/GC (auto) | análise de gargalo em load tests |

## Tracing

OpenTelemetry (100% de sampling local) com instrumentação automática: servidor
HTTP/cliente, envios `KafkaTemplate` e recebimentos `@KafkaListener` propagam
o contexto de trace W3C pelos headers do Kafka. Jaeger UI:
`http://localhost:16686`.

## Health

Liveness responde "o processo está vivo?" (sem dependência de infraestrutura,
então um banco doente nunca provoca restart). Readiness responde "consegue
fazer seu trabalho?": a API exige PostgreSQL; o worker exige PostgreSQL **e**
Kafka (um indicator customizado, pois o Boot não traz nenhum); o parceiro
deliberadamente **não** é gate de readiness — o worker continua pronto
durante outages do parceiro e segue tentando.

| Serviço | App | Management (health, prometheus) |
|---|---|---|
| pix-api | 8080 | 8081 |
| pix-worker | 8084 (ociosa, sem endpoints de negócio) | 8083 |
| pix-partner-mock | 8082 | 8082 |

HTTP de negócio nunca se mistura com HTTP de management.

## Kafka lag

`kafka_consumer_lag_records` mostra se os workers acompanham a produção.
Para checagens ad-hoc: `kafka-consumer-groups.sh --describe --group pix-processing`.

## Cardinalidade

Sem labels `transactionId`, `pixKey`, `description` ou `amount` — labels de
alta cardinalidade explodem o número de séries temporais e podem degradar o
próprio Prometheus. Investigação individual usa `transactionId` em logs e
traces.
