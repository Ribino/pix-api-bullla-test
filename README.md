# Plataforma PIX

Plataforma simplificada de processamento de transações PIX, construída como
desafio técnico para uma vaga de Senior Backend Engineer.

> **Status do projeto: funcional e completo dentro do escopo definido.**
> Estrutura do projeto, schema do banco (Flyway), domínio da transação,
> persistência (Spring JDBC), criação transacional do outbox, API REST PIX
> (`POST /pix`, `GET /pix/{transactionId}`), consumer Kafka (worker),
> integração HTTP com o parceiro (com retry/backoff), retry assíncrono + DLQ,
> observabilidade (Actuator/Micrometer/Prometheus/OpenTelemetry), testes k6,
> testes de escala e testes de falha/recuperação estão implementados.
> Ficam como evoluções futuras: circuit breaker, Grafana/alertas, replay
> automático da DLQ e política de retenção do outbox.

## Visão geral

A plataforma desacopla a API da integração com uma instituição financeira
externa, que possui latência significativa (aproximadamente 2 segundos) e pode
falhar temporariamente. O processamento é assíncrono, usando Kafka, e o
PostgreSQL é a fonte da verdade (source of truth) para o estado das transações.

## Arquitetura (alvo)

```
Client
  |
  v
PIX API  --->  PostgreSQL  (pix_transaction + outbox_event)
  |                |
  |                v
  |          Outbox Publisher
  |                |
  v                v
          Kafka (3 partitions)
                 |
        +--------+--------+
        v        v        v
     Worker   Worker   Worker
        |
        v
  Partner API Mock
```

A API e os workers são stateless e podem escalar horizontalmente.

## Persistência

O schema é versionado com **Flyway** e criado exclusivamente pelas migrations
SQL em `pix-api/src/main/resources/db/migration`. Não há JPA/Hibernate.

- `pix_transaction` — estado das transações PIX, com `UNIQUE (transaction_id)`.
- `outbox_event` — eventos do transactional outbox, com índice parcial nos
  eventos pendentes.

Criar uma transação persiste a linha `pix_transaction` e seu
`outbox_event` (tipo de evento `PIX_REQUESTED`) na **mesma transação do
PostgreSQL**. Idempotência baseada em `transaction_id` + fingerprint da
requisição.

## Outbox Publisher

Um publisher agendado (`@Scheduled`, a cada `outbox.publisher.fixed-delay-ms`,
padrão 5000) move eventos `PENDING` para o tópico Kafka `pix.requested`:

```
POST /pix
    |
    v
Transação PostgreSQL (pix_transaction + outbox_event PENDING)
    |
    v
Outbox Publisher (reserva com FOR UPDATE SKIP LOCKED, lote de N)
    |
    v
Kafka pix.requested (key = transactionId, value = payload gravado)
    |
    +-- ack --> outbox_event PUBLISHED
    +-- falha --> volta para PENDING, attempts + 1
```

- A reserva marca linhas como `PUBLISHING` em uma única transação curta, então
  múltiplas instâncias da API nunca processam o mesmo evento concorrentemente
  no caminho normal.
- Uma linha só é marcada como `PUBLISHED` após o acknowledgement do broker
  (producer usa `acks = all`).
- O pipeline é **at-least-once**: duplicatas são possíveis em janelas de crash
  e serão absorvidas pela idempotência do consumer.
- Eventos presos em `PUBLISHING` (instância que caiu) voltam a ficar elegíveis
  após `stuck-after-minutes` (padrão 5).

## Worker (Kafka Consumer)

O worker consome `pix.requested` no consumer group `pix-processing` e conduz
cada transação até um estado terminal:

```
Kafka pix.requested (key = transactionId)
    |
    v
Consumer group pix-processing
    |
    v
PixRequestedListener (fino: parse, delega, ack)
    |
    v
ProcessPixTransactionUseCase
    |
    +-- PROCESSING --> partner gateway --> SUCCESS / FAILED (escrita condicional)
    +-- SUCCESS / FAILED --> ignora (idempotente, sem segundo efeito)
    +-- transactionId desconhecido / malformado --> log ERROR + acknowledge (descarta com barulho)
    +-- falha no parceiro --> sem ack (redelivery posterior)
```

### Consumer group `pix-processing`

O Kafka atribui cada partição a exatamente um consumer dentro do grupo, então
o mesmo evento nunca é processado concorrentemente por dois workers saudáveis.
O tópico tem **3 partições** (ambiente de demonstração): 1 worker assume as 3,
3 workers assumem 1 cada. Um 4º worker ficaria ocioso nesse tópico — são as
partições, e não as instâncias, que limitam o paralelismo.

### Key = transactionId

Todos os eventos de uma transação caem na mesma partição, preservando a ordem
por transação. Como uma partição tem um único dono, o mesmo `transactionId`
nunca é processado concorrentemente em estado estável (steady state).

### At-least-once

Se o worker cair antes de commitar o offset, o Kafka reentrega tudo após o
último commit. O sistema aceita entrega repetida; a segurança vem da
idempotência, não das garantias de entrega.

### Idempotência

- A linha da transação é o registro de idempotência — sem tabelas extras.
  `SUCCESS`/`FAILED` são terminais: redelivery apenas loga e ignora, nunca
  chama o parceiro de novo.
- A escrita de status é condicional (`UPDATE ... WHERE status = 'PROCESSING'`),
  então mesmo uma corrida de rebalance (dois donos por um instante) resulta em
  uma única transição; o perdedor observa zero linhas afetadas e ignora.
- No lado do parceiro, `transactionId` é a chave de idempotência documentada
  da integração HTTP (o parceiro não pode ser alterado).

### Offset

`enable.auto.commit=false` com `ack-mode: manual_immediate`: o listener só
confirma (acknowledge) depois que o caso de uso termina. Nunca commitar antes
de processar — um crash no meio perderia a mensagem. Falhas deixam o offset
sem commit, então o Kafka reentrega depois. Inconsistências permanentes
(`transactionId` desconhecido, JSON malformado) são logadas como erro e
confirmadas para nunca travar uma partição.

### Partner gateway

`PixPartnerGateway` é implementado por `HttpPixPartnerGateway` (WebClient)
contra o `POST /partner/pix` do `pix-partner-mock`:

```
pix-worker
    |
    v
Resilience4j Retry (só transitório)
    |
    v
Partner (HTTP 200 / 400 / 5xx / timeout)
```

- **Timeout** (`pix.partner.timeout-ms`, padrão 3000) limita toda chamada HTTP;
  timeout é sempre falha transitória, nunca um hang silencioso.
- **Retry** (só `resilience4j-retry`): `5xx`, timeouts e erros de conexão têm
  retry; `400`/`422` são permanentes e nunca têm retry.
- **Tentativas**: `pix.partner.retry-max-attempts` (padrão 3) significa 3
  tentativas no total, e não 1 + 3 retries.
- **Backoff**: exponencial (`retry-initial-interval-ms` 500,
  `retry-multiplier` 2.0, teto em `retry-max-interval-ms` 2000) com jitter,
  para que workers simultâneos não tentem de novo em lockstep contra um
  parceiro em recuperação.
- **Resultado**: HTTP 200 → `SUCCESS`; 400/422 → `FAILED`; qualquer outra
  coisa após esgotar retries → sem ack Kafka (redelivery posterior). O ACK só
  acontece depois que o estado terminal foi persistido.
- **Retry local vs redelivery Kafka** são mecanismos diferentes: o Resilience4j
  repete a chamada HTTP dentro de um processamento; redelivery só acontece
  quando nenhum ack foi enviado (rebalance/restart), então não há hot loop.
- O parceiro trata `transactionId` como chave de idempotência: repetições
  retornam o resultado gravado sem um segundo efeito financeiro (o mock mantém
  um mapa em memória; endpoints admin em `/partner/admin` expõem controle de
  cenário e estatísticas para testes).

## REST API

### `POST /pix`

Cria uma transação PIX. A requisição é persistida junto com seu evento de
outbox e a API retorna imediatamente — ela **não** espera o Kafka nem a
instituição parceira. O processamento é assíncrono.

Request:

```json
{
  "transactionId": "tx-123456",
  "amount": 150.75,
  "pixKey": "cliente@email.com",
  "description": "Pagamento de fatura"
}
```

Response `202 Accepted` (transação nova) ou `200 OK` (já concluída):

```json
{
  "transactionId": "tx-123456",
  "status": "PROCESSING",
  "createdAt": "2026-09-28T21:00:00Z"
}
```

`createdAt` é um instante ISO-8601 em UTC.

### `GET /pix/{transactionId}`

Retorna a representação atual de uma transação.

Response `200 OK`:

```json
{
  "transactionId": "tx-123456",
  "status": "SUCCESS",
  "createdAt": "2026-09-28T21:00:00Z"
}
```

`status` é um de `PROCESSING`, `SUCCESS`, `FAILED`.

### Status codes

| Situação                                  | Status |
|-------------------------------------------|--------|
| `POST /pix` — transação nova              | `202`  |
| `POST /pix` — duplicada ainda `PROCESSING`| `202`  |
| `POST /pix` — duplicada já `SUCCESS`      | `200`  |
| `POST /pix` — duplicada já `FAILED`       | `200`  |
| `POST /pix` — mesmo `transactionId`, dados diferentes | `409` |
| `GET /pix/{transactionId}` — encontrada   | `200`  |
| `GET /pix/{transactionId}` — não encontrada | `404` |
| Payload de request inválido               | `400`  |
| Erro inesperado                           | `500`  |

### Idempotência

Idempotência por `transactionId` mais um fingerprint determinístico da
requisição (SHA-256 de `transactionId`, `amount`, `pixKey`, `description`):

- **`transactionId` novo** — transação e outbox event são criados; `202`.
- **`transactionId` existente, mesmo fingerprint** — nada novo é criado. A
  transação atual é retornada: `202` enquanto `PROCESSING`, `200` se já for
  `SUCCESS` ou `FAILED`.
- **`transactionId` existente, fingerprint diferente** — `409 Conflict`, body
  `{"code":"TRANSACTION_ID_CONFLICT", ...}`; a transação existente não é
  modificada.

Segurança de concorrência garantida pela unique constraint do PostgreSQL em
`transaction_id` (ver `INSERT ... ON CONFLICT`), então a API é segura com
múltiplas instâncias.

### Formato de erro

Erros seguem um formato consistente:

```json
{
  "code": "TRANSACTION_ID_CONFLICT",
  "message": "A transaction with the provided transactionId already exists with different request data.",
  "transactionId": "tx-123456"
}
```

Erros de validação incluem um array `details`:

```json
{
  "code": "VALIDATION_ERROR",
  "message": "Request validation failed.",
  "details": [
    { "field": "amount", "message": "must be greater than 0" }
  ]
}
```

### OpenAPI

Com a API no ar, a documentação interativa está disponível em:

- Swagger UI: `http://localhost:8080/swagger-ui.html`
- OpenAPI JSON: `http://localhost:8080/v3/api-docs`

## Observabilidade

Logs carregam `transactionId` (mais `event`, `result`, `error`, `retryCount`,
`reason` quando relevante) para acompanhar uma operação entre serviços.
Métricas (Micrometer/Prometheus) descrevem comportamento agregado; tracing
OpenTelemetry acompanha uma operação entre serviços.

### Logs

`transactionId` investiga uma operação individual. Payloads nunca são logados
por completo e `pixKey` é omitido salvo necessidade de diagnóstico.

### Métricas

| Métrica | Significado |
|---|---|
| `http_server_requests` (auto) | RPS da API, status, duração (P50/P95/P99 no Prometheus) |
| `pix_requests_total{result}` | Entrada PIX: `accepted`, `duplicate`, `conflict` |
| `pix_processing_total{result}` | transições terminais: `success`, `failed` |
| `pix_processing_duration{result}` | evento Kafka recebido → estado terminal persistido |
| `pix_partner_requests_total{result}` | chamadas ao parceiro: `success`, `declined`, `5xx`, `timeout`, `connection_error` |
| `pix_partner_request_duration{result}` | latência do parceiro por tentativa |
| `pix_partner_retries_total{reason}` | retries locais do Resilience4j |
| `pix_async_retries_total` | eventos roteados para `pix.retry` |
| `pix_dlq_messages_total{reason}` | eventos movidos para `pix.dlq` |
| `pix_outbox_published_total` / `pix_outbox_publish_failures_total` | resultados do outbox publisher |
| `pix_outbox_pending` | linhas pendentes no outbox (atualizado a cada 15s, nunca por request) |
| `kafka_consumer_lag_records{group,topic}` | end − offset commitado por grupo/tópico |
| JVM/CPU/GC (auto) | análise de gargalo em load tests |

### Tracing

OpenTelemetry (100% de sampling local) com instrumentação automática: servidor
HTTP/cliente, envios `KafkaTemplate` e recebimentos `@KafkaListener` propagam
o contexto de trace W3C pelos headers do Kafka. Jaeger UI:
`http://localhost:16686`.

### Health

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

### Kafka lag

`kafka_consumer_lag_records` mostra se os workers acompanham a produção.
Para checagens ad-hoc: `kafka-consumer-groups.sh --describe --group pix-processing`.

### Cardinalidade

Sem labels `transactionId`, `pixKey`, `description` ou `amount` — labels de
alta cardinalidade explodem o número de séries temporais e podem degradar o
próprio Prometheus. Investigação individual usa `transactionId` em logs e
traces.

## Stack

- Java 21
- Spring Boot 3.5
- Maven (multi-módulo, com Maven Wrapper)
- Spring Web + Bean Validation
- Spring JDBC (`JdbcTemplate`)
- Spring Kafka
- PostgreSQL + Flyway
- Springdoc OpenAPI
- Actuator, Micrometer, Prometheus, OpenTelemetry
- Docker / Docker Compose
- JUnit 5, Mockito, Testcontainers
- k6 (testes de carga, pasta `load-test/`)

## Módulos

| Módulo             | Responsabilidade                                                        |
|--------------------|-------------------------------------------------------------------------|
| `pix-api`          | REST API, persistência da transação, transactional outbox, publisher    |
| `pix-worker`       | Kafka consumer, processamento PIX contra a instituição parceira        |
| `pix-partner-mock` | Simula a instituição financeira externa para desenvolvimento local     |

Arquivos de apoio:

- `infrastructure/docker-compose.yml` — PostgreSQL, Kafka, Prometheus e Jaeger locais.
- `docs/architecture-decisions.md` — decisões de arquitetura.
- `docs/load-test.md` — testes de carga e resultados de escala.
- `docs/failure-recovery.md` — testes manuais de falha e recuperação.
- `docs/final-review.md` — revisão técnica final.
- `requests.http` — requisições prontas para a extensão REST Client (1 clique).

## Requisitos

- JDK 21
- Docker e Docker Compose
- Maven não é necessário — use o Maven Wrapper embutido (`./mvnw`)

## Como executar

### 1. Subir a infraestrutura

```bash
docker compose -f infrastructure/docker-compose.yml up -d
```

Isso sobe PostgreSQL (5432), Kafka (9092), Prometheus (9090) e Jaeger (16686).
Para derrubar tudo ao final:

```bash
docker compose -f infrastructure/docker-compose.yml down
```

### 2. Subir as aplicações (uma por terminal, nesta ordem)

A API é dona das migrations Flyway, então suba a API antes do worker em um
banco zerado.

```bash
# terminal 1 — partner mock
./mvnw -pl pix-partner-mock spring-boot:run

# terminal 2 — API (porta 8080; aplica as migrations na primeira subida)
./mvnw -pl pix-api spring-boot:run

# terminal 3 — worker (consome pix.requested)
./mvnw -pl pix-worker spring-boot:run
```

Para mais workers, repita o comando do worker com portas livres (a porta
principal 8084 é ociosa; só a de management precisa ser única):

```bash
PIX_WORKER_PORT=8085 PIX_WORKER_MANAGEMENT_PORT=8086 ./mvnw -pl pix-worker spring-boot:run
```

### 3. Testar a API

Via curl:

```bash
# criar transação (responde 202 em milissegundos, sem esperar o parceiro)
curl -X POST http://localhost:8080/pix \
  -H 'Content-Type: application/json' \
  -d '{"transactionId":"tx-123456","amount":150.75,"pixKey":"cliente@email.com","description":"Pagamento de fatura"}'

# consultar (PROCESSING -> SUCCESS em ~2s, a latência do parceiro)
curl http://localhost:8080/pix/tx-123456

# health da API e do worker
curl http://localhost:8081/actuator/health/readiness
curl http://localhost:8083/actuator/health/readiness
```

Ou, com um clique por requisição, abra `requests.http` na raiz do projeto com
a extensão **REST Client** do VS Code: health checks, criar/consultar,
idempotência (202/200), conflito (409), payload inválido (400) e controle de
cenários do partner mock (`/partner/admin/*`).

Fluxo esperado ponta a ponta: `POST → 202` → em ~2s `GET` retorna `SUCCESS`.

### 4. Observar

- Prometheus: `http://localhost:9090` (ex.: `sum(rate(pix_processing_total[1m]))`,
  `pix_outbox_pending`, `sum(kafka_consumer_lag_records) by (group)`).
- Jaeger: `http://localhost:16686` (trace `POST /pix → Kafka → worker → partner`).
- Swagger UI: `http://localhost:8080/swagger-ui.html`.

## Infraestrutura (portas)

| Serviço    | Porta |
|------------|-------|
| Postgres   | 5432  |
| Kafka      | 9092  |
| Prometheus | 9090  |
| Jaeger UI  | 16686 |

## Comandos Maven

Build do projeto inteiro (inclui testes de integração com Testcontainers —
exige Docker rodando):

```bash
./mvnw clean verify
```

Instalar módulos no repositório local:

```bash
./mvnw clean install
```

Rodar um módulo específico:

```bash
./mvnw -pl pix-api spring-boot:run
./mvnw -pl pix-worker spring-boot:run
./mvnw -pl pix-partner-mock spring-boot:run
```

## Testes de carga

```bash
BASE_URL=http://localhost:8080 RATE=50 DURATION=60s k6 run load-test/pix.js
```

Detalhes, métricas a observar e resultados (1/2/3 workers, capacidade do
outbox) em `docs/load-test.md`. Requer o `k6` instalado.

## Configuração

Cada módulo lê sua configuração do `application.yml` e suporta overrides por
variável de ambiente, com defaults sensatos para desenvolvimento local:

| Variável                      | Default                     |
|-------------------------------|-----------------------------|
| `PIX_DB_URL`                  | `jdbc:postgresql://localhost:5432/pixdb` |
| `PIX_DB_USERNAME`             | `pix`                       |
| `PIX_DB_PASSWORD`             | `pix`                       |
| `PIX_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092`            |
| `PIX_API_PORT`                | `8080`                      |
| `PIX_PARTNER_PORT`            | `8082`                      |
| `PARTNER_BASE_URL`            | `http://localhost:8082`     |
| `PARTNER_TIMEOUT_MS`          | `3000`                      |

Sem secrets hardcoded; os defaults acima servem apenas para desenvolvimento
local.
