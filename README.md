# Plataforma PIX

Plataforma simplificada de processamento de transações PIX, construída como
desafio técnico para uma vaga de Senior Backend Engineer: API assíncrona sobre
Kafka, com PostgreSQL como fonte da verdade, outbox transacional, retry/DLQ e
observabilidade enxuta.

## Tecnologias

- Java 21 / Spring Boot 3.5 / Maven (multi-módulo, com Wrapper)
- Spring Web + Bean Validation / Spring JDBC (`JdbcTemplate`) / Spring Kafka
- PostgreSQL + Flyway / Kafka / Resilience4j (retry) / WebClient
- Actuator, Micrometer, Prometheus, OpenTelemetry (tracing + Jaeger)
- Docker / Docker Compose / Testcontainers
- JUnit 5, Mockito, k6 (pasta `load-test/`)

Detalhes de arquitetura, API, observabilidade e configuração em `docs/`
(ver **Documentação** abaixo).

## Como subir

Pré-requisitos: JDK 21, Docker + Docker Compose. Maven não é necessário
(use `./mvnw`).

```bash
# 1. Build (inclui todos os testes; exige Docker para Testcontainers)
./mvnw clean verify

# 2. Infraestrutura: Postgres, Kafka, Prometheus, Jaeger
docker compose -f infrastructure/docker-compose.yml up -d

# 3. Aplicações, uma por terminal e NESTA ORDEM
# (a API é dona das migrations Flyway — suba a API antes do worker em banco zerado)
./mvnw -pl pix-partner-mock spring-boot:run
./mvnw -pl pix-api spring-boot:run
./mvnw -pl pix-worker spring-boot:run
```

Workers extras (porta principal ociosa; só a de management precisa ser única):

```bash
PIX_WORKER_PORT=8085 PIX_WORKER_MANAGEMENT_PORT=8086 ./mvnw -pl pix-worker spring-boot:run
```

Para derrubar tudo ao final:

```bash
docker compose -f infrastructure/docker-compose.yml down
```

## Como testar

**Via REST Client (1 clique por requisição)** — abra `requests.http` na raiz
com a extensão REST Client do VS Code: health checks, criar/consultar,
idempotência, conflito 409, payload inválido, 404 e controle de cenários do
partner mock.

**Smoke rápido via curl:**

```bash
# criar (202 em milissegundos, sem esperar o parceiro)
curl -X POST http://localhost:8080/pix \
  -H 'Content-Type: application/json' \
  -d '{"transactionId":"tx-123456","amount":150.75,"pixKey":"cliente@email.com","description":"Pagamento de fatura"}'

# consultar (PROCESSING -> SUCCESS em ~2s, a latência do parceiro)
curl http://localhost:8080/pix/tx-123456

# saúde
curl http://localhost:8081/actuator/health/readiness
curl http://localhost:8083/actuator/health/readiness
```

**Testes automatizados:**

```bash
./mvnw clean verify
```

**Carga (requer k6):**

```bash
BASE_URL=http://localhost:8080 RATE=50 DURATION=60s k6 run load-test/pix.js
```

## Documentação

- `docs/architecture.md` — visão geral, diagrama, módulos, persistência,
  outbox publisher, worker, idempotência e partner gateway.
- `docs/api.md` — referência da REST API (endpoints, status codes,
  idempotência, erros, OpenAPI) e endpoints de apoio do mock.
- `docs/observability.md` — logs, métricas, tracing, health, lag e cardinalidade.
- `docs/configuration.md` — portas e variáveis de ambiente.
- `docs/architecture-decisions.md` — decisões, alternativas e trade-offs.
- `docs/load-test.md` — k6, resultados 1/2/3 workers e capacidade do outbox.
- `docs/failure-recovery.md` — cenários manuais de falha e recuperação.
- `docs/final-review.md` — revisão técnica final e critério de encerramento.
