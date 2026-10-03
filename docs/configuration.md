# Configuração

Cada módulo lê sua configuração do `application.yml` e suporta overrides por
variável de ambiente, com defaults sensatos para desenvolvimento local. Sem
secrets hardcoded; os defaults abaixo servem apenas para desenvolvimento local.

## Portas

| Serviço    | Porta | Variável de ambiente |
|------------|-------|----------------------|
| Postgres   | 5432  | `PIX_DB_PORT`        |
| Kafka      | 9092  | `PIX_KAFKA_PORT`     |
| API (app)  | 8080  | `PIX_API_PORT`       |
| API (management) | 8081 | `PIX_API_MANAGEMENT_PORT` |
| Worker (app, ociosa) | 8084 | `PIX_WORKER_PORT` |
| Worker (management) | 8083 | `PIX_WORKER_MANAGEMENT_PORT` |
| Partner mock | 8082 | `PIX_PARTNER_PORT`  |
| Prometheus | 9090  | `PIX_PROMETHEUS_PORT` |
| Jaeger UI  | 16686 | `PIX_JAEGER_UI_PORT` |

A porta principal do worker (8084) é ociosa por desenho: o Spring Boot não
sobe o management server para app não-web, então o worker é um app web reativo
sem endpoints de negócio — todo tráfego de negócio continua no Kafka e o
management fica isolado na 8083. Para mais workers locais, repita o processo
com portas livres (ex.: `PIX_WORKER_PORT=8085 PIX_WORKER_MANAGEMENT_PORT=8086`).

## Variáveis principais

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

Outras chaves (retry, outbox publisher, retry assíncrono/DLQ, tracing) seguem
o mesmo padrão `propriedade` → `VARIÁVEL_DE_AMBIENTE` — ver os
`application.yml` de cada módulo para a lista completa.
