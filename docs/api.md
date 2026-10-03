# Referência da API

Contratos HTTP da `pix-api` e endpoints de apoio do `pix-partner-mock`.
Para testar com 1 clique, use `requests.http` na raiz do projeto (extensão
REST Client do VS Code).

## `POST /pix`

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

- `transactionId`: obrigatório, não-blank, até 100 caracteres.
- `amount`: obrigatório, `BigDecimal` maior que zero (nunca `double`/`float`).
- `pixKey`: obrigatório, não-blank, até 255 caracteres (string livre, sem
  validação de e-mail).
- `description`: opcional, até 500 caracteres.

Response `202 Accepted` (transação nova) ou `200 OK` (já concluída):

```json
{
  "transactionId": "tx-123456",
  "status": "PROCESSING",
  "createdAt": "2026-09-28T21:00:00Z"
}
```

`createdAt` é um instante ISO-8601 em UTC.

## `GET /pix/{transactionId}`

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

## Status codes

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

## Idempotência

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

## Formato de erro

Erros seguem um formato consistente (sem stack traces, sem SQL, sem detalhes
internos):

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

## OpenAPI

Com a API no ar, a documentação interativa está disponível em:

- Swagger UI: `http://localhost:8080/swagger-ui.html`
- OpenAPI JSON: `http://localhost:8080/v3/api-docs`

## Partner mock (apoio a testes)

`POST /partner/pix` simula a instituição externa (latência ~2s configurável,
cenários `success`/`flaky`/`always-500`/`always-400`/`slow`, idempotência por
`transactionId` em mapa em memória — detalhe de mock, não de produção).

Endpoints de controle **só para testes** (sem auth, por desenho):

- `POST /partner/admin/scenario` — troca o cenário (`mode`, `latencyMs`,
  `failTimes`, `slowDelayMs`).
- `POST /partner/admin/reset` — limpa estado e estatísticas.
- `GET /partner/admin/stats` — `requestCount`, `processedCount`,
  `processedTransactionIds`.
