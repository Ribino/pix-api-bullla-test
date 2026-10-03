# Arquitetura

Visão geral da plataforma, seus módulos e o fluxo de uma transação PIX.

## Visão geral

A plataforma desacopla a API da integração com uma instituição financeira
externa, que possui latência significativa (aproximadamente 2 segundos) e pode
falhar temporariamente. O processamento é assíncrono, usando Kafka, e o
PostgreSQL é a fonte da verdade (source of truth) para o estado das transações.

## Diagrama

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

## Módulos

| Módulo             | Responsabilidade                                                        |
|--------------------|-------------------------------------------------------------------------|
| `pix-api`          | REST API, persistência da transação, transactional outbox, publisher    |
| `pix-worker`       | Kafka consumer, processamento PIX contra a instituição parceira        |
| `pix-partner-mock` | Simula a instituição financeira externa para desenvolvimento local     |

## Persistência

O schema é versionado com **Flyway** e criado exclusivamente pelas migrations
SQL em `pix-api/src/main/resources/db/migration`. Não há JPA/Hibernate. A API
é dona das migrations — o worker exige banco migrado, então suba a API antes
do worker em um banco zerado.

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
  retornam o resultado gravado sem um segundo efeito financeiro. Esgotado o
  retry local, o evento segue para `pix.retry` (com delay via pause de
  partição) e depois para `pix.dlq` (1 partição, sem consumer automático —
  replay é manual/operacional). A transação permanece `PROCESSING` o tempo
  todo: nenhum estado novo foi criado para a DLQ.
