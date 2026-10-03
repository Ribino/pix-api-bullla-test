# Decisões de Arquitetura

Este documento registra as decisões de arquitetura da plataforma PIX.

> Status: plataforma funcional dentro do escopo definido. Este documento
> consolida as decisões tomadas ao longo das etapas, com justificativas,
> alternativas e trade-offs.

## Contexto

A plataforma processa transações PIX e precisa ser desacoplada da integração
com uma instituição financeira externa que possui latência significativa
(aproximadamente 2 segundos) e pode falhar temporariamente. O processamento é,
portanto, assíncrono e baseado em Kafka.

## Decisões estabelecidas

- **Processamento assíncrono** das transações PIX.
- **Kafka** como espinha dorsal de mensageria entre a API e os workers.
- **3 partições** no ambiente de demonstração.
- **Consumer group `pix-processing`** para os workers.
- **PostgreSQL como fonte da verdade** para o estado das transações.
- **Transactional Outbox** para publicar eventos da API com confiabilidade.
- **Outbox Publisher baseado em polling**.
- **Idempotência baseada em `transactionId`**.
- **At-least-once processing**.
- **Retry com exponential backoff**.
- **Escala horizontal da API**.
- **Escala horizontal dos workers**.
- **Docker Compose** para o ambiente local.

## Outbox Publisher

Status: aceito.

### Contexto

O `POST /pix` persiste a transação e seu `outbox_event` na mesma transação do
PostgreSQL. Os eventos pendentes precisam chegar ao tópico Kafka
`pix.requested` sem acoplar a API ao Kafka e sem perder eventos quando o Kafka
estiver indisponível. Múltiplas instâncias da `pix-api` executam o publisher
concorrentemente, e não há transação distribuída entre PostgreSQL e Kafka.

### Decisão

- Poll em `outbox_event` com `@Scheduled` (`outbox.publisher.fixed-delay-ms`,
  padrão 5000, sobrescrevível por env) em lotes limitados
  (`outbox.publisher.batch-size`, padrão 100).
- Reserva das linhas em uma única transação curta com
  `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED LIMIT ?) RETURNING`.
  A reserva marca `status = 'PUBLISHING'`, `attempts = attempts + 1` e
  `claimed_at = now`.
- Publica cada evento reservado com `KafkaTemplate` usando
  `topic = pix.requested`, `key = transactionId` e o payload exato gravado
  no outbox. Aguarda o acknowledgement do broker (`.get(send-timeout-ms)`) e
  só então marca a linha como `PUBLISHED` com `published_at`. O producer usa
  `acks = all`, então um acknowledgement significa que o registro está
  duravelmente no tópico.
- Em falha, reverte a linha para `PENDING` (mantendo o `attempts` incrementado)
  para nova tentativa num poll posterior. Nunca deleta eventos automaticamente.
- Eventos presos em `PUBLISHING` (instância caiu entre a reserva e a conclusão)
  voltam a ficar elegíveis quando `claimed_at` for mais antigo que
  `stuck-after-minutes` (padrão 5). Duplicatas nessa janela são aceitas:
  o pipeline é at-least-once e o consumer é idempotente.
- O tópico `pix.requested` (3 partições, conforme a arquitetura definida) é
  autocriado pela API via bean `NewTopic` (replicação 1, ambiente local).

### Alternativas consideradas

- **Reserva só com PENDING (sem estado intermediário).** Rejeitada: após o
  commit da reserva a linha continua `PENDING`, então uma segunda instância
  reserva e publica concorrentemente no *caminho normal* — duplicatas por
  desenho, não só em janelas de falha.
- **Segurar o row lock durante a chamada ao Kafka.** Rejeitada: uma transação
  de banco nunca deve abranger uma chamada externa ilimitada (pressão no pool,
  retenção de lock), e ainda assim não tornaria PostgreSQL + Kafka atômicos.
- **Exactly-once de ponta a ponta.** Rejeitada como inalcançável aqui: a janela
  entre acknowledgement do Kafka e commit do `PUBLISHED` sempre permite
  duplicata após crash. At-least-once mais idempotência no consumer é a
  escolha coerente.

### Consequências e trade-offs

- Duplicatas no caminho normal entre instâncias são evitadas; duplicatas
  continuam possíveis em janelas de crash/recuperação (aceito, tratado
  downstream).
- Um evento envenenado (ex.: rejeitado permanentemente pelo broker) tenta de
  novo a cada poll e só acumula `attempts`; a política de backoff/DLQ do lado
  do outbox fica para uma etapa posterior, com `attempts` pronto para guiá-la.
- O timeout de `claimed_at` (padrão 5 minutos) precisa exceder qualquer
  latência legítima de publicação, senão uma instância lenta porém viva pode
  ter seu evento retomado.
- A tabela do outbox cresce de forma append-mostly com índice parcial nas
  linhas pendentes; linhas `PUBLISHED` são retidas (política de limpeza
  pendente).

## Worker consumer e idempotência

Status: aceito.

### Contexto

O tópico `pix.requested` (3 partições, key = `transactionId`) alimenta o
consumer group `pix-processing`. O Kafka oferece entrega at-least-once: um
crash antes do commit do offset causa redelivery, e um rebalance pode entregar
brevemente a mesma partição a dois workers. Uma entrega repetida nunca pode
causar uma segunda operação financeira, e o parceiro (externo, imutável)
usa `transactionId` como chave de idempotência.

### Decisão

- Gerenciamento manual de offset (`enable.auto.commit=false`,
  `ack-mode: manual_immediate`): confirma (acknowledge) somente após o caso de
  uso concluir. Nunca commitar antes de processar.
- `@KafkaListener` fino delegando para `ProcessPixTransactionUseCase`;
  regras de negócio ficam no caso de uso, nunca no listener.
- Sem tabelas novas: `pix_transaction.status` é o registro de idempotência.
  `SUCCESS`/`FAILED` são terminais — redelivery ignora sem tocar no parceiro.
  Transições usam escrita condicional
  (`UPDATE ... WHERE status = 'PROCESSING'`) para que mesmo uma corrida de
  rebalance resulte em uma única transição.
- Porta `PixPartnerGateway` implementada pelo cliente HTTP real
  (`HttpPixPartnerGateway` com WebClient + Resilience4j Retry);
  `transactionId` segue documentado como chave de idempotência do parceiro.
- Inconsistências permanentes (`transactionId` desconhecido, payload
  malformado) são logadas como erro e confirmadas para nunca travar uma
  partição; falhas transitórias ficam sem acknowledge para redelivery.
- Retry local (Resilience4j, 3 tentativas com backoff+jitter) só para falhas
  transitórias (5xx/timeout/conexão); 400/422 viram `DECLINED` → `FAILED` sem
  retry. Esgotado o retry local, roteamento para `pix.retry` (com delay via
  pause de partição) e depois `pix.dlq`, sem novos estados de domínio — a
  transação permanece `PROCESSING` até decisão operacional.

### Alternativas consideradas

- **Auto-commit de offsets.** Rejeitada: um crash entre poll e processamento
  perde mensagens silenciosamente.
- **Tabela dedicada de eventos processados.** Rejeitada: duplicaria o status
  já gravado em `pix_transaction` sem garantia extra.
- **Lock distribuído para consumers.** Rejeitada: o assignment de partições do
  Kafka já dá ownership único por partição; coordenação extra adiciona um modo
  de falha sem benefício.
- **Exactly-once de ponta a ponta (transações EOS).** Rejeitada: complexidade
  e requisitos de broker muito além desta etapa; at-least-once mais
  idempotência é a escolha coerente já estabelecida.

### Consequências e trade-offs

- Duplicatas são possíveis (crash antes do ack, janela de rebalance); todos os
  caminhos são seguros por construção, e a janela de rebalance no meio da
  chamada ao parceiro é absorvida pela idempotência do parceiro por
  `transactionId` (implementada e testada).
- Mensagens sem acknowledge só são reentregues em rebalance/restart (commits
  do Kafka são cumulativos), então redelivery não é um loop imediato de retry.
- O worker possui uma cópia do modelo de leitura `pix_transaction` (sem
  módulos compartilhados entre serviços ainda); o schema continua sendo das
  migrations da `pix-api` e o worker não embarca Flyway.

## Integração com parceiro e retry

Status: aceito.

### Contexto

O worker chama o parceiro via HTTP (`POST /partner/pix`). O parceiro tem
~2s de latência e falha de forma transitória (5xx, timeouts, erros de conexão)
e permanente (rejeições de negócio 400/422). Não há transação distribuída
entre Kafka, PostgreSQL e HTTP, então um crash pode sempre cair entre
"parceiro aprovou" e "banco atualizado".

### Decisão

- **Timeout** em toda chamada HTTP (`pix.partner.timeout-ms`, padrão 3000),
  sempre classificado como transitório.
- **Retry** (só `resilience4j-retry` — sem CircuitBreaker/RateLimiter ainda):
  retryable = 5xx, timeouts, falhas de conexão, status inesperados;
  nunca retryable = 400/422, que mapeiam para `DECLINED` → `FAILED`.
- **Limite**: `retry-max-attempts` (padrão 3) conta tentativas totais, não
  retries após a primeira.
- **Backoff**: exponencial com jitter (inicial 500ms, x2, teto 2000ms).
  Retries imediatos martelariam um parceiro em dificuldade; o jitter espalha
  workers simultâneos em recuperação (thundering herd).
- **Forma da classificação**: `PartnerResult` (`APPROVED`/`DECLINED`) para
  outcomes terminais, `TransientPartnerException` para qualquer coisa
  retryable. Sem tipo `PermanentPartnerException` — um outcome permanente é
  um resultado normal, não uma exceção; só a falha *em obter um outcome* é
  excepcional.
- **Após esgotar**: deixa o offset Kafka sem commit (redelivery em
  rebalance/restart), de onde segue para `pix.retry` e eventualmente `pix.dlq`.
  Sem DLQ... (ver decisão de retry assíncrono/DLQ abaixo). Sem hot loop, pois
  ack manual nunca rebobina imediatamente.

### Alternativas consideradas

- **Sem jitter.** Rejeitada: barato de adicionar (`ofExponentialRandomBackoff`)
  e endereça diretamente retries correlacionados de workers paralelos.
- **Retry em 4xx também.** Rejeitada: rejeições de negócio nunca se curam
  esperando; tentar de novo desperdiça capacidade do parceiro e atrasa o
  `FAILED` terminal.
- **Marcar FAILED após esgotar retries.** Rejeitada por ora: uma indisponibilidade
  transitória falharia terminalmente transações que uma redelivery posterior
  ainda poderia concluir; redelivery preserva a chance de sucesso.
- **Transação distribuída (Kafka + Postgres + HTTP).** Rejeitada: não existe
  nada assim entre esses sistemas. A segurança vem do par `transactionId`
  (chave de idempotência do parceiro, reenviada em toda tentativa e redelivery)
  + `UPDATE ... WHERE status = 'PROCESSING'` condicional.

### Consequências e trade-offs

- Crash-após-sucesso-do-parceiro é seguro: redelivery reenvia o mesmo
  `transactionId`, o parceiro retorna o outcome gravado, a escrita condicional
  converge para o estado terminal.
- Um evento envenenado (400 permanente) termina rápido com uma única chamada.
- Tempestades de retry são limitadas (3 tentativas, backoff com teto, jitter);
  uma indisponibilidade total estaciona mensagens até redelivery.

## Retry assíncrono e DLQ

Status: aceito.

### Contexto

Esgotado o retry local, voltar a mensagem para `pix.requested` criaria redelivery
imediato em loop. É preciso estacionar a mensagem com atraso e, após N ciclos,
preservá-la para investigação — sem travar a partição principal e sem novos
estados de domínio.

### Decisão

- Roteamento para `pix.retry` (mesma key `transactionId`, headers
  `pix-retry-count`, `pix-failure-reason`, `pix-first-failure-at`,
  `pix-original-topic`) com ACK da mensagem original; listener dedicado no
  group `pix-retry` republica em `pix.requested` quando vencido o delay
  (`5s,15s,30s` por ciclo, configurável).
- Delay via pause/rewind de partição (`seek` + `pause`, resume por scheduler):
  sem `Thread.sleep`, sem busy loop, sem scheduler distribuído.
- Após `max-retries` async (padrão 3): publica em `pix.dlq` (1 partição, log
  de investigação) com os mesmos headers + `pix-failure-reason=RETRY_EXHAUSTED`
  e payload intacto; **sem consumer automático** — replay é manual/operacional.
- A transação permanece `PROCESSING` o tempo todo: nenhum estado novo foi
  criado para representar DLQ; a DLQ representa o problema operacional.

### Alternativas consideradas

- **Sem DLQ (redelivery infinito).** Rejeitada: reprocessa 9 chamadas caras a
  cada rebalance e a mensagem envenenada bloqueia a partição atrás dela.
- **DLQ direta sem retry async.** Rejeitada: perde a chance de auto-recuperação
  em blips transitórios.
- **Múltiplos tópicos de retry com delays fixos.** Rejeitada: mesma mecânica
  ×3 sem benefício nesta escala.
- **Tabela de retries no banco.** Rejeitada: o offset + headers do Kafka já
  carregam o estado; nova tabela seria redundância.

### Consequências e trade-offs

- Uma mensagem não-vencida segura sua partição no retry (head-of-line por
  partição) — aceitável em baixo volume; documentado.
- Rebalance durante pause é auto-curável (resume por tempo; headers intactos).
- Replay da DLQ é manual via console-producer, preservando key e payload.

## Estratégia de Observabilidade

Status: aceito.

### Decisão

Observabilidade enxuta com Actuator, Micrometer/Prometheus e OpenTelemetry:
poucos counters/timers/gauges de negócio com labels de baixa cardinalidade,
logs estruturados estilo `event=` com `transactionId` como chave, propagação
de trace W3C através da fronteira Kafka via observation do Spring Kafka, e
probes de liveness/readiness com escopos de dependência explícitos.

### Motivo

Responder às perguntas de revisão com o mínimo de maquinário: velocidade da
API (métricas HTTP), saúde do processamento (`pix_processing_*`), gargalo e
falhas do parceiro (`pix_partner_*`), backlog (`kafka_consumer_lag_records`,
`pix_outbox_pending`), pressão de retry (`pix_partner_retries_total`,
`pix_async_retries_total`), mensagens problemáticas (`pix_dlq_messages_total`)
e diagnóstico por transação (logs + trace). Tudo aqui também alimenta a etapa
futura de k6.

### Métricas

Somente as métricas listadas na tabela de Observabilidade do README foram
implementadas — outcomes de negócio, um gauge atualizado por agendamento
(nunca por request), mais métricas auto de Boot/JVM/Kafka. Sem infraestrutura
customizada para métricas.

### Trade-offs

- **Cardinalidade**: sem labels `transactionId`/PII; lookup individual fica em
  logs/traces. Custo: alertas precisam ser escritos sobre agregados.
- **Overhead**: poucos counters + 100% de sampling local; produção reduziria
  sampling por volume/custo.
- **Limites**: sem Grafana/alertas/agregação de logs (só UIs de Prometheus +
  Jaeger); gauge de lag consulta o AdminClient a cada 15s (grosseiro porém
  barato); Jaeger all-in-one e Prometheus são locais, não topologia de produção.
- Dois achados ao implementar: o Boot **não** traz health indicator de Kafka
  (um pequeno custom foi adicionado para o readiness do worker), e um
  `src/test/resources/application.yml` com mesmo nome **sombreia** o principal
  no classpath — propriedades só-de-teste agora vivem em `@TestPropertySource`.
- O worker roda sem `web-application-type: none`: o Spring Boot não sobe o
  management server para app não-web, então o worker é um app web reativo cuja
  porta principal (8084) não serve endpoints de negócio — todo tráfego de
  negócio continua no Kafka, management isolado na 8083.
