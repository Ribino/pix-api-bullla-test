# Revisão Final

Revisão técnica completa sem alteração de código. Build final: `./mvnw clean verify` → **BUILD SUCCESS** (02:03, 118 testes, 30 classes, 0 falhas).

### Resumo

Nenhum problema **CRÍTICO** encontrado. Arquitetura, código, testes, observabilidade, performance, failure/recovery e documentação estão coerentes entre si e com a implementação — as poucas divergências encontradas (ex.: `WebClient.builder()` estático quebrando o trace, `application.yml` de teste sombreando o principal) foram corrigidas nas etapas 6–8 e estão registradas nos ADRs. Restam itens **ATENÇÃO**, todos limitações conscientes e documentadas.

### Checklist

| Área | Resultado | Observação |
|---|---|---|
| Arquitetura | ATENÇÃO | 13 de 14 itens OK; transação DB aberta durante chamada HTTP no worker (sem row lock, ver Riscos) |
| Código | ATENÇÃO | Separação e tratamento corretos; único ponto real é o mesmo da linha acima |
| Testes | OK | 118 testes, threads/DB/Kafka reais, awaits com deadline, asserções de comportamento |
| Observabilidade | OK | As 9 perguntas da etapa 8 são respondíveis; validado em smoke |
| Performance | OK | Dados suportam as conclusões (escala linear 0.5→1.0→1.5 msg/s) |
| Failure/Recovery | OK | 7 cenários mapeiam para caminhos reais de código e teste |
| Segurança | ATENÇÃO | Sem auth, admin do mock aberto, defaults locais — aceito para take-home local, sem PII em logs e SQL parametrizado |
| Documentação | OK | Cobre os 13 pontos; sem contradições (ex.: `DEFERRED` removido de código e docs; números do load-test batem com os relatórios) |

### Problemas encontrados

1. **ATENÇÃO — Transação JDBC aberta durante chamada HTTP externa.**
   Localização: `pix-worker/.../application/usecase/pix/ProcessPixTransactionUseCase.java:33` (`@Transactional` envolve `pixPartnerGateway.process()`, até ~11s no caminho de timeout).
   Impacto: segura uma conexão do pool durante chamadas de até ~11s; **não** segura row locks (SELECT simples + `UPDATE ... WHERE status='PROCESSING'` condicional), então a correção está preservada — evidência: testes de concorrência e redelivery verdes.
   Mitigação atual: escrita condicional + pool default (10) folgado para 3 threads de partição. Correção definitiva (split read/process/write) ficaria para uma etapa de resiliência, fora do escopo aqui.

2. **ATENÇÃO — Linhas `PUBLISHED` do outbox nunca são limpas.**
   Localização: schema `outbox_event` + ausência de qualquer `DELETE`/retenção.
   Impacto: crescimento ilimitado de tabela/storage ao longo de meses; polling segue eficiente via índice parcial. Retenção/arquivamento é decisão operacional futura, documentada como pendente.

3. **ATENÇÃO — Worker depende das migrations da API.**
   Localização: `pix-worker` sem Flyway; `infrastructure/docker-compose.yml` sem ordenação entre apps.
   Impacto: em ambiente zerado, worker sobe antes da API e falha até as tabelas existirem; auto-recupera via redelivery (offset não confirmado), sem perda. Mitigação: subir a API primeiro.

4. **ATENÇÃO — Topologia local sem HA.**
   Localização: `infrastructure/docker-compose.yml` (1 broker RF=1, 1 Postgres, sem backup).
   Impacto: perda/indisponibilidade em falha de container com perda de volume. Aceito: ambiente local de demonstração.

5. **ATENÇÃO — Mock com estado em memória e admin aberto.**
   Localização: `pix-partner-mock` (`ConcurrentHashMap`, `Thread.sleep` de latência, `/partner/admin/*` sem auth).
   Impacto: restrito a testes locais; restart do mock apaga o registro de idempotência. Por desenho: mock não vai a produção.

6. **ATENÇÃO — Scrape via `host.docker.internal`.**
   Localização: `infrastructure/prometheus/prometheus.yml` + `extra_hosts`.
   Impacto: em Docker antigo sem `host-gateway`, o Prometheus não alcança os apps no host. Verificado funcionando aqui.

7. **ATENÇÃO — Sem auth, rate-limit ou limites explícitos de payload.**
   Impacto: exposição total em rede aberta. Aceito: fora do escopo do teste técnico; Boot defaults aplicam-se; nenhuma secret hardcoded e nenhuma PII em labels/logs.

### Riscos aceitos

- **Pressão no pool por transação aberta (item 1).** Impacto: degradação sob alta concorrência; probabilidade baixa no demo (≤3 threads concorrentes). Mitigação: escrita condicional + pool folgado. Não resolvido: exige reestruturação do caso de uso.
- **Crescimento do outbox (item 2).** Impacto: storage/vacuum a longo prazo; probabilidade certa com o tempo. Mitigação: índice parcial. Não resolvido: política de retenção é operacional.
- **Disponibilidade local (itens 3–4).** Impacto: janelas de erro transitório; probabilidade média em fresh env. Mitigação: redelivery + volumes. Não resolvido: HA é infra de produção.
- **Sem circuit breaker / jitter entre ciclos async / replay automático de DLQ.** Impacto: trovão sincronizado em recovery massivo; DLQ exige operação manual. Probabilidade baixa no demo. Mitigação: limites e jitter intra-retry. Não resolvido: próxima etapa de resiliência.
- **Segurança mínima (itens 5, 7).** Impacto: nenhum em ambiente local fechado. Não resolvido: fora do escopo.

### Critério de encerramento

**APROVADO PARA ENCERRAMENTO**

`./mvnw clean verify` passa; API, idempotência, Outbox, Kafka, escala de workers, retry, DLQ, reprocessamento, crash/recovery e observabilidade funcionam e estão documentados; load tests e failure tests registrados; a arquitetura está explicada em `docs/architecture.md`; nenhum problema classificado como **CRÍTICO**. Os itens **ATENÇÃO** acima são limitações conscientes e documentadas.
