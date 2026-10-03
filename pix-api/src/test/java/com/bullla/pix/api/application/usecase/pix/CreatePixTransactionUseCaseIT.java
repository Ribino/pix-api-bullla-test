package com.bullla.pix.api.application.usecase.pix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.bullla.pix.api.PostgresIntegrationTestBase;
import com.bullla.pix.api.application.exception.DuplicateTransactionConflictException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class CreatePixTransactionUseCaseIT extends PostgresIntegrationTestBase {

    @Autowired
    private CreatePixTransactionUseCase createPixTransactionUseCase;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void createsTransactionAndOutboxEventInTheSameDatabaseTransaction() throws Exception {
        PixTransactionResult result = createPixTransactionUseCase.execute(command("tx-new", "150.75", "Pagamento de fatura"));

        assertThat(result.created()).isTrue();
        assertThat(result.transaction().status().name()).isEqualTo("PROCESSING");
        assertThat(count("pix_transaction")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);

        Map<String, Object> event = jdbcTemplate.queryForMap(
                "SELECT aggregate_id, event_type, status, attempts, published_at, payload::text AS payload FROM outbox_event");
        assertThat(event.get("aggregate_id")).isEqualTo("tx-new");
        assertThat(event.get("event_type")).isEqualTo("PIX_REQUESTED");
        assertThat(event.get("status")).isEqualTo("PENDING");
        assertThat(event.get("attempts")).isEqualTo(0);
        assertThat(event.get("published_at")).isNull();

        JsonNode payload = objectMapper.readTree((String) event.get("payload"));
        assertThat(payload.get("eventType").asText()).isEqualTo("PIX_REQUESTED");
        assertThat(payload.get("transactionId").asText()).isEqualTo("tx-new");
        assertThat(payload.get("pixKey").asText()).isEqualTo("cliente@email.com");
        assertThat(payload.get("description").asText()).isEqualTo("Pagamento de fatura");
        assertThat(payload.get("amount").decimalValue()).isEqualByComparingTo("150.75");
    }

    @Test
    void usesAnEventIdDifferentFromTheTransactionId() throws Exception {
        createPixTransactionUseCase.execute(command("tx-event-id", "10.00", "Pagamento"));

        UUID transactionInternalId = jdbcTemplate.queryForObject("SELECT id FROM pix_transaction", UUID.class);
        UUID eventId = jdbcTemplate.queryForObject("SELECT id FROM outbox_event", UUID.class);
        JsonNode payload = objectMapper.readTree(
                jdbcTemplate.queryForObject("SELECT payload::text FROM outbox_event", String.class));

        assertThat(eventId).isNotEqualTo(transactionInternalId);
        assertThat(payload.get("eventId").asText()).isEqualTo(eventId.toString());
        assertThat(payload.get("transactionId").asText()).isEqualTo("tx-event-id");
    }

    @Test
    void isIdempotentForTheSameTransactionIdAndPayload() {
        CreatePixTransactionCommand command = command("tx-idempotent", "150.75", "Pagamento");

        PixTransactionResult first = createPixTransactionUseCase.execute(command);
        PixTransactionResult second = createPixTransactionUseCase.execute(command);

        assertThat(first.created()).isTrue();
        assertThat(second.created()).isFalse();
        assertThat(second.transaction().id()).isEqualTo(first.transaction().id());
        assertThat(count("pix_transaction")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
    }

    @Test
    void signalsConflictForTheSameTransactionIdWithDifferentPayload() {
        createPixTransactionUseCase.execute(command("tx-conflict", "150.75", "Pagamento"));

        assertThatThrownBy(() -> createPixTransactionUseCase.execute(command("tx-conflict", "999.99", "Pagamento")))
                .isInstanceOf(DuplicateTransactionConflictException.class);

        assertThat(count("pix_transaction")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
    }

    @Test
    void persistsOnlyOneTransactionUnderConcurrentCreations() throws Exception {
        CreatePixTransactionCommand command = command("tx-concurrent", "150.75", "Pagamento");
        int threads = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<PixTransactionResult>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < threads; i++) {
                Callable<PixTransactionResult> task = () -> {
                    ready.countDown();
                    start.await();
                    return createPixTransactionUseCase.execute(command);
                };
                futures.add(executor.submit(task));
            }

            assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            List<PixTransactionResult> results = new ArrayList<>();
            for (Future<PixTransactionResult> future : futures) {
                results.add(future.get(30, TimeUnit.SECONDS));
            }

            assertThat(results).filteredOn(PixTransactionResult::created).hasSize(1);
            assertThat(count("pix_transaction")).isEqualTo(1);
            assertThat(count("outbox_event")).isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    private CreatePixTransactionCommand command(String transactionId, String amount, String description) {
        return new CreatePixTransactionCommand(
                transactionId, new BigDecimal(amount), "cliente@email.com", description);
    }
}
