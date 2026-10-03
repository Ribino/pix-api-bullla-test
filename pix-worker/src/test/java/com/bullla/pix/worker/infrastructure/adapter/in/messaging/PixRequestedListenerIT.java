package com.bullla.pix.worker.infrastructure.adapter.in.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.bullla.pix.worker.WorkerIntegrationTestBase;
import com.bullla.pix.worker.domain.gateway.pix.PartnerResult;
import com.bullla.pix.worker.domain.gateway.pix.PixPartnerGateway;
import com.bullla.pix.worker.domain.model.pix.PixTransactionStatus;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
class PixRequestedListenerIT extends WorkerIntegrationTestBase {

    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.2"));

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @MockitoBean
    private PixPartnerGateway pixPartnerGateway;

    @Test
    void consumesApprovedEventAndTransitionsTransaction() throws Exception {
        when(pixPartnerGateway.process(any())).thenReturn(PartnerResult.APPROVED);
        insertPixTransaction("tx-listener-1", PixTransactionStatus.PROCESSING);

        kafkaTemplate.send("pix.requested", "tx-listener-1", payload("tx-listener-1")).get(15, TimeUnit.SECONDS);

        awaitStatus("tx-listener-1", "SUCCESS");
        assertThat(statusOf("tx-listener-1")).isEqualTo("SUCCESS");
    }

    @Test
    void dropsUnknownTransactionWithoutBlockingThePartition() throws Exception {
        when(pixPartnerGateway.process(any())).thenReturn(PartnerResult.APPROVED);
        insertPixTransaction("tx-listener-2", PixTransactionStatus.PROCESSING);

        kafkaTemplate.send("pix.requested", "tx-unknown", payload("tx-unknown")).get(15, TimeUnit.SECONDS);
        kafkaTemplate.send("pix.requested", "tx-listener-2", payload("tx-listener-2")).get(15, TimeUnit.SECONDS);

        awaitStatus("tx-listener-2", "SUCCESS");
        assertThat(statusOf("tx-listener-2")).isEqualTo("SUCCESS");
    }

    @Test
    void dropsMalformedPayloadWithoutBlockingThePartition() throws Exception {
        when(pixPartnerGateway.process(any())).thenReturn(PartnerResult.APPROVED);
        insertPixTransaction("tx-listener-3", PixTransactionStatus.PROCESSING);

        kafkaTemplate.send("pix.requested", "tx-listener-3", "not-json{").get(15, TimeUnit.SECONDS);
        kafkaTemplate.send("pix.requested", "tx-listener-3", payload("tx-listener-3")).get(15, TimeUnit.SECONDS);

        awaitStatus("tx-listener-3", "SUCCESS");
        assertThat(statusOf("tx-listener-3")).isEqualTo("SUCCESS");
    }

    private void awaitStatus(String transactionId, String expected) {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        while (Instant.now().isBefore(deadline)) {
            String status = jdbcTemplate.queryForObject(
                    "SELECT status FROM pix_transaction WHERE transaction_id = ?", String.class, transactionId);
            if (expected.equals(status)) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while awaiting status", interrupted);
            }
        }
        throw new AssertionError("Timed out awaiting status " + expected + " for " + transactionId);
    }

    private String payload(String transactionId) {
        return """
                {"eventId":"%s","eventType":"PIX_REQUESTED",\
                "transactionId":"%s","amount":150.75,\
                "pixKey":"cliente@email.com","description":"Pagamento",\
                "occurredAt":"2026-09-29T15:00:00Z"}
                """.formatted(UUID.randomUUID(), transactionId);
    }
}
