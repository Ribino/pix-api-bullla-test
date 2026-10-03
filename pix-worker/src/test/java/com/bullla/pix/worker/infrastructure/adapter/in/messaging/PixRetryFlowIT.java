package com.bullla.pix.worker.infrastructure.adapter.in.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.bullla.pix.worker.WorkerIntegrationTestBase;
import com.bullla.pix.worker.domain.failure.FailureReason;
import com.bullla.pix.worker.domain.gateway.pix.PartnerResult;
import com.bullla.pix.worker.domain.gateway.pix.PixPartnerGateway;
import com.bullla.pix.worker.domain.gateway.pix.TransientPartnerException;
import com.bullla.pix.worker.domain.model.pix.PixTransactionStatus;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(properties = {
        "pix.retry.delays=1s",
        "pix.retry.pause-check-ms=200",
        "pix.retry.max-retries=2"
})
class PixRetryFlowIT extends WorkerIntegrationTestBase {

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

    @Autowired
    private io.micrometer.core.instrument.MeterRegistry meterRegistry;

    @MockitoBean
    private PixPartnerGateway pixPartnerGateway;

    @Test
    void recoversThroughAsyncRetryAfterTransientFailure() throws Exception {
        when(pixPartnerGateway.process(any()))
                .thenThrow(new TransientPartnerException("down", new RuntimeException(), FailureReason.PARTNER_UNAVAILABLE))
                .thenReturn(PartnerResult.APPROVED);
        insertPixTransaction("tx-retry-ok", PixTransactionStatus.PROCESSING);

        publish("tx-retry-ok");
        awaitStatus("tx-retry-ok", "SUCCESS");

        List<ConsumerRecord<String, String>> retried = consumeKeys("pix.retry", "retry-ok", List.of("tx-retry-ok"));
        assertThat(retried).hasSize(1);
        assertThat(header(retried.get(0), "pix-retry-count")).isEqualTo("1");
        assertThat(statusOf("tx-retry-ok")).isEqualTo("SUCCESS");
    }

    @Test
    void movesToDlqAfterExhaustingAsyncRetries() throws Exception {
        when(pixPartnerGateway.process(any()))
                .thenThrow(new TransientPartnerException("down", new RuntimeException(), FailureReason.PARTNER_TIMEOUT));
        insertPixTransaction("tx-retry-dlq", PixTransactionStatus.PROCESSING);

        publish("tx-retry-dlq");
        List<ConsumerRecord<String, String>> dlq = consumeKeys("pix.dlq", "dlq-ok", List.of("tx-retry-dlq"));

        assertThat(dlq).hasSize(1);
        assertThat(header(dlq.get(0), "pix-retry-count")).isEqualTo("2");
        assertThat(header(dlq.get(0), "pix-failure-reason")).isEqualTo("RETRY_EXHAUSTED");
        assertThat(dlq.get(0).value()).contains("\"transactionId\":\"tx-retry-dlq\"");
        assertThat(statusOf("tx-retry-dlq")).isEqualTo("PROCESSING");
        assertThat(meterRegistry.counter("pix_dlq_messages_total", "reason", "retry_exhausted").count())
                .isGreaterThanOrEqualTo(1);
        assertThat(meterRegistry.counter("pix_async_retries_total").count())
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    void neverRoutesPermanentFailures() throws Exception {
        when(pixPartnerGateway.process(any())).thenReturn(PartnerResult.DECLINED);
        insertPixTransaction("tx-retry-perm", PixTransactionStatus.PROCESSING);

        publish("tx-retry-perm");
        awaitStatus("tx-retry-perm", "FAILED");

        assertThat(consumeKeys("pix.retry", "retry-none", List.of("tx-retry-perm"), Duration.ofSeconds(8))).isEmpty();
        assertThat(consumeKeys("pix.dlq", "dlq-none", List.of("tx-retry-perm"), Duration.ofSeconds(8))).isEmpty();
    }

    private void publish(String transactionId) throws Exception {
        kafkaTemplate.send("pix.requested", transactionId, payload(transactionId)).get(15, TimeUnit.SECONDS);
    }

    private void awaitStatus(String transactionId, String expected) {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(45));
        while (Instant.now().isBefore(deadline)) {
            if (expected.equals(statusOf(transactionId))) {
                return;
            }
            sleep200();
        }
        throw new AssertionError("Timed out awaiting status " + expected + " for " + transactionId);
    }

    private List<ConsumerRecord<String, String>> consumeKeys(String topic, String group, List<String> keys) {
        return consumeKeys(topic, group, keys, Duration.ofSeconds(45));
    }

    private List<ConsumerRecord<String, String>> consumeKeys(
            String topic, String group, List<String> keys, Duration timeout) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, group + "-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> matching = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline)
                    && matching.size() < keys.size()) {
                consumer.poll(Duration.ofSeconds(1)).forEach(record -> {
                    if (keys.contains(record.key()) && matching.stream().noneMatch(found -> found.key().equals(record.key()))) {
                        matching.add(record);
                    }
                });
            }
        }
        return matching;
    }

    private String header(ConsumerRecord<String, String> record, String key) {
        return new String(record.headers().lastHeader(key).value(), java.nio.charset.StandardCharsets.UTF_8);
    }

    private void sleep200() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while awaiting condition", interrupted);
        }
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
