package com.bullla.pix.api.application.usecase.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.bullla.pix.api.PostgresIntegrationTestBase;
import com.bullla.pix.api.domain.repository.outbox.OutboxEventRepository;
import com.bullla.pix.api.infrastructure.adapter.out.messaging.KafkaOutboxEventPublisher;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

class PublishOutboxEventsUseCaseIT extends PostgresIntegrationTestBase {

    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.2"));

    static {
        KAFKA.start();
        createPixRequestedTopic();
    }

    private static void createPixRequestedTopic() {
        Properties adminConfig = new Properties();
        adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminConfig)) {
            admin.createTopics(List.of(new NewTopic("pix.requested", 3, (short) 1)))
                    .all().get(60, TimeUnit.SECONDS);
            TopicDescription description = admin.describeTopics(List.of("pix.requested"))
                    .topicNameValues().get("pix.requested").get(60, TimeUnit.SECONDS);
            assertThat(description.partitions()).hasSize(3);
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to create pix.requested topic", exception);
        }
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private PublishOutboxEventsUseCase publishOutboxEventsUseCase;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private OutboxPublisherProperties properties;

    @Test
    void publishesPendingEventsToPixRequestedAndMarksThemPublished() {
        UUID firstId = insertPending("tx-a", "{\"transactionId\":\"tx-a\"}");
        UUID secondId = insertPending("tx-b", "{\"transactionId\":\"tx-b\"}");

        PublishOutboxResult result = publishOutboxEventsUseCase.execute();

        assertThat(result).isEqualTo(new PublishOutboxResult(2, 2, 0));

        List<ConsumerRecord<String, String>> records = consume("publish-ok", 2);
        assertThat(records).extracting(ConsumerRecord::topic).containsOnly("pix.requested");
        assertThat(records).extracting(ConsumerRecord::key).containsExactlyInAnyOrder("tx-a", "tx-b");
        assertThat(records).extracting(record -> transactionIdOf(record.value()))
                .containsExactlyInAnyOrder("tx-a", "tx-b");

        assertThat(statusOf(firstId)).isEqualTo("PUBLISHED");
        assertThat(statusOf(secondId)).isEqualTo("PUBLISHED");
        assertThat(attemptsOf(firstId)).isEqualTo(1);
        assertThat(publishedAtOf(firstId)).isNotNull();
    }

    @Test
    void keepsEventAvailableAndCountsAttemptWhenKafkaIsUnavailable() {
        UUID id = insertPending("tx-down", "{\"transactionId\":\"tx-down\"}");
        KafkaTemplate<String, String> deadTemplate = deadBrokerTemplate();
        try {
            PublishOutboxEventsUseCase deadUseCase = new PublishOutboxEventsUseCase(
                    outboxEventRepository,
                    new KafkaOutboxEventPublisher(deadTemplate, properties),
                    properties);

            PublishOutboxResult failed = deadUseCase.execute();

            assertThat(failed).isEqualTo(new PublishOutboxResult(1, 0, 1));
            assertThat(statusOf(id)).isEqualTo("PENDING");
            assertThat(attemptsOf(id)).isEqualTo(1);
            assertThat(publishedAtOf(id)).isNull();
            assertThat(count("outbox_event")).isEqualTo(1);

            PublishOutboxResult recovered = publishOutboxEventsUseCase.execute();

            assertThat(recovered).isEqualTo(new PublishOutboxResult(1, 1, 0));
            assertThat(statusOf(id)).isEqualTo("PUBLISHED");
            List<ConsumerRecord<String, String>> records = consumeKeys("recover-ok", List.of("tx-down"));
            assertThat(records).extracting(ConsumerRecord::key).containsExactly("tx-down");
        } finally {
            deadTemplate.destroy();
        }
    }

    @Test
    void distributesEventsAcrossConcurrentExecutionsWithoutOverlap() throws Exception {
        List<String> expectedKeys = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            String transactionId = "tx-conc-" + i;
            expectedKeys.add(transactionId);
            insertPending(transactionId, "{\"n\":" + i + "}");
        }

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            AtomicReference<PublishOutboxResult> first = new AtomicReference<>();
            AtomicReference<PublishOutboxResult> second = new AtomicReference<>();
            java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
            Future<?> firstFuture = pool.submit(() -> {
                start.await();
                first.set(publishOutboxEventsUseCase.execute());
                return null;
            });
            Future<?> secondFuture = pool.submit(() -> {
                start.await();
                second.set(publishOutboxEventsUseCase.execute());
                return null;
            });
            start.countDown();
            firstFuture.get(60, TimeUnit.SECONDS);
            secondFuture.get(60, TimeUnit.SECONDS);

            assertThat(first.get().failed()).isZero();
            assertThat(second.get().failed()).isZero();
            assertThat(first.get().published() + second.get().published()).isEqualTo(10);
            assertThat(count("outbox_event")).isEqualTo(10);
            List<Integer> attempts = jdbcTemplate.queryForList(
                    "SELECT attempts FROM outbox_event", Integer.class);
            assertThat(attempts).containsOnly(1);

            List<ConsumerRecord<String, String>> records = consumeKeys("concurrent-ok", expectedKeys);
            assertThat(records).extracting(ConsumerRecord::key)
                    .containsExactlyInAnyOrderElementsOf(expectedKeys);
        } finally {
            pool.shutdownNow();
        }
    }

    private String transactionIdOf(String payload) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().readTree(payload).get("transactionId").asText();
        } catch (Exception exception) {
            throw new IllegalStateException("Payload is not valid JSON: " + payload, exception);
        }
    }

    private UUID insertPending(String transactionId, String payload) {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update(
                "INSERT INTO outbox_event (id, aggregate_id, event_type, payload, status, attempts, created_at, published_at)"
                        + " VALUES (?, ?, 'PIX_REQUESTED', CAST(? AS jsonb), 'PENDING', 0, ?, NULL)",
                id, transactionId, payload, OffsetDateTime.now(ZoneOffset.UTC));
        return id;
    }

    private String statusOf(UUID id) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM outbox_event WHERE id = ?", String.class, id);
    }

    private int attemptsOf(UUID id) {
        Integer attempts = jdbcTemplate.queryForObject(
                "SELECT attempts FROM outbox_event WHERE id = ?", Integer.class, id);
        return attempts == null ? 0 : attempts;
    }

    private Object publishedAtOf(UUID id) {
        return jdbcTemplate.queryForMap(
                "SELECT published_at FROM outbox_event WHERE id = ?", id).get("published_at");
    }

    private KafkaTemplate<String, String> deadBrokerTemplate() {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
    }

    private List<ConsumerRecord<String, String>> consume(String groupId, int expected) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of("pix.requested"));
            Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
            while (records.size() < expected && Instant.now().isBefore(deadline)) {
                consumer.poll(Duration.ofSeconds(1)).forEach(records::add);
            }
        }
        assertThat(records).hasSize(expected);
        return records;
    }

    private List<ConsumerRecord<String, String>> consumeKeys(String groupId, List<String> expectedKeys) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> matching = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of("pix.requested"));
            Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
            while (matching.size() < expectedKeys.size() && Instant.now().isBefore(deadline)) {
                consumer.poll(Duration.ofSeconds(1)).forEach(record -> {
                    if (expectedKeys.contains(record.key()) && matching.stream().noneMatch(found -> found.key().equals(record.key()))) {
                        matching.add(record);
                    }
                });
            }
        }
        assertThat(matching).hasSize(expectedKeys.size());
        return matching;
    }
}
