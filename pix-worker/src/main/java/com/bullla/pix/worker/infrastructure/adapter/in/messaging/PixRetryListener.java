package com.bullla.pix.worker.infrastructure.adapter.in.messaging;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class PixRetryListener {

    private static final Logger log = LoggerFactory.getLogger(PixRetryListener.class);
    private static final long REPUBLISH_TIMEOUT_SECONDS = 30;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaListenerEndpointRegistry endpointRegistry;
    private final String mainTopic;
    private final List<Duration> delays;
    private final AtomicReference<Instant> pausedUntil = new AtomicReference<>();

    public PixRetryListener(
            KafkaTemplate<String, String> kafkaTemplate,
            KafkaListenerEndpointRegistry endpointRegistry,
            @Value("${pix.kafka.topic:pix.requested}") String mainTopic,
            @Value("${pix.retry.delays:5s,15s,30s}") List<Duration> delays) {
        this.kafkaTemplate = kafkaTemplate;
        this.endpointRegistry = endpointRegistry;
        this.mainTopic = mainTopic;
        this.delays = delays;
    }

    @KafkaListener(
            id = "pixRetryListener",
            topics = "${pix.retry.topic:pix.retry}",
            groupId = "${pix.retry.group-id:pix-retry}")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment,
            Consumer<?, ?> consumer) {
        RetryHeaders.Parsed headers = RetryHeaders.parse(record.headers());
        Instant dueAt = headers.firstFailureAt().plus(delayFor(headers.retryCount()));
        if (Instant.now().isBefore(dueAt)) {
            consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
            pausedUntil.accumulateAndGet(dueAt,
                    (current, candidate) -> current == null || candidate.isBefore(current) ? candidate : current);
            endpointRegistry.getListenerContainer("pixRetryListener").pause();
            log.info("retry not due yet, rewound and paused transactionId={} asyncRetry={} dueAt={}",
                    record.key(), headers.retryCount(), dueAt);
            return;
        }
        try {
            ProducerRecord<String, String> republish = new ProducerRecord<>(
                    mainTopic, null, record.key(), record.value(),
                    new RecordHeaders(RetryHeaders.of(
                            headers.retryCount(), headers.reason(), headers.firstFailureAt(), mainTopic)));
            kafkaTemplate.send(republish).get(REPUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("retry republished to main topic transactionId={} asyncRetry={}", record.key(), headers.retryCount());
            acknowledgment.acknowledge();
        } catch (Exception exception) {
            log.warn("retry republish failed, will be redelivered transactionId={} error={}",
                    record.key(), exception.toString());
        }
    }

    @Scheduled(fixedRateString = "${pix.retry.pause-check-ms:1000}")
    public void resumeWhenDue() {
        Instant paused = pausedUntil.get();
        if (paused == null || Instant.now().isBefore(paused)) {
            return;
        }
        pausedUntil.set(null);
        endpointRegistry.getListenerContainer("pixRetryListener").resume();
        log.info("retry consumer resumed");
    }

    private Duration delayFor(int retryCount) {
        if (delays.isEmpty()) {
            return Duration.ZERO;
        }
        return delays.get(Math.min(Math.max(retryCount, 1), delays.size()) - 1);
    }
}
