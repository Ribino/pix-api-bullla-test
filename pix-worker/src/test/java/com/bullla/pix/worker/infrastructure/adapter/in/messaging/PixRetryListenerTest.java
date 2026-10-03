package com.bullla.pix.worker.infrastructure.adapter.in.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;

@ExtendWith(MockitoExtension.class)
class PixRetryListenerTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private KafkaListenerEndpointRegistry endpointRegistry;

    @Mock
    private MessageListenerContainer retryContainer;

    @Mock
    private Acknowledgment acknowledgment;

    @Mock
    private Consumer<String, String> consumer;

    private PixRetryListener listener;

    @BeforeEach
    void setUp() {
        listener = new PixRetryListener(kafkaTemplate, endpointRegistry,
                "pix.requested", List.of(Duration.ofSeconds(30)));
        lenient().when(endpointRegistry.getListenerContainer("pixRetryListener")).thenReturn(retryContainer);
        lenient().when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void republishesDueRetryToMainTopicPreservingHeaders() {
        ConsumerRecord<String, String> record = retryRecord(1,
                Instant.now().minus(Duration.ofMinutes(5)));

        listener.consume(record, acknowledgment, consumer);

        org.mockito.ArgumentCaptor<ProducerRecord<String, String>> sent =
                org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(sent.capture());
        assertThat(sent.getValue().topic()).isEqualTo("pix.requested");
        assertThat(sent.getValue().key()).isEqualTo("tx-1");
        assertThat(sent.getValue().value()).isEqualTo("{\"a\":1}");
        assertThat(header(sent.getValue(), "pix-retry-count")).isEqualTo("1");
        verify(acknowledgment).acknowledge();
    }

    @Test
    void pausesConsumerWhenRetryIsNotDueYet() {
        ConsumerRecord<String, String> record = retryRecord(1, Instant.now().plus(Duration.ofMinutes(5)));

        listener.consume(record, acknowledgment, consumer);

        verify(retryContainer).pause();
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void doesNotAcknowledgeWhenRepublishFails() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));
        ConsumerRecord<String, String> record = retryRecord(1,
                Instant.now().minus(Duration.ofMinutes(5)));

        listener.consume(record, acknowledgment, consumer);

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void doesNotResumeWhilePausedUntilFuture() {
        ConsumerRecord<String, String> record = retryRecord(1, Instant.now().plus(Duration.ofMinutes(5)));
        listener.consume(record, acknowledgment, consumer);

        listener.resumeWhenDue();

        verify(retryContainer, never()).resume();
    }

    private ConsumerRecord<String, String> retryRecord(int retryCount, Instant firstFailureAt) {
        Header[] headers = RetryHeaders.of(retryCount,
                        com.bullla.pix.worker.domain.failure.FailureReason.PARTNER_TIMEOUT,
                        firstFailureAt, "pix.requested")
                .toArray(new Header[0]);
        return new ConsumerRecord<String, String>("pix.retry", 0, 11L,
                System.currentTimeMillis(), org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                0, 0, "tx-1", "{\"a\":1}", new RecordHeaders(headers), java.util.Optional.empty());
    }

    private String header(ProducerRecord<String, String> record, String key) {
        return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
    }
}
