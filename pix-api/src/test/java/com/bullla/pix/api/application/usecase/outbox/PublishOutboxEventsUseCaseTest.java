package com.bullla.pix.api.application.usecase.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.bullla.pix.api.application.exception.EventPublicationException;
import com.bullla.pix.api.domain.model.outbox.OutboxEvent;
import com.bullla.pix.api.domain.model.outbox.OutboxEventStatus;
import com.bullla.pix.api.domain.repository.outbox.OutboxEventRepository;
import com.bullla.pix.api.domain.service.OutboxEventPublisher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PublishOutboxEventsUseCaseTest {

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private OutboxEventPublisher outboxEventPublisher;

    private OutboxPublisherProperties properties;
    private PublishOutboxEventsUseCase publishOutboxEventsUseCase;

    @BeforeEach
    void setUp() {
        properties = new OutboxPublisherProperties();
        properties.setBatchSize(100);
        properties.setStuckAfterMinutes(5);
        publishOutboxEventsUseCase = new PublishOutboxEventsUseCase(
                outboxEventRepository, outboxEventPublisher, properties);
    }

    @Test
    void publishesClaimedBatchAndMarksEachEventPublished() {
        OutboxEvent first = pendingEvent("tx-1");
        OutboxEvent second = pendingEvent("tx-2");
        when(outboxEventRepository.claimPendingBatch(100, Duration.ofMinutes(5)))
                .thenReturn(List.of(first, second));

        PublishOutboxResult result = publishOutboxEventsUseCase.execute();

        assertThat(result).isEqualTo(new PublishOutboxResult(2, 2, 0));
        verify(outboxEventPublisher).publish(first);
        verify(outboxEventPublisher).publish(second);
        verify(outboxEventRepository).markPublished(eq(first.id()), any(Instant.class));
        verify(outboxEventRepository).markPublished(eq(second.id()), any(Instant.class));
        verify(outboxEventRepository, never()).markPending(any(UUID.class));
    }

    @Test
    void requeuesFailedEventAndContinuesWithTheRestOfTheBatch() {
        OutboxEvent failing = pendingEvent("tx-1");
        OutboxEvent succeeding = pendingEvent("tx-2");
        when(outboxEventRepository.claimPendingBatch(100, Duration.ofMinutes(5)))
                .thenReturn(List.of(failing, succeeding));
        doThrow(new EventPublicationException("kafka down", new RuntimeException("boom")))
                .when(outboxEventPublisher).publish(failing);

        PublishOutboxResult result = publishOutboxEventsUseCase.execute();

        assertThat(result).isEqualTo(new PublishOutboxResult(2, 1, 1));
        verify(outboxEventRepository).markPending(failing.id());
        verify(outboxEventRepository, never()).markPublished(eq(failing.id()), any(Instant.class));
        verify(outboxEventPublisher).publish(succeeding);
        verify(outboxEventRepository).markPublished(eq(succeeding.id()), any(Instant.class));
    }

    @Test
    void doesNothingWhenThereIsNothingPending() {
        when(outboxEventRepository.claimPendingBatch(100, Duration.ofMinutes(5))).thenReturn(List.of());

        PublishOutboxResult result = publishOutboxEventsUseCase.execute();

        assertThat(result).isEqualTo(new PublishOutboxResult(0, 0, 0));
        verifyNoInteractions(outboxEventPublisher);
        verify(outboxEventRepository, never()).markPublished(any(UUID.class), any(Instant.class));
        verify(outboxEventRepository, never()).markPending(any(UUID.class));
    }

    private OutboxEvent pendingEvent(String transactionId) {
        return new OutboxEvent(
                UUID.randomUUID(),
                transactionId,
                OutboxEvent.PIX_REQUESTED,
                "{\"transactionId\":\"" + transactionId + "\"}",
                OutboxEventStatus.PENDING,
                0,
                Instant.now(),
                null);
    }
}
