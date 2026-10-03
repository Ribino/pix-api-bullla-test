package com.bullla.pix.api.application.usecase.outbox;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.bullla.pix.api.domain.model.outbox.OutboxEvent;
import com.bullla.pix.api.domain.repository.outbox.OutboxEventRepository;
import com.bullla.pix.api.domain.service.OutboxEventPublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class PublishOutboxEventsUseCase {

    private static final Logger log = LoggerFactory.getLogger(PublishOutboxEventsUseCase.class);

    private final OutboxEventRepository outboxEventRepository;
    private final OutboxEventPublisher outboxEventPublisher;
    private final OutboxPublisherProperties properties;

    public PublishOutboxEventsUseCase(
            OutboxEventRepository outboxEventRepository,
            OutboxEventPublisher outboxEventPublisher,
            OutboxPublisherProperties properties) {
        this.outboxEventRepository = outboxEventRepository;
        this.outboxEventPublisher = outboxEventPublisher;
        this.properties = properties;
    }

    public PublishOutboxResult execute() {
        List<OutboxEvent> batch = outboxEventRepository.claimPendingBatch(
                properties.getBatchSize(), Duration.ofMinutes(properties.getStuckAfterMinutes()));

        int published = 0;
        int failed = 0;
        for (OutboxEvent event : batch) {
            log.info("outbox event selected eventId={} transactionId={}", event.id(), event.aggregateId());
            try {
                log.info("publishing event eventId={} transactionId={}", event.id(), event.aggregateId());
                outboxEventPublisher.publish(event);
                outboxEventRepository.markPublished(event.id(), Instant.now());
                log.info("event published eventId={} transactionId={}", event.id(), event.aggregateId());
                published++;
            } catch (Exception exception) {
                log.warn("event publication failed eventId={} transactionId={} error={}",
                        event.id(), event.aggregateId(), exception.toString());
                requeue(event);
                failed++;
            }
        }
        return new PublishOutboxResult(batch.size(), published, failed);
    }

    private void requeue(OutboxEvent event) {
        try {
            outboxEventRepository.markPending(event.id());
        } catch (Exception exception) {
            log.error("failed to requeue outbox event eventId={} transactionId={} error={}",
                    event.id(), event.aggregateId(), exception.toString());
        }
    }
}
