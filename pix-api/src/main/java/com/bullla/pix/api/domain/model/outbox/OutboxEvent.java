package com.bullla.pix.api.domain.model.outbox;

import java.time.Instant;
import java.util.UUID;

public record OutboxEvent(
        UUID id,
        String aggregateId,
        String eventType,
        Object payload,
        OutboxEventStatus status,
        int attempts,
        Instant createdAt,
        Instant publishedAt) {

    public static final String PIX_REQUESTED = "PIX_REQUESTED";

    public static OutboxEvent pendingPixRequested(UUID id, String aggregateId, Object payload, Instant createdAt) {
        return new OutboxEvent(
                id,
                aggregateId,
                PIX_REQUESTED,
                payload,
                OutboxEventStatus.PENDING,
                0,
                createdAt,
                null);
    }
}
