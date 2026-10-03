package com.bullla.pix.api.domain.repository.outbox;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.bullla.pix.api.domain.model.outbox.OutboxEvent;

public interface OutboxEventRepository {

    void save(OutboxEvent event);

    List<OutboxEvent> claimPendingBatch(int limit, Duration stuckAfter);

    void markPublished(UUID eventId, Instant publishedAt);

    void markPending(UUID eventId);
}
