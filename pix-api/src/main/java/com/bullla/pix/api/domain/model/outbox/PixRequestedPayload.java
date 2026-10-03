package com.bullla.pix.api.domain.model.outbox;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PixRequestedPayload(
        UUID eventId,
        String eventType,
        String transactionId,
        BigDecimal amount,
        String pixKey,
        String description,
        Instant occurredAt) {
}
