package com.bullla.pix.worker.application.usecase.pix;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PixRequestedEvent(
        UUID eventId,
        String eventType,
        String transactionId,
        BigDecimal amount,
        String pixKey,
        String description,
        Instant occurredAt) {
}
