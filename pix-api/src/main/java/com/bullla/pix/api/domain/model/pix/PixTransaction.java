package com.bullla.pix.api.domain.model.pix;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import com.bullla.pix.api.domain.exception.InvalidStatusTransitionException;

public record PixTransaction(
        UUID id,
        String transactionId,
        BigDecimal amount,
        String pixKey,
        String description,
        PixTransactionStatus status,
        String requestFingerprint,
        Instant createdAt,
        Instant updatedAt) {

    public static PixTransaction processing(
            UUID id,
            String transactionId,
            BigDecimal amount,
            String pixKey,
            String description,
            String requestFingerprint,
            Instant now) {
        return new PixTransaction(
                id,
                transactionId,
                amount,
                pixKey,
                description,
                PixTransactionStatus.PROCESSING,
                requestFingerprint,
                now,
                now);
    }

    public PixTransaction transitionTo(PixTransactionStatus target, Instant now) {
        if (!status.canTransitionTo(target)) {
            throw new InvalidStatusTransitionException(status, target);
        }
        return new PixTransaction(
                id,
                transactionId,
                amount,
                pixKey,
                description,
                target,
                requestFingerprint,
                createdAt,
                now);
    }
}
