package com.bullla.pix.worker.domain.messaging;

import java.time.Instant;

import com.bullla.pix.worker.domain.failure.FailureReason;

public record RetryMessage(
        String transactionId,
        String payload,
        int retryCount,
        FailureReason reason,
        Instant firstFailureAt,
        String originalTopic) {
}
