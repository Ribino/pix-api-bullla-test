package com.bullla.pix.worker.domain.failure;

public enum FailureReason {
    PARTNER_TIMEOUT,
    PARTNER_UNAVAILABLE,
    PARTNER_CONNECTION_ERROR,
    RETRY_EXHAUSTED,
    UNKNOWN_PARTNER_ERROR
}
