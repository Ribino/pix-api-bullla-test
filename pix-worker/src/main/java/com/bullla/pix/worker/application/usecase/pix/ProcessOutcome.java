package com.bullla.pix.worker.application.usecase.pix;

import com.bullla.pix.worker.domain.failure.FailureReason;
import com.bullla.pix.worker.domain.model.pix.PixTransactionStatus;

public record ProcessOutcome(ProcessStatus status, PixTransactionStatus transactionStatus, FailureReason reason) {

    public static ProcessOutcome processed(PixTransactionStatus transactionStatus) {
        return new ProcessOutcome(ProcessStatus.PROCESSED, transactionStatus, null);
    }

    public static ProcessOutcome skipped() {
        return new ProcessOutcome(ProcessStatus.SKIPPED, null, null);
    }

    public static ProcessOutcome retryable(FailureReason reason) {
        return new ProcessOutcome(ProcessStatus.RETRYABLE, null, reason);
    }
}
