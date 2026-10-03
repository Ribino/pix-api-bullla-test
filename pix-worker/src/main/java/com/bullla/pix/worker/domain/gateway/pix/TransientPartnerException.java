package com.bullla.pix.worker.domain.gateway.pix;

import com.bullla.pix.worker.domain.failure.FailureReason;

public class TransientPartnerException extends PartnerException {

    private final FailureReason reason;

    public TransientPartnerException(String message, Throwable cause) {
        this(message, cause, FailureReason.UNKNOWN_PARTNER_ERROR);
    }

    public TransientPartnerException(String message, Throwable cause, FailureReason reason) {
        super(message, cause);
        this.reason = reason;
    }

    public FailureReason getReason() {
        return reason;
    }
}
