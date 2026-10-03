package com.bullla.pix.worker.domain.exception;

import com.bullla.pix.worker.domain.model.pix.PixTransactionStatus;

public class InvalidStatusTransitionException extends RuntimeException {

    public InvalidStatusTransitionException(PixTransactionStatus from, PixTransactionStatus to) {
        super("Invalid PIX transaction status transition from " + from + " to " + to);
    }
}
