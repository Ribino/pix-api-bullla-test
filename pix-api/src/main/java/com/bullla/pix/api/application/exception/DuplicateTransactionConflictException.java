package com.bullla.pix.api.application.exception;

public class DuplicateTransactionConflictException extends RuntimeException {

    private final String transactionId;

    public DuplicateTransactionConflictException(String transactionId) {
        super("Transaction " + transactionId + " already exists with a different request fingerprint");
        this.transactionId = transactionId;
    }

    public String getTransactionId() {
        return transactionId;
    }
}
