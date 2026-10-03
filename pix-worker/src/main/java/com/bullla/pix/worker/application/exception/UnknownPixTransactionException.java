package com.bullla.pix.worker.application.exception;

public class UnknownPixTransactionException extends RuntimeException {

    private final String transactionId;

    public UnknownPixTransactionException(String transactionId) {
        super("No PIX transaction found for transactionId " + transactionId);
        this.transactionId = transactionId;
    }

    public String getTransactionId() {
        return transactionId;
    }
}
