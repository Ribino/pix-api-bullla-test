package com.bullla.pix.api.application.exception;

public class TransactionNotFoundException extends RuntimeException {

    private final String transactionId;

    public TransactionNotFoundException(String transactionId) {
        super("Transaction " + transactionId + " was not found");
        this.transactionId = transactionId;
    }

    public String getTransactionId() {
        return transactionId;
    }
}
