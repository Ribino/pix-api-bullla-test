package com.bullla.pix.api.application.usecase.pix;

import java.math.BigDecimal;

public record CreatePixTransactionCommand(
        String transactionId,
        BigDecimal amount,
        String pixKey,
        String description) {
}
