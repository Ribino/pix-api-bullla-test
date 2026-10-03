package com.bullla.pix.api.application.usecase.pix;

import com.bullla.pix.api.domain.model.pix.PixTransaction;

public record PixTransactionResult(PixTransaction transaction, boolean created) {

    public static PixTransactionResult created(PixTransaction transaction) {
        return new PixTransactionResult(transaction, true);
    }

    public static PixTransactionResult existing(PixTransaction transaction) {
        return new PixTransactionResult(transaction, false);
    }
}
