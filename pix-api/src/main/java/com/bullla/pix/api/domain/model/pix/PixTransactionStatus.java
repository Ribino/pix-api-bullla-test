package com.bullla.pix.api.domain.model.pix;

public enum PixTransactionStatus {
    PROCESSING,
    SUCCESS,
    FAILED;

    public boolean canTransitionTo(PixTransactionStatus target) {
        return switch (this) {
            case PROCESSING -> target == SUCCESS || target == FAILED;
            case SUCCESS, FAILED -> false;
        };
    }
}
