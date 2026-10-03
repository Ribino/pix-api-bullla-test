package com.bullla.pix.api.domain.model.pix;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PixTransactionStatusTest {

    @Test
    void allowsProcessingToSuccessOrFailed() {
        assertThat(PixTransactionStatus.PROCESSING.canTransitionTo(PixTransactionStatus.SUCCESS)).isTrue();
        assertThat(PixTransactionStatus.PROCESSING.canTransitionTo(PixTransactionStatus.FAILED)).isTrue();
    }

    @Test
    void rejectsTransitionsFromTerminalStates() {
        assertThat(PixTransactionStatus.SUCCESS.canTransitionTo(PixTransactionStatus.PROCESSING)).isFalse();
        assertThat(PixTransactionStatus.SUCCESS.canTransitionTo(PixTransactionStatus.FAILED)).isFalse();
        assertThat(PixTransactionStatus.FAILED.canTransitionTo(PixTransactionStatus.PROCESSING)).isFalse();
        assertThat(PixTransactionStatus.FAILED.canTransitionTo(PixTransactionStatus.SUCCESS)).isFalse();
    }

    @Test
    void rejectsTransitionToSameState() {
        assertThat(PixTransactionStatus.PROCESSING.canTransitionTo(PixTransactionStatus.PROCESSING)).isFalse();
        assertThat(PixTransactionStatus.SUCCESS.canTransitionTo(PixTransactionStatus.SUCCESS)).isFalse();
        assertThat(PixTransactionStatus.FAILED.canTransitionTo(PixTransactionStatus.FAILED)).isFalse();
    }
}
