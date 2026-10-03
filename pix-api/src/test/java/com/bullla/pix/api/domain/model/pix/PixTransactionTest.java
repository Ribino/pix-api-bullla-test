package com.bullla.pix.api.domain.model.pix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import com.bullla.pix.api.domain.exception.InvalidStatusTransitionException;

import org.junit.jupiter.api.Test;

class PixTransactionTest {

    @Test
    void transitionsFromProcessingToSuccessUpdatingTimestamp() {
        Instant createdAt = Instant.parse("2026-01-01T00:00:00Z");
        PixTransaction transaction = transaction(createdAt);
        Instant transitionedAt = Instant.parse("2026-01-01T00:05:00Z");

        PixTransaction updated = transaction.transitionTo(PixTransactionStatus.SUCCESS, transitionedAt);

        assertThat(updated.status()).isEqualTo(PixTransactionStatus.SUCCESS);
        assertThat(updated.updatedAt()).isEqualTo(transitionedAt);
        assertThat(updated.createdAt()).isEqualTo(createdAt);
    }

    @Test
    void rejectsInvalidTransition() {
        PixTransaction transaction = transaction(Instant.now())
                .transitionTo(PixTransactionStatus.SUCCESS, Instant.now());

        assertThatThrownBy(() -> transaction.transitionTo(PixTransactionStatus.FAILED, Instant.now()))
                .isInstanceOf(InvalidStatusTransitionException.class);
    }

    private PixTransaction transaction(Instant now) {
        return PixTransaction.processing(
                UUID.randomUUID(),
                "tx-1",
                new BigDecimal("150.75"),
                "cliente@email.com",
                "Pagamento",
                "fingerprint",
                now);
    }
}
