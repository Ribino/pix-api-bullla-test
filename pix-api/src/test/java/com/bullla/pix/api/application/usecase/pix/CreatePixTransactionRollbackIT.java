package com.bullla.pix.api.application.usecase.pix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

import java.math.BigDecimal;

import com.bullla.pix.api.PostgresIntegrationTestBase;
import com.bullla.pix.api.domain.model.outbox.OutboxEvent;
import com.bullla.pix.api.domain.repository.outbox.OutboxEventRepository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

class CreatePixTransactionRollbackIT extends PostgresIntegrationTestBase {

    @Autowired
    private CreatePixTransactionUseCase createPixTransactionUseCase;

    @MockitoBean
    private OutboxEventRepository outboxEventRepository;

    @Test
    void rollsBackTheTransactionWhenOutboxPersistenceFails() {
        doThrow(new IllegalStateException("simulated outbox failure"))
                .when(outboxEventRepository).save(any(OutboxEvent.class));

        assertThatThrownBy(() -> createPixTransactionUseCase.execute(new CreatePixTransactionCommand(
                "tx-rollback", new BigDecimal("10.00"), "cliente@email.com", "Pagamento")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("simulated outbox failure");

        assertThat(count("pix_transaction")).isZero();
        assertThat(count("outbox_event")).isZero();
    }
}
