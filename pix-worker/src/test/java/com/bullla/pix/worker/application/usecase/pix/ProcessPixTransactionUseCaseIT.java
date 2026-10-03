package com.bullla.pix.worker.application.usecase.pix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import com.bullla.pix.worker.WorkerIntegrationTestBase;
import com.bullla.pix.worker.application.exception.UnknownPixTransactionException;
import com.bullla.pix.worker.doubles.FakePixPartnerGateway;
import com.bullla.pix.worker.domain.gateway.pix.PartnerException;
import com.bullla.pix.worker.domain.gateway.pix.PartnerResult;
import com.bullla.pix.worker.domain.model.pix.PixTransactionStatus;
import com.bullla.pix.worker.infrastructure.adapter.out.persistence.JdbcPixTransactionRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProcessPixTransactionUseCaseIT extends WorkerIntegrationTestBase {

    private JdbcPixTransactionRepository pixTransactionRepository;
    private FakePixPartnerGateway partnerGateway;
    private ProcessPixTransactionUseCase processPixTransactionUseCase;

    @BeforeEach
    void setUpCollaborators() {
        pixTransactionRepository = new JdbcPixTransactionRepository(jdbcTemplate);
        partnerGateway = new FakePixPartnerGateway(PartnerResult.APPROVED);
        processPixTransactionUseCase =
                new ProcessPixTransactionUseCase(pixTransactionRepository, partnerGateway);
    }

    @Test
    void approvesAndSecondDeliveryDoesNotProcessAgain() {
        insertPixTransaction("tx-1", PixTransactionStatus.PROCESSING);

        ProcessOutcome first = processPixTransactionUseCase.execute(event("tx-1"));
        ProcessOutcome second = processPixTransactionUseCase.execute(event("tx-1"));

        assertThat(first).isEqualTo(ProcessOutcome.processed(PixTransactionStatus.SUCCESS));
        assertThat(second).isEqualTo(ProcessOutcome.skipped());
        assertThat(statusOf("tx-1")).isEqualTo("SUCCESS");
        assertThat(partnerGateway.processedCount()).isEqualTo(1);
    }

    @Test
    void declinesToFailed() {
        partnerGateway = new FakePixPartnerGateway(PartnerResult.DECLINED);
        processPixTransactionUseCase =
                new ProcessPixTransactionUseCase(pixTransactionRepository, partnerGateway);
        insertPixTransaction("tx-1", PixTransactionStatus.PROCESSING);

        ProcessOutcome result = processPixTransactionUseCase.execute(event("tx-1"));

        assertThat(result).isEqualTo(ProcessOutcome.processed(PixTransactionStatus.FAILED));
        assertThat(statusOf("tx-1")).isEqualTo("FAILED");
    }

    @Test
    void throwsForUnknownTransaction() {
        assertThatThrownBy(() -> processPixTransactionUseCase.execute(event("missing")))
                .isInstanceOf(UnknownPixTransactionException.class);
    }

    @Test
    void partnerFailureLeavesTransactionUnchanged() {
        partnerGateway = FakePixPartnerGateway.throwing(new PartnerException("partner down", new RuntimeException()));
        processPixTransactionUseCase =
                new ProcessPixTransactionUseCase(pixTransactionRepository, partnerGateway);
        insertPixTransaction("tx-1", PixTransactionStatus.PROCESSING);

        assertThatThrownBy(() -> processPixTransactionUseCase.execute(event("tx-1")))
                .isInstanceOf(PartnerException.class);
        assertThat(statusOf("tx-1")).isEqualTo("PROCESSING");
    }

    @Test
    void conditionalUpdateRejectsTransitionOfNonProcessingRow() {
        insertPixTransaction("tx-1", PixTransactionStatus.SUCCESS);

        boolean updated = pixTransactionRepository.updateStatusIfProcessing(
                "tx-1", PixTransactionStatus.FAILED, Instant.now());

        assertThat(updated).isFalse();
        assertThat(statusOf("tx-1")).isEqualTo("SUCCESS");
    }

    private PixRequestedEvent event(String transactionId) {
        return new PixRequestedEvent(
                UUID.randomUUID(), "PIX_REQUESTED", transactionId,
                new BigDecimal("150.75"), "cliente@email.com", "Pagamento", Instant.now());
    }
}
