package com.bullla.pix.worker.application.usecase.pix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import com.bullla.pix.worker.application.exception.UnknownPixTransactionException;
import com.bullla.pix.worker.doubles.FakePixPartnerGateway;
import com.bullla.pix.worker.domain.failure.FailureReason;
import com.bullla.pix.worker.domain.gateway.pix.PartnerException;
import com.bullla.pix.worker.domain.gateway.pix.TransientPartnerException;
import com.bullla.pix.worker.domain.gateway.pix.PartnerResult;
import com.bullla.pix.worker.domain.model.pix.PixTransaction;
import com.bullla.pix.worker.domain.model.pix.PixTransactionStatus;
import com.bullla.pix.worker.domain.repository.pix.PixTransactionRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProcessPixTransactionUseCaseTest {

    @Mock
    private PixTransactionRepository pixTransactionRepository;

    private FakePixPartnerGateway partnerGateway;
    private ProcessPixTransactionUseCase processPixTransactionUseCase;

    @BeforeEach
    void setUp() {
        partnerGateway = new FakePixPartnerGateway(PartnerResult.APPROVED);
        processPixTransactionUseCase = new ProcessPixTransactionUseCase(pixTransactionRepository, partnerGateway);
    }

    @Test
    void transitionsProcessingToSuccessWhenPartnerApproves() {
        PixTransaction transaction = transaction(PixTransactionStatus.PROCESSING);
        when(pixTransactionRepository.findByTransactionId("tx-1")).thenReturn(Optional.of(transaction));
        when(pixTransactionRepository.updateStatusIfProcessing(
                eq("tx-1"), eq(PixTransactionStatus.SUCCESS), any(Instant.class))).thenReturn(true);

        ProcessOutcome result = processPixTransactionUseCase.execute(event("tx-1"));

        assertThat(result).isEqualTo(ProcessOutcome.processed(PixTransactionStatus.SUCCESS));
        assertThat(partnerGateway.processedCount()).isEqualTo(1);
        verify(pixTransactionRepository).updateStatusIfProcessing(
                eq("tx-1"), eq(PixTransactionStatus.SUCCESS), any(Instant.class));
    }

    @Test
    void transitionsProcessingToFailedWhenPartnerDeclines() {
        partnerGateway = new FakePixPartnerGateway(PartnerResult.DECLINED);
        processPixTransactionUseCase = new ProcessPixTransactionUseCase(pixTransactionRepository, partnerGateway);
        PixTransaction transaction = transaction(PixTransactionStatus.PROCESSING);
        when(pixTransactionRepository.findByTransactionId("tx-1")).thenReturn(Optional.of(transaction));
        when(pixTransactionRepository.updateStatusIfProcessing(
                eq("tx-1"), eq(PixTransactionStatus.FAILED), any(Instant.class))).thenReturn(true);

        ProcessOutcome result = processPixTransactionUseCase.execute(event("tx-1"));

        assertThat(result).isEqualTo(ProcessOutcome.processed(PixTransactionStatus.FAILED));
        verify(pixTransactionRepository).updateStatusIfProcessing(
                eq("tx-1"), eq(PixTransactionStatus.FAILED), any(Instant.class));
    }

    @Test
    void skipsTransactionAlreadySuccessful() {
        PixTransaction transaction = transaction(PixTransactionStatus.SUCCESS);
        when(pixTransactionRepository.findByTransactionId("tx-1")).thenReturn(Optional.of(transaction));

        ProcessOutcome result = processPixTransactionUseCase.execute(event("tx-1"));

        assertThat(result).isEqualTo(ProcessOutcome.skipped());
        assertThat(partnerGateway.processedCount()).isZero();
        verify(pixTransactionRepository, never()).updateStatusIfProcessing(
                any(String.class), any(PixTransactionStatus.class), any(Instant.class));
    }

    @Test
    void skipsTransactionAlreadyFailed() {
        PixTransaction transaction = transaction(PixTransactionStatus.FAILED);
        when(pixTransactionRepository.findByTransactionId("tx-1")).thenReturn(Optional.of(transaction));

        ProcessOutcome result = processPixTransactionUseCase.execute(event("tx-1"));

        assertThat(result).isEqualTo(ProcessOutcome.skipped());
        assertThat(partnerGateway.processedCount()).isZero();
    }

    @Test
    void throwsWhenTransactionDoesNotExist() {
        when(pixTransactionRepository.findByTransactionId("missing")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> processPixTransactionUseCase.execute(event("missing")))
                .isInstanceOf(UnknownPixTransactionException.class);
        assertThat(partnerGateway.processedCount()).isZero();
    }

    @Test
    void propagatesPartnerFailureWithoutChangingState() {
        partnerGateway = FakePixPartnerGateway.throwing(new PartnerException("partner down", new RuntimeException()));
        processPixTransactionUseCase = new ProcessPixTransactionUseCase(pixTransactionRepository, partnerGateway);
        PixTransaction transaction = transaction(PixTransactionStatus.PROCESSING);
        when(pixTransactionRepository.findByTransactionId("tx-1")).thenReturn(Optional.of(transaction));

        assertThatThrownBy(() -> processPixTransactionUseCase.execute(event("tx-1")))
                .isInstanceOf(PartnerException.class);
        verify(pixTransactionRepository, never()).updateStatusIfProcessing(
                any(String.class), any(PixTransactionStatus.class), any(Instant.class));
    }

    @Test
    void returnsRetryableWhenPartnerFailureIsTransient() {
        partnerGateway = FakePixPartnerGateway.throwing(new TransientPartnerException(
                "partner down", new RuntimeException(), FailureReason.PARTNER_UNAVAILABLE));
        processPixTransactionUseCase = new ProcessPixTransactionUseCase(pixTransactionRepository, partnerGateway);
        PixTransaction transaction = transaction(PixTransactionStatus.PROCESSING);
        when(pixTransactionRepository.findByTransactionId("tx-1")).thenReturn(Optional.of(transaction));

        ProcessOutcome result = processPixTransactionUseCase.execute(event("tx-1"));

        assertThat(result).isEqualTo(ProcessOutcome.retryable(FailureReason.PARTNER_UNAVAILABLE));
        verify(pixTransactionRepository, never()).updateStatusIfProcessing(
                any(String.class), any(PixTransactionStatus.class), any(Instant.class));
    }

    @Test
    void skipsWhenConcurrentTransitionAlreadyHappened() {
        PixTransaction transaction = transaction(PixTransactionStatus.PROCESSING);
        when(pixTransactionRepository.findByTransactionId("tx-1")).thenReturn(Optional.of(transaction));
        when(pixTransactionRepository.updateStatusIfProcessing(
                eq("tx-1"), eq(PixTransactionStatus.SUCCESS), any(Instant.class))).thenReturn(false);

        ProcessOutcome result = processPixTransactionUseCase.execute(event("tx-1"));

        assertThat(result).isEqualTo(ProcessOutcome.skipped());
        assertThat(partnerGateway.processedCount()).isEqualTo(1);
    }

    private PixRequestedEvent event(String transactionId) {
        return new PixRequestedEvent(
                UUID.randomUUID(), "PIX_REQUESTED", transactionId,
                new BigDecimal("150.75"), "cliente@email.com", "Pagamento", Instant.now());
    }

    private PixTransaction transaction(PixTransactionStatus status) {
        Instant now = Instant.now();
        return new PixTransaction(
                UUID.randomUUID(), "tx-1", new BigDecimal("150.75"), "cliente@email.com",
                "Pagamento", status, "fingerprint", now, now);
    }
}
