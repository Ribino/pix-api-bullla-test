package com.bullla.pix.api.application.usecase.pix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import com.bullla.pix.api.application.exception.DuplicateTransactionConflictException;
import com.bullla.pix.api.domain.model.outbox.OutboxEvent;
import com.bullla.pix.api.domain.model.pix.PixTransaction;
import com.bullla.pix.api.domain.model.pix.PixTransactionStatus;
import com.bullla.pix.api.domain.repository.outbox.OutboxEventRepository;
import com.bullla.pix.api.domain.repository.pix.PixTransactionRepository;
import com.bullla.pix.api.domain.service.RequestFingerprint;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CreatePixTransactionUseCaseTest {

    private static final CreatePixTransactionCommand COMMAND =
            new CreatePixTransactionCommand("tx-1", new BigDecimal("150.75"), "cliente@email.com", "Pagamento");

    @Mock
    private PixTransactionRepository pixTransactionRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    private final RequestFingerprint requestFingerprint = new RequestFingerprint();

    private CreatePixTransactionUseCase createPixTransactionUseCase;

    @BeforeEach
    void setUp() {
        createPixTransactionUseCase = new CreatePixTransactionUseCase(
                pixTransactionRepository, outboxEventRepository, requestFingerprint);
    }

    @Test
    void createsTransactionAndOutboxEventWhenTransactionIdIsNew() {
        when(pixTransactionRepository.findByTransactionId("tx-1")).thenReturn(Optional.empty());
        when(pixTransactionRepository.insertIfAbsent(any(PixTransaction.class))).thenReturn(true);

        PixTransactionResult result = createPixTransactionUseCase.execute(COMMAND);

        assertThat(result.created()).isTrue();
        assertThat(result.transaction().status()).isEqualTo(PixTransactionStatus.PROCESSING);
        assertThat(result.transaction().requestFingerprint()).isEqualTo(fingerprintFor(COMMAND));
        verify(outboxEventRepository).save(any(OutboxEvent.class));
    }

    @Test
    void returnsExistingTransactionWhenFingerprintMatches() {
        PixTransaction existing = transactionWithFingerprint(fingerprintFor(COMMAND));
        when(pixTransactionRepository.findByTransactionId("tx-1")).thenReturn(Optional.of(existing));

        PixTransactionResult result = createPixTransactionUseCase.execute(COMMAND);

        assertThat(result.created()).isFalse();
        assertThat(result.transaction()).isEqualTo(existing);
        verify(pixTransactionRepository, never()).insertIfAbsent(any(PixTransaction.class));
        verifyNoInteractions(outboxEventRepository);
    }

    @Test
    void throwsConflictWhenFingerprintDiffers() {
        PixTransaction existing = transactionWithFingerprint(
                requestFingerprint.generate("tx-1", new BigDecimal("999.99"), "cliente@email.com", "Pagamento"));
        when(pixTransactionRepository.findByTransactionId("tx-1")).thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> createPixTransactionUseCase.execute(COMMAND))
                .isInstanceOf(DuplicateTransactionConflictException.class);

        verify(pixTransactionRepository, never()).insertIfAbsent(any(PixTransaction.class));
        verifyNoInteractions(outboxEventRepository);
    }

    @Test
    void resolvesTransactionCreatedConcurrentlyWithSameFingerprint() {
        PixTransaction concurrent = transactionWithFingerprint(fingerprintFor(COMMAND));
        when(pixTransactionRepository.findByTransactionId("tx-1"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(concurrent));
        when(pixTransactionRepository.insertIfAbsent(any(PixTransaction.class))).thenReturn(false);

        PixTransactionResult result = createPixTransactionUseCase.execute(COMMAND);

        assertThat(result.created()).isFalse();
        assertThat(result.transaction()).isEqualTo(concurrent);
        verifyNoInteractions(outboxEventRepository);
    }

    private String fingerprintFor(CreatePixTransactionCommand command) {
        return requestFingerprint.generate(
                command.transactionId(), command.amount(), command.pixKey(), command.description());
    }

    private PixTransaction transactionWithFingerprint(String fingerprint) {
        Instant now = Instant.now();
        return new PixTransaction(
                UUID.randomUUID(),
                "tx-1",
                new BigDecimal("150.75"),
                "cliente@email.com",
                "Pagamento",
                PixTransactionStatus.PROCESSING,
                fingerprint,
                now,
                now);
    }
}
