package com.bullla.pix.api.application.usecase.pix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import com.bullla.pix.api.application.exception.TransactionNotFoundException;
import com.bullla.pix.api.domain.model.pix.PixTransaction;
import com.bullla.pix.api.domain.model.pix.PixTransactionStatus;
import com.bullla.pix.api.domain.repository.pix.PixTransactionRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GetPixTransactionUseCaseTest {

    @Mock
    private PixTransactionRepository pixTransactionRepository;

    private GetPixTransactionUseCase getPixTransactionUseCase;

    @BeforeEach
    void setUp() {
        getPixTransactionUseCase = new GetPixTransactionUseCase(pixTransactionRepository);
    }

    @Test
    void returnsTransactionWhenFound() {
        PixTransaction transaction = transaction();
        when(pixTransactionRepository.findByTransactionId("tx-1")).thenReturn(Optional.of(transaction));

        assertThat(getPixTransactionUseCase.execute("tx-1")).isEqualTo(transaction);
    }

    @Test
    void throwsWhenTransactionDoesNotExist() {
        when(pixTransactionRepository.findByTransactionId("missing")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> getPixTransactionUseCase.execute("missing"))
                .isInstanceOf(TransactionNotFoundException.class);
    }

    private PixTransaction transaction() {
        Instant now = Instant.now();
        return new PixTransaction(
                UUID.randomUUID(),
                "tx-1",
                new BigDecimal("150.75"),
                "cliente@email.com",
                "Pagamento",
                PixTransactionStatus.PROCESSING,
                "fingerprint",
                now,
                now);
    }
}
