package com.bullla.pix.worker.infrastructure.adapter.in.messaging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bullla.pix.worker.application.exception.UnknownPixTransactionException;
import com.bullla.pix.worker.application.usecase.pix.PixRequestedEvent;
import com.bullla.pix.worker.application.usecase.pix.PixRetryRouter;
import com.bullla.pix.worker.application.usecase.pix.ProcessPixTransactionUseCase;
import com.bullla.pix.worker.application.usecase.pix.ProcessOutcome;
import com.bullla.pix.worker.domain.failure.FailureReason;
import com.bullla.pix.worker.domain.model.pix.PixTransactionStatus;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

@ExtendWith(MockitoExtension.class)
class PixRequestedListenerTest {

    private static final String PAYLOAD = """
            {"eventId":"11111111-1111-1111-1111-111111111111","eventType":"PIX_REQUESTED",\
            "transactionId":"tx-1","amount":150.75,"pixKey":"cliente@email.com",\
            "description":"Pagamento","occurredAt":"2026-09-29T15:00:00Z"}
            """;

    @Mock
    private ProcessPixTransactionUseCase processPixTransactionUseCase;

    @Mock
    private PixRetryRouter pixRetryRouter;

    @Mock
    private Acknowledgment acknowledgment;

    private PixRequestedListener listener;

    @BeforeEach
    void setUp() {
        listener = new PixRequestedListener(processPixTransactionUseCase, pixRetryRouter,
                new ObjectMapper().findAndRegisterModules(),
                new com.bullla.pix.worker.infrastructure.metrics.PixWorkerMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry()));
    }

    @Test
    void acknowledgesAfterSuccessfulProcessing() {
        when(processPixTransactionUseCase.execute(any(PixRequestedEvent.class)))
                .thenReturn(ProcessOutcome.processed(PixTransactionStatus.SUCCESS));

        listener.consume(record(PAYLOAD), acknowledgment);

        verify(acknowledgment).acknowledge();
    }

    @Test
    void acknowledgesSkippedMessages() {
        when(processPixTransactionUseCase.execute(any(PixRequestedEvent.class)))
                .thenReturn(ProcessOutcome.skipped());

        listener.consume(record(PAYLOAD), acknowledgment);

        verify(acknowledgment).acknowledge();
    }

    @Test
    void acknowledgesWhenRouterSettlesRetryableOutcome() {
        when(processPixTransactionUseCase.execute(any(PixRequestedEvent.class)))
                .thenReturn(ProcessOutcome.retryable(FailureReason.PARTNER_TIMEOUT));
        when(pixRetryRouter.route(any(), any(), anyInt(), any(), any(), any())).thenReturn(true);

        listener.consume(record(PAYLOAD), acknowledgment);

        verify(acknowledgment).acknowledge();
    }

    @Test
    void doesNotAcknowledgeWhenRouterCannotSettle() {
        when(processPixTransactionUseCase.execute(any(PixRequestedEvent.class)))
                .thenReturn(ProcessOutcome.retryable(FailureReason.PARTNER_TIMEOUT));
        when(pixRetryRouter.route(any(), any(), anyInt(), any(), any(), any())).thenReturn(false);

        listener.consume(record(PAYLOAD), acknowledgment);

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void doesNotAcknowledgeWhenProcessingFails() {
        when(processPixTransactionUseCase.execute(any(PixRequestedEvent.class)))
                .thenThrow(new RuntimeException("transient failure"));

        listener.consume(record(PAYLOAD), acknowledgment);

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void acknowledgesUnknownTransactionsAfterLogging() {
        when(processPixTransactionUseCase.execute(any(PixRequestedEvent.class)))
                .thenThrow(new UnknownPixTransactionException("tx-unknown"));

        listener.consume(record(PAYLOAD), acknowledgment);

        verify(acknowledgment).acknowledge();
    }

    @Test
    void acknowledgesMalformedPayloadsAfterLogging() {
        listener.consume(record("not-json{"), acknowledgment);

        verify(acknowledgment).acknowledge();
    }

    private ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>("pix.requested", 0, 7L, "tx-1", value);
    }
}
