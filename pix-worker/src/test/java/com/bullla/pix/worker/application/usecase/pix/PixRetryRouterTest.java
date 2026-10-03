package com.bullla.pix.worker.application.usecase.pix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import java.time.Instant;

import com.bullla.pix.worker.domain.failure.FailureReason;
import com.bullla.pix.worker.domain.messaging.RetryEventPublisher;
import com.bullla.pix.worker.domain.messaging.RetryMessage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PixRetryRouterTest {

    private static final Instant FIRST_FAILURE_AT = Instant.parse("2026-09-29T15:00:00Z");

    @Mock
    private RetryEventPublisher retryEventPublisher;

    private PixRetryRouter router;

    @BeforeEach
    void setUp() {
        router = new PixRetryRouter(retryEventPublisher, 3);
    }

    @Test
    void sendsToRetryWithIncrementedCountWhileBelowLimit() {
        boolean settled = router.route("tx-1", "{\"a\":1}", 1,
                FailureReason.PARTNER_TIMEOUT, FIRST_FAILURE_AT, "pix.requested");

        assertThat(settled).isTrue();
        ArgumentCaptor<RetryMessage> message = ArgumentCaptor.forClass(RetryMessage.class);
        verify(retryEventPublisher).sendToRetry(message.capture());
        assertThat(message.getValue()).isEqualTo(new RetryMessage(
                "tx-1", "{\"a\":1}", 2, FailureReason.PARTNER_TIMEOUT, FIRST_FAILURE_AT, "pix.requested"));
    }

    @Test
    void sendsToDlqOnceLimitIsExceeded() {
        boolean settled = router.route("tx-1", "{\"a\":1}", 3,
                FailureReason.PARTNER_UNAVAILABLE, FIRST_FAILURE_AT, "pix.requested");

        assertThat(settled).isTrue();
        ArgumentCaptor<RetryMessage> message = ArgumentCaptor.forClass(RetryMessage.class);
        verify(retryEventPublisher).sendToDlq(message.capture());
        assertThat(message.getValue()).isEqualTo(new RetryMessage(
                "tx-1", "{\"a\":1}", 3, FailureReason.RETRY_EXHAUSTED, FIRST_FAILURE_AT, "pix.requested"));
    }

    @Test
    void preservesPayloadAndTransactionIdOnEveryRoute() {
        router.route("tx-9", "payload", 0, FailureReason.PARTNER_TIMEOUT, FIRST_FAILURE_AT, "pix.requested");

        ArgumentCaptor<RetryMessage> message = ArgumentCaptor.forClass(RetryMessage.class);
        verify(retryEventPublisher).sendToRetry(message.capture());
        assertThat(message.getValue().transactionId()).isEqualTo("tx-9");
        assertThat(message.getValue().payload()).isEqualTo("payload");
    }
}
