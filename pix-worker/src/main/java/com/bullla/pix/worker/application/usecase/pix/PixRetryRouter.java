package com.bullla.pix.worker.application.usecase.pix;

import java.time.Instant;

import com.bullla.pix.worker.domain.failure.FailureReason;
import com.bullla.pix.worker.domain.messaging.RetryEventPublisher;
import com.bullla.pix.worker.domain.messaging.RetryMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class PixRetryRouter {

    private static final Logger log = LoggerFactory.getLogger(PixRetryRouter.class);

    private final RetryEventPublisher retryEventPublisher;
    private final int maxRetries;

    public PixRetryRouter(
            RetryEventPublisher retryEventPublisher,
            @Value("${pix.retry.max-retries:3}") int maxRetries) {
        this.retryEventPublisher = retryEventPublisher;
        this.maxRetries = maxRetries;
    }

    public boolean route(String transactionId, String payload, int retryCount,
            FailureReason reason, Instant firstFailureAt, String originalTopic) {
        int nextRetryCount = retryCount + 1;
        if (nextRetryCount > maxRetries) {
            retryEventPublisher.sendToDlq(new RetryMessage(
                    transactionId, payload, retryCount, FailureReason.RETRY_EXHAUSTED, firstFailureAt, originalTopic));
            log.warn("event moved to dlq transactionId={} asyncRetries={} lastReason={}",
                    transactionId, retryCount, reason);
            return true;
        }
        retryEventPublisher.sendToRetry(new RetryMessage(
                transactionId, payload, nextRetryCount, reason, firstFailureAt, originalTopic));
        log.info("event scheduled for async retry transactionId={} asyncRetry={} reason={}",
                transactionId, nextRetryCount, reason);
        return true;
    }
}
