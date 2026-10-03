package com.bullla.pix.worker.domain.messaging;

public interface RetryEventPublisher {

    void sendToRetry(RetryMessage message);

    void sendToDlq(RetryMessage message);
}
