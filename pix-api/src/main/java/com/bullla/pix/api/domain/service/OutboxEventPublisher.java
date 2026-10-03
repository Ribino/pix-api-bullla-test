package com.bullla.pix.api.domain.service;

import com.bullla.pix.api.domain.model.outbox.OutboxEvent;

public interface OutboxEventPublisher {

    void publish(OutboxEvent event);
}
