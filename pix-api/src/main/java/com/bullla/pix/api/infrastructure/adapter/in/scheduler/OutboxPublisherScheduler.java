package com.bullla.pix.api.infrastructure.adapter.in.scheduler;

import com.bullla.pix.api.application.usecase.outbox.PublishOutboxEventsUseCase;
import com.bullla.pix.api.application.usecase.outbox.PublishOutboxResult;
import com.bullla.pix.api.infrastructure.metrics.PixApiMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "outbox.publisher", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisherScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherScheduler.class);

    private final PublishOutboxEventsUseCase publishOutboxEventsUseCase;
    private final PixApiMetrics metrics;

    public OutboxPublisherScheduler(PublishOutboxEventsUseCase publishOutboxEventsUseCase, PixApiMetrics metrics) {
        this.publishOutboxEventsUseCase = publishOutboxEventsUseCase;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:5000}")
    public void poll() {
        PublishOutboxResult result = publishOutboxEventsUseCase.execute();
        metrics.countOutboxPublished(result.published());
        metrics.countOutboxPublishFailures(result.failed());
        if (result.claimed() > 0) {
            log.info("outbox poll finished claimed={} published={} failed={}",
                    result.claimed(), result.published(), result.failed());
        }
    }
}
