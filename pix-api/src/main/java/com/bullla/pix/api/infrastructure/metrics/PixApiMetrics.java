package com.bullla.pix.api.infrastructure.metrics;

import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.MeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class PixApiMetrics {

    private static final Logger log = LoggerFactory.getLogger(PixApiMetrics.class);

    private final MeterRegistry registry;
    private final JdbcTemplate jdbcTemplate;
    private final AtomicInteger pendingOutboxEvents = new AtomicInteger();

    public PixApiMetrics(MeterRegistry registry, JdbcTemplate jdbcTemplate) {
        this.registry = registry;
        this.jdbcTemplate = jdbcTemplate;
        registry.gauge("pix_outbox_pending", pendingOutboxEvents);
    }

    public void countRequest(String result) {
        registry.counter("pix_requests_total", "result", result).increment();
    }

    public void countOutboxPublished(int amount) {
        registry.counter("pix_outbox_published_total").increment(amount);
    }

    public void countOutboxPublishFailures(int amount) {
        registry.counter("pix_outbox_publish_failures_total").increment(amount);
    }

    @Scheduled(fixedDelayString = "${outbox.metrics.gauge-interval-ms:15000}")
    public void refreshPendingOutboxGauge() {
        try {
            Integer pending = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM outbox_event WHERE status = 'PENDING'", Integer.class);
            pendingOutboxEvents.set(pending == null ? 0 : pending);
        } catch (Exception exception) {
            log.warn("failed to refresh pix_outbox_pending gauge error={}", exception.toString());
        }
    }
}
