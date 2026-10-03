package com.bullla.pix.api.infrastructure.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

@ExtendWith(MockitoExtension.class)
class PixApiMetricsTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Test
    void countsRequestsByResult() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PixApiMetrics metrics = new PixApiMetrics(registry, jdbcTemplate);

        metrics.countRequest("accepted");
        metrics.countRequest("accepted");
        metrics.countRequest("duplicate");

        assertThat(registry.counter("pix_requests_total", "result", "accepted").count()).isEqualTo(2);
        assertThat(registry.counter("pix_requests_total", "result", "duplicate").count()).isEqualTo(1);
    }

    @Test
    void refreshesPendingOutboxGaugeFromDatabase() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PixApiMetrics metrics = new PixApiMetrics(registry, jdbcTemplate);
        when(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_event WHERE status = 'PENDING'", Integer.class))
                .thenReturn(7);

        metrics.refreshPendingOutboxGauge();

        assertThat(registry.get("pix_outbox_pending").gauge().value()).isEqualTo(7);
    }

    @Test
    void keepsLastGaugeValueWhenDatabaseQueryFails() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PixApiMetrics metrics = new PixApiMetrics(registry, jdbcTemplate);
        when(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_event WHERE status = 'PENDING'", Integer.class))
                .thenReturn(4)
                .thenThrow(new RuntimeException("db down"));

        metrics.refreshPendingOutboxGauge();
        metrics.refreshPendingOutboxGauge();

        assertThat(registry.get("pix_outbox_pending").gauge().value()).isEqualTo(4);
    }

    @Test
    void countsOutboxPublishOutcomes() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PixApiMetrics metrics = new PixApiMetrics(registry, jdbcTemplate);

        metrics.countOutboxPublished(2);
        metrics.countOutboxPublishFailures(1);

        assertThat(registry.counter("pix_outbox_published_total").count()).isEqualTo(2);
        assertThat(registry.counter("pix_outbox_publish_failures_total").count()).isEqualTo(1);
    }
}
