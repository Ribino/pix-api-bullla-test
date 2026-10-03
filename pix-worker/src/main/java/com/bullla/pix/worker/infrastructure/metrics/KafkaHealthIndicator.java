package com.bullla.pix.worker.infrastructure.metrics;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

@Component
public class KafkaHealthIndicator implements HealthIndicator {

    private static final Logger log = LoggerFactory.getLogger(KafkaHealthIndicator.class);
    private static final long DESCRIBE_TIMEOUT_SECONDS = 5;

    private final Admin adminClient;

    public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
        this.adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    @Override
    public Health health() {
        try {
            String clusterId = adminClient
                    .describeCluster()
                    .clusterId()
                    .get(DESCRIBE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            int nodes = adminClient.describeCluster().nodes()
                    .get(DESCRIBE_TIMEOUT_SECONDS, TimeUnit.SECONDS).size();
            return Health.up()
                    .withDetail("clusterId", clusterId)
                    .withDetail("nodes", nodes)
                    .build();
        } catch (Exception exception) {
            log.warn("kafka health check failed error={}", exception.toString());
            return Health.down(exception).build();
        }
    }

    @PreDestroy
    public void close() {
        adminClient.close(Duration.ofSeconds(5));
    }
}
