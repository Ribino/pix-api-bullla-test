package com.bullla.pix.worker.infrastructure.metrics;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;

@Component
public class KafkaConsumerLagMetrics {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerLagMetrics.class);
    private static final long ADMIN_TIMEOUT_SECONDS = 10;

    private final Admin adminClient;
    private final Map<GroupTopic, AtomicLong> lags;

    public KafkaConsumerLagMetrics(
            KafkaAdmin kafkaAdmin,
            MeterRegistry registry,
            @Value("${pix.kafka.group-id:pix-processing}") String mainGroup,
            @Value("${pix.kafka.topic:pix.requested}") String mainTopic,
            @Value("${pix.retry.group-id:pix-retry}") String retryGroup,
            @Value("${pix.retry.topic:pix.retry}") String retryTopic) {
        this.adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
        this.lags = Map.of(
                new GroupTopic(mainGroup, mainTopic), gaugeFor(registry, mainGroup, mainTopic),
                new GroupTopic(retryGroup, retryTopic), gaugeFor(registry, retryGroup, retryTopic));
    }

    @Scheduled(fixedDelayString = "${pix.kafka.lag-interval-ms:15000}")
    public void refresh() {
        lags.forEach((groupTopic, gauge) -> {
            try {
                gauge.set(computeLag(groupTopic.group(), groupTopic.topic()));
            } catch (Exception exception) {
                log.warn("failed to refresh consumer lag group={} topic={} error={}",
                        groupTopic.group(), groupTopic.topic(), exception.toString());
            }
        });
    }

    @PreDestroy
    public void close() {
        adminClient.close(Duration.ofSeconds(5));
    }

    private long computeLag(String group, String topic) throws Exception {
        Map<TopicPartition, Long> committed = adminClient
                .listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                .get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS).entrySet().stream()
                .filter(entry -> entry.getKey().topic().equals(topic))
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
        if (committed.isEmpty()) {
            return 0;
        }
        Map<TopicPartition, OffsetSpec> latest = committed.keySet().stream()
                .collect(Collectors.toMap(partition -> partition, partition -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = adminClient
                .listOffsets(latest).all().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return ends.entrySet().stream()
                .mapToLong(entry -> Math.max(0, entry.getValue().offset()
                        - committed.getOrDefault(entry.getKey(), 0L)))
                .sum();
    }

    private static AtomicLong gaugeFor(MeterRegistry registry, String group, String topic) {
        AtomicLong gauge = new AtomicLong();
        registry.gauge("kafka_consumer_lag_records",
                io.micrometer.core.instrument.Tags.of("group", group, "topic", topic), gauge);
        return gauge;
    }

    private record GroupTopic(String group, String topic) {
    }
}
