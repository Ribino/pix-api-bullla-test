package com.bullla.pix.api.infrastructure.config;

import com.bullla.pix.api.infrastructure.adapter.out.messaging.OutboxKafkaTopics;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@ConditionalOnProperty(prefix = "outbox.publisher", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaTopicConfig {

    @Bean
    public NewTopic pixRequestedTopic(
            @Value("${pix.kafka.topic-replicas:1}") int replicas) {
        return TopicBuilder
                .name(OutboxKafkaTopics.PIX_REQUESTED)
                .partitions(OutboxKafkaTopics.PIX_REQUESTED_PARTITIONS)
                .replicas(replicas)
                .build();
    }
}
