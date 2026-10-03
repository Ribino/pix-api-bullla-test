package com.bullla.pix.worker.infrastructure.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class WorkerKafkaTopicConfig {

    @Bean
    public NewTopic pixRetryTopic(
            @Value("${pix.retry.topic:pix.retry}") String topic,
            @Value("${pix.kafka.topic-replicas:1}") int replicas) {
        return TopicBuilder
                .name(topic)
                .partitions(3)
                .replicas(replicas)
                .build();
    }

    @Bean
    public NewTopic pixDlqTopic(
            @Value("${pix.dlq.topic:pix.dlq}") String topic,
            @Value("${pix.kafka.topic-replicas:1}") int replicas) {
        return TopicBuilder
                .name(topic)
                .partitions(1)
                .replicas(replicas)
                .build();
    }
}
