package com.bullla.pix.api.infrastructure.config;

import com.bullla.pix.api.application.usecase.outbox.OutboxPublisherProperties;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(OutboxPublisherProperties.class)
public class SchedulingConfig {
}
