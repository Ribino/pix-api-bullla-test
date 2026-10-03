package com.bullla.pix.worker.infrastructure.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(PartnerClientProperties.class)
public class PartnerClientConfig {
}
