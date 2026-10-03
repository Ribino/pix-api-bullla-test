package com.bullla.pix.api.infrastructure.config;

import com.bullla.pix.api.domain.service.RequestFingerprint;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DomainServiceConfig {

    @Bean
    public RequestFingerprint requestFingerprint() {
        return new RequestFingerprint();
    }
}
