package com.bullla.pix.worker.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pix.partner")
public class PartnerClientProperties {

    private String baseUrl = "http://localhost:8082";
    private long timeoutMs = 3000;
    private int retryMaxAttempts = 3;
    private long retryInitialIntervalMs = 500;
    private double retryMultiplier = 2.0;
    private long retryMaxIntervalMs = 2000;

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public int getRetryMaxAttempts() {
        return retryMaxAttempts;
    }

    public void setRetryMaxAttempts(int retryMaxAttempts) {
        this.retryMaxAttempts = retryMaxAttempts;
    }

    public long getRetryInitialIntervalMs() {
        return retryInitialIntervalMs;
    }

    public void setRetryInitialIntervalMs(long retryInitialIntervalMs) {
        this.retryInitialIntervalMs = retryInitialIntervalMs;
    }

    public double getRetryMultiplier() {
        return retryMultiplier;
    }

    public void setRetryMultiplier(double retryMultiplier) {
        this.retryMultiplier = retryMultiplier;
    }

    public long getRetryMaxIntervalMs() {
        return retryMaxIntervalMs;
    }

    public void setRetryMaxIntervalMs(long retryMaxIntervalMs) {
        this.retryMaxIntervalMs = retryMaxIntervalMs;
    }
}
