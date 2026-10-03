package com.bullla.pix.api.application.usecase.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "outbox.publisher")
public class OutboxPublisherProperties {

    private boolean enabled = true;
    private long fixedDelayMs = 5000;
    private int batchSize = 100;
    private long sendTimeoutMs = 10000;
    private long stuckAfterMinutes = 5;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public long getFixedDelayMs() {
        return fixedDelayMs;
    }

    public void setFixedDelayMs(long fixedDelayMs) {
        this.fixedDelayMs = fixedDelayMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getSendTimeoutMs() {
        return sendTimeoutMs;
    }

    public void setSendTimeoutMs(long sendTimeoutMs) {
        this.sendTimeoutMs = sendTimeoutMs;
    }

    public long getStuckAfterMinutes() {
        return stuckAfterMinutes;
    }

    public void setStuckAfterMinutes(long stuckAfterMinutes) {
        this.stuckAfterMinutes = stuckAfterMinutes;
    }
}
