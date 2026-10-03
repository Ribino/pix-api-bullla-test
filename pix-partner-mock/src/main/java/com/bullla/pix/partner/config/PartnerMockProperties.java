package com.bullla.pix.partner.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "partner.mock")
public class PartnerMockProperties {

    private String scenario = "success";
    private long latencyMs = 2000;
    private int failTimes = 2;
    private long slowDelayMs = 6000;

    public String getScenario() {
        return scenario;
    }

    public void setScenario(String scenario) {
        this.scenario = scenario;
    }

    public long getLatencyMs() {
        return latencyMs;
    }

    public void setLatencyMs(long latencyMs) {
        this.latencyMs = latencyMs;
    }

    public int getFailTimes() {
        return failTimes;
    }

    public void setFailTimes(int failTimes) {
        this.failTimes = failTimes;
    }

    public long getSlowDelayMs() {
        return slowDelayMs;
    }

    public void setSlowDelayMs(long slowDelayMs) {
        this.slowDelayMs = slowDelayMs;
    }
}
