package com.bullla.pix.worker.infrastructure.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.springframework.stereotype.Component;

@Component
public class PixWorkerMetrics {

    private final MeterRegistry registry;

    public PixWorkerMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public Timer.Sample startProcessing() {
        return Timer.start(registry);
    }

    public void stopProcessing(Timer.Sample sample, String result) {
        sample.stop(registry.timer("pix_processing_duration", "result", result));
    }

    public void countProcessing(String result) {
        registry.counter("pix_processing_total", "result", result).increment();
    }

    public Timer.Sample startPartnerCall() {
        return Timer.start(registry);
    }

    public void stopPartnerCall(Timer.Sample sample, String result) {
        sample.stop(registry.timer("pix_partner_request_duration", "result", result));
    }

    public void countPartnerRequest(String result) {
        registry.counter("pix_partner_requests_total", "result", result).increment();
    }

    public void countPartnerRetry(String reason) {
        registry.counter("pix_partner_retries_total", "reason", reason).increment();
    }

    public void countAsyncRetry() {
        registry.counter("pix_async_retries_total").increment();
    }

    public void countDlq(String reason) {
        registry.counter("pix_dlq_messages_total", "reason", reason).increment();
    }
}
