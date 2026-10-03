package com.bullla.pix.partner.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.bullla.pix.partner.config.PartnerMockProperties;
import com.bullla.pix.partner.web.PartnerRequest;
import com.bullla.pix.partner.web.PartnerResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Service
public class PartnerMockService {

    private static final Logger log = LoggerFactory.getLogger(PartnerMockService.class);

    private final AtomicReference<Scenario> scenario;
    private final ConcurrentHashMap<String, StoredOutcome> outcomesByTransactionId = new ConcurrentHashMap<>();
    private final AtomicInteger requestCount = new AtomicInteger();
    private final AtomicInteger transientFailures = new AtomicInteger();

    public PartnerMockService(PartnerMockProperties properties) {
        this.scenario = new AtomicReference<>(new Scenario(
                ScenarioMode.valueOf(properties.getScenario().toUpperCase()),
                properties.getLatencyMs(),
                properties.getFailTimes(),
                properties.getSlowDelayMs()));
    }

    public ResponseEntity<PartnerResponse> process(PartnerRequest request) {
        StoredOutcome recorded = outcomesByTransactionId.get(request.transactionId());
        if (recorded != null) {
            log.info("idempotent replay transactionId={} status={}", request.transactionId(), recorded.status());
            return ResponseEntity.status(recorded.status()).body(recorded.response());
        }

        requestCount.incrementAndGet();
        Scenario current = scenario.get();
        sleep(current.latencyMs());

        return switch (current.mode()) {
            case SUCCESS -> record(request, 200, new PartnerResponse(request.transactionId(), "APPROVED"));
            case FLAKY -> {
                if (transientFailures.incrementAndGet() <= current.failTimes()) {
                    log.info("simulated transient failure transactionId={}", request.transactionId());
                    yield ResponseEntity.status(503)
                            .body(new PartnerResponse(request.transactionId(), "TEMPORARY_FAILURE"));
                }
                yield record(request, 200, new PartnerResponse(request.transactionId(), "APPROVED"));
            }
            case ALWAYS_500 -> {
                log.info("simulated transient failure transactionId={}", request.transactionId());
                yield ResponseEntity.status(503)
                        .body(new PartnerResponse(request.transactionId(), "TEMPORARY_FAILURE"));
            }
            case ALWAYS_400 -> record(request, 400,
                    new PartnerResponse(request.transactionId(), "DECLINED"));
            case SLOW -> {
                sleep(current.slowDelayMs());
                yield record(request, 200, new PartnerResponse(request.transactionId(), "APPROVED"));
            }
        };
    }

    public void configure(ScenarioMode mode, long latencyMs, int failTimes, long slowDelayMs) {
        scenario.set(new Scenario(mode, latencyMs, failTimes, slowDelayMs));
        reset();
    }

    public void reset() {
        outcomesByTransactionId.clear();
        requestCount.set(0);
        transientFailures.set(0);
    }

    public MockStats stats() {
        return new MockStats(
                requestCount.get(),
                outcomesByTransactionId.size(),
                new ArrayList<>(outcomesByTransactionId.keySet()));
    }

    private ResponseEntity<PartnerResponse> record(PartnerRequest request, int status, PartnerResponse response) {
        outcomesByTransactionId.put(request.transactionId(), new StoredOutcome(status, response));
        log.info("processed transactionId={} status={}", request.transactionId(), status);
        return ResponseEntity.status(status).body(response);
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while simulating latency", interrupted);
        }
    }

    public record Scenario(ScenarioMode mode, long latencyMs, int failTimes, long slowDelayMs) {
    }

    private record StoredOutcome(int status, PartnerResponse response) {
    }

    public record MockStats(int requestCount, int processedCount, List<String> processedTransactionIds) {
    }
}
