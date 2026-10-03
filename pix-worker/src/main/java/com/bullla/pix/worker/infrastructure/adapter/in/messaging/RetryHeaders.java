package com.bullla.pix.worker.infrastructure.adapter.in.messaging;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.bullla.pix.worker.domain.failure.FailureReason;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

public final class RetryHeaders {

    static final String RETRY_COUNT = "pix-retry-count";
    static final String FAILURE_REASON = "pix-failure-reason";
    static final String FIRST_FAILURE_AT = "pix-first-failure-at";
    static final String ORIGINAL_TOPIC = "pix-original-topic";

    private RetryHeaders() {
    }

    public record Parsed(int retryCount, FailureReason reason, Instant firstFailureAt) {
    }

    public static Parsed parse(Headers headers) {
        return new Parsed(readInt(headers, RETRY_COUNT, 0), readReason(headers), readInstant(headers));
    }

    public static List<Header> of(int retryCount, FailureReason reason, Instant firstFailureAt, String originalTopic) {
        List<Header> result = new ArrayList<>();
        result.add(new RecordHeader(RETRY_COUNT, String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8)));
        result.add(new RecordHeader(FAILURE_REASON, reason.name().getBytes(StandardCharsets.UTF_8)));
        result.add(new RecordHeader(FIRST_FAILURE_AT, firstFailureAt.toString().getBytes(StandardCharsets.UTF_8)));
        result.add(new RecordHeader(ORIGINAL_TOPIC, originalTopic.getBytes(StandardCharsets.UTF_8)));
        return result;
    }

    private static int readInt(Headers headers, String key, int defaultValue) {
        Header header = headers.lastHeader(key);
        if (header == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
        } catch (NumberFormatException invalid) {
            return defaultValue;
        }
    }

    private static FailureReason readReason(Headers headers) {
        Header header = headers.lastHeader(FAILURE_REASON);
        if (header == null) {
            return FailureReason.UNKNOWN_PARTNER_ERROR;
        }
        try {
            return FailureReason.valueOf(new String(header.value(), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException invalid) {
            return FailureReason.UNKNOWN_PARTNER_ERROR;
        }
    }

    private static Instant readInstant(Headers headers) {
        Header header = headers.lastHeader(FIRST_FAILURE_AT);
        if (header == null) {
            return Instant.now();
        }
        try {
            return Instant.parse(new String(header.value(), StandardCharsets.UTF_8));
        } catch (Exception invalid) {
            return Instant.now();
        }
    }
}
