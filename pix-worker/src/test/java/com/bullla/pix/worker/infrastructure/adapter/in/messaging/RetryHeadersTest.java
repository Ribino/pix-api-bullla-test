package com.bullla.pix.worker.infrastructure.adapter.in.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import com.bullla.pix.worker.domain.failure.FailureReason;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class RetryHeadersTest {

    private static final Instant FIRST_FAILURE_AT = Instant.parse("2026-09-29T15:00:00Z");

    @Test
    void roundTripsAllFields() {
        List<Header> headers = RetryHeaders.of(2, FailureReason.PARTNER_TIMEOUT, FIRST_FAILURE_AT, "pix.requested");

        RetryHeaders.Parsed parsed = RetryHeaders.parse(new RecordHeaders(headers.toArray(new Header[0])));

        assertThat(parsed.retryCount()).isEqualTo(2);
        assertThat(parsed.reason()).isEqualTo(FailureReason.PARTNER_TIMEOUT);
        assertThat(parsed.firstFailureAt()).isEqualTo(FIRST_FAILURE_AT);
    }

    @Test
    void defaultsWhenHeadersAreMissing() {
        RetryHeaders.Parsed parsed = RetryHeaders.parse(new RecordHeaders());

        assertThat(parsed.retryCount()).isZero();
        assertThat(parsed.reason()).isEqualTo(FailureReason.UNKNOWN_PARTNER_ERROR);
        assertThat(parsed.firstFailureAt()).isNotNull();
    }

    @Test
    void defaultsCorruptValues() {
        Headers headers = new RecordHeaders(new Header[]{
                new RecordHeader("pix-retry-count", "nan".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("pix-failure-reason", "NOPE".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("pix-first-failure-at", "not-a-date".getBytes(StandardCharsets.UTF_8)),
        });

        RetryHeaders.Parsed parsed = RetryHeaders.parse(headers);

        assertThat(parsed.retryCount()).isZero();
        assertThat(parsed.reason()).isEqualTo(FailureReason.UNKNOWN_PARTNER_ERROR);
        assertThat(parsed.firstFailureAt()).isNotNull();
    }
}
