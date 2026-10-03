package com.bullla.pix.api.infrastructure.adapter.in.web.dto;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.bullla.pix.api.domain.model.pix.PixTransaction;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "PIX transaction representation")
public record PixResponse(
        @Schema(example = "tx-123456") String transactionId,
        @Schema(example = "PROCESSING") String status,
        @Schema(example = "2026-09-28T21:00:00Z") Instant createdAt) {

    public static PixResponse from(PixTransaction transaction) {
        return new PixResponse(
                transaction.transactionId(),
                transaction.status().name(),
                transaction.createdAt().truncatedTo(ChronoUnit.SECONDS));
    }
}
