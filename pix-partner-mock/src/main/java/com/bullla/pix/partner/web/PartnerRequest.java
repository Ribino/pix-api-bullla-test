package com.bullla.pix.partner.web;

import java.math.BigDecimal;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record PartnerRequest(
        @NotBlank
        @Size(max = 100)
        String transactionId,

        @NotNull
        @DecimalMin(value = "0", inclusive = false)
        BigDecimal amount,

        @NotBlank
        @Size(max = 255)
        String pixKey,

        @Size(max = 500)
        String description) {
}
