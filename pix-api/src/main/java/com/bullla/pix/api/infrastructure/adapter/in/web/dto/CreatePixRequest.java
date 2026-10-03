package com.bullla.pix.api.infrastructure.adapter.in.web.dto;

import java.math.BigDecimal;

import com.bullla.pix.api.application.usecase.pix.CreatePixTransactionCommand;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

@Schema(description = "Request payload for creating a PIX transaction")
public record CreatePixRequest(
        @Schema(description = "Client-provided unique transaction identifier", example = "tx-123456", maxLength = 100)
        @NotBlank
        @Size(max = 100)
        String transactionId,

        @Schema(description = "Transaction amount, must be greater than zero", example = "150.75")
        @NotNull
        @DecimalMin(value = "0", inclusive = false, message = "must be greater than 0")
        BigDecimal amount,

        @Schema(description = "PIX key (any string, not validated as an email)", example = "cliente@email.com", maxLength = 255)
        @NotBlank
        @Size(max = 255)
        String pixKey,

        @Schema(description = "Optional transaction description", example = "Pagamento de fatura", maxLength = 500)
        @Size(max = 500)
        String description) {

    public CreatePixTransactionCommand toCommand() {
        return new CreatePixTransactionCommand(transactionId, amount, pixKey, description);
    }
}
