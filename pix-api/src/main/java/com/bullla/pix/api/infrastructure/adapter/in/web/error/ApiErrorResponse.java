package com.bullla.pix.api.infrastructure.adapter.in.web.error;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ApiErrorResponse(String code, String message, String transactionId) {
}
