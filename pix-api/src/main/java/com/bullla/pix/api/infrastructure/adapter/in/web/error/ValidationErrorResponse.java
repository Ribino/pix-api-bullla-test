package com.bullla.pix.api.infrastructure.adapter.in.web.error;

import java.util.List;

public record ValidationErrorResponse(String code, String message, List<FieldValidationError> details) {
}
