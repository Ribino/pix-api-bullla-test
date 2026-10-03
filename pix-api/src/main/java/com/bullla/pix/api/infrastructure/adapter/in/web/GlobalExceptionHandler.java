package com.bullla.pix.api.infrastructure.adapter.in.web;

import java.util.List;

import com.bullla.pix.api.application.exception.DuplicateTransactionConflictException;
import com.bullla.pix.api.application.exception.TransactionNotFoundException;
import com.bullla.pix.api.infrastructure.adapter.in.web.error.ApiErrorResponse;
import com.bullla.pix.api.infrastructure.adapter.in.web.error.FieldValidationError;
import com.bullla.pix.api.infrastructure.adapter.in.web.error.ValidationErrorResponse;
import com.bullla.pix.api.infrastructure.metrics.PixApiMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    private final PixApiMetrics metrics;

    public GlobalExceptionHandler(PixApiMetrics metrics) {
        this.metrics = metrics;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ValidationErrorResponse handleValidation(MethodArgumentNotValidException exception) {
        List<FieldValidationError> details = exception.getBindingResult().getFieldErrors().stream()
                .map(error -> new FieldValidationError(error.getField(), error.getDefaultMessage()))
                .toList();
        return new ValidationErrorResponse("VALIDATION_ERROR", "Request validation failed.", details);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ValidationErrorResponse handleUnreadableBody(HttpMessageNotReadableException exception) {
        return new ValidationErrorResponse(
                "VALIDATION_ERROR",
                "Request validation failed.",
                List.of(new FieldValidationError("body", "malformed or unreadable request body")));
    }

    @ExceptionHandler(TransactionNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ApiErrorResponse handleNotFound(TransactionNotFoundException exception) {
        return new ApiErrorResponse("TRANSACTION_NOT_FOUND", "Transaction not found.", exception.getTransactionId());
    }

    @ExceptionHandler(DuplicateTransactionConflictException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public ApiErrorResponse handleConflict(DuplicateTransactionConflictException exception) {
        metrics.countRequest("conflict");
        log.info("event=pix_request_received transactionId={} result=conflict", exception.getTransactionId());
        return new ApiErrorResponse(
                "TRANSACTION_ID_CONFLICT",
                "A transaction with the provided transactionId already exists with different request data.",
                exception.getTransactionId());
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ApiErrorResponse handleUnexpected(Exception exception) {
        log.error("Unexpected error while handling request", exception);
        return new ApiErrorResponse("INTERNAL_SERVER_ERROR", "Unexpected error.", null);
    }
}
