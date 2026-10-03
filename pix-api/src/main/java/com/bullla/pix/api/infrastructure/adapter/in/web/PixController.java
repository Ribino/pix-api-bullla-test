package com.bullla.pix.api.infrastructure.adapter.in.web;

import com.bullla.pix.api.application.usecase.pix.CreatePixTransactionUseCase;
import com.bullla.pix.api.application.usecase.pix.GetPixTransactionUseCase;
import com.bullla.pix.api.application.usecase.pix.PixTransactionResult;
import com.bullla.pix.api.domain.model.pix.PixTransaction;
import com.bullla.pix.api.domain.model.pix.PixTransactionStatus;
import com.bullla.pix.api.infrastructure.adapter.in.web.dto.CreatePixRequest;
import com.bullla.pix.api.infrastructure.adapter.in.web.dto.PixResponse;
import com.bullla.pix.api.infrastructure.metrics.PixApiMetrics;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/pix")
@Tag(name = "PIX", description = "PIX transaction operations")
public class PixController {

    private static final Logger log = LoggerFactory.getLogger(PixController.class);

    private final CreatePixTransactionUseCase createPixTransactionUseCase;
    private final GetPixTransactionUseCase getPixTransactionUseCase;
    private final PixApiMetrics metrics;

    public PixController(
            CreatePixTransactionUseCase createPixTransactionUseCase,
            GetPixTransactionUseCase getPixTransactionUseCase,
            PixApiMetrics metrics) {
        this.createPixTransactionUseCase = createPixTransactionUseCase;
        this.getPixTransactionUseCase = getPixTransactionUseCase;
        this.metrics = metrics;
    }

    @PostMapping
    @Operation(summary = "Create a PIX transaction",
            description = "Persists the transaction and its outbox event atomically. "
                    + "Returns immediately, without waiting for any downstream processing.")
    @ApiResponses({
            @ApiResponse(responseCode = "202", description = "Transaction accepted for asynchronous processing"),
            @ApiResponse(responseCode = "200", description = "Transaction already exists and reached a final status"),
            @ApiResponse(responseCode = "400", description = "Invalid request payload"),
            @ApiResponse(responseCode = "409", description = "transactionId already used with different request data"),
            @ApiResponse(responseCode = "500", description = "Unexpected error")
    })
    public ResponseEntity<PixResponse> create(@Valid @RequestBody CreatePixRequest request) {
        PixTransactionResult result = createPixTransactionUseCase.execute(request.toCommand());
        String outcome = result.created() ? "accepted" : "duplicate";
        metrics.countRequest(outcome);
        log.info("event=pix_request_received transactionId={} result={}",
                request.transactionId(), outcome);
        return ResponseEntity.status(httpStatusFor(result)).body(PixResponse.from(result.transaction()));
    }

    @GetMapping("/{transactionId}")
    @Operation(summary = "Get a PIX transaction by transactionId")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Transaction found"),
            @ApiResponse(responseCode = "404", description = "Transaction not found"),
            @ApiResponse(responseCode = "500", description = "Unexpected error")
    })
    public PixResponse get(@PathVariable String transactionId) {
        PixTransaction transaction = getPixTransactionUseCase.execute(transactionId);
        return PixResponse.from(transaction);
    }

    private HttpStatus httpStatusFor(PixTransactionResult result) {
        if (result.created() || result.transaction().status() == PixTransactionStatus.PROCESSING) {
            return HttpStatus.ACCEPTED;
        }
        return HttpStatus.OK;
    }
}
