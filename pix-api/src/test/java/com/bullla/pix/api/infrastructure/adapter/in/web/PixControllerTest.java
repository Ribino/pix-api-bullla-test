package com.bullla.pix.api.infrastructure.adapter.in.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import com.bullla.pix.api.application.exception.DuplicateTransactionConflictException;
import com.bullla.pix.api.application.exception.TransactionNotFoundException;
import com.bullla.pix.api.application.usecase.pix.CreatePixTransactionCommand;
import com.bullla.pix.api.application.usecase.pix.CreatePixTransactionUseCase;
import com.bullla.pix.api.application.usecase.pix.GetPixTransactionUseCase;
import com.bullla.pix.api.application.usecase.pix.PixTransactionResult;
import com.bullla.pix.api.domain.model.pix.PixTransaction;
import com.bullla.pix.api.domain.model.pix.PixTransactionStatus;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(PixController.class)
class PixControllerTest {

    private static final String CREATED_AT = "2026-09-28T21:00:00Z";

    private static final String VALID_REQUEST = """
            {
              "transactionId": "tx-1",
              "amount": 150.75,
              "pixKey": "cliente@email.com",
              "description": "Pagamento de fatura"
            }
            """;

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CreatePixTransactionUseCase createPixTransactionUseCase;

    @MockitoBean
    private GetPixTransactionUseCase getPixTransactionUseCase;

    @MockitoBean
    private com.bullla.pix.api.infrastructure.metrics.PixApiMetrics metrics;

    @Test
    void createsNewTransactionWith202() throws Exception {
        when(createPixTransactionUseCase.execute(any(CreatePixTransactionCommand.class)))
                .thenReturn(PixTransactionResult.created(transaction(PixTransactionStatus.PROCESSING)));

        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(VALID_REQUEST))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.transactionId").value("tx-1"))
                .andExpect(jsonPath("$.status").value("PROCESSING"))
                .andExpect(jsonPath("$.createdAt").value(CREATED_AT));
    }

    @Test
    void returns202ForDuplicateStillProcessing() throws Exception {
        when(createPixTransactionUseCase.execute(any(CreatePixTransactionCommand.class)))
                .thenReturn(PixTransactionResult.existing(transaction(PixTransactionStatus.PROCESSING)));

        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(VALID_REQUEST))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PROCESSING"));
    }

    @Test
    void returns200ForDuplicateAlreadySuccessful() throws Exception {
        when(createPixTransactionUseCase.execute(any(CreatePixTransactionCommand.class)))
                .thenReturn(PixTransactionResult.existing(transaction(PixTransactionStatus.SUCCESS)));

        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(VALID_REQUEST))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.createdAt").value(CREATED_AT));
    }

    @Test
    void returns200ForDuplicateAlreadyFailed() throws Exception {
        when(createPixTransactionUseCase.execute(any(CreatePixTransactionCommand.class)))
                .thenReturn(PixTransactionResult.existing(transaction(PixTransactionStatus.FAILED)));

        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(VALID_REQUEST))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("FAILED"));
    }

    @Test
    void returns409WhenTransactionIdConflicts() throws Exception {
        when(createPixTransactionUseCase.execute(any(CreatePixTransactionCommand.class)))
                .thenThrow(new DuplicateTransactionConflictException("tx-1"));

        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(VALID_REQUEST))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("TRANSACTION_ID_CONFLICT"))
                .andExpect(jsonPath("$.message").value(
                        "A transaction with the provided transactionId already exists with different request data."))
                .andExpect(jsonPath("$.transactionId").value("tx-1"));
    }

    @Test
    void returns400WhenAmountIsNotPositive() throws Exception {
        String body = """
                {"transactionId":"tx-1","amount":0,"pixKey":"cliente@email.com"}
                """;

        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.message").value("Request validation failed."))
                .andExpect(jsonPath("$.details[0].field").value("amount"))
                .andExpect(jsonPath("$.details[0].message").value("must be greater than 0"));
    }

    @Test
    void returns400WhenTransactionIdIsMissing() throws Exception {
        String body = """
                {"amount":150.75,"pixKey":"cliente@email.com"}
                """;

        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.details[0].field").value("transactionId"));
    }

    @Test
    void returns400WhenPixKeyIsMissing() throws Exception {
        String body = """
                {"transactionId":"tx-1","amount":150.75}
                """;

        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.details[0].field").value("pixKey"));
    }

    @Test
    void returns200WhenGettingProcessingTransaction() throws Exception {
        when(getPixTransactionUseCase.execute("tx-1"))
                .thenReturn(transaction(PixTransactionStatus.PROCESSING));

        mockMvc.perform(get("/pix/tx-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.transactionId").value("tx-1"))
                .andExpect(jsonPath("$.status").value("PROCESSING"))
                .andExpect(jsonPath("$.createdAt").value(CREATED_AT));
    }

    @Test
    void returns200WhenGettingSuccessfulTransaction() throws Exception {
        when(getPixTransactionUseCase.execute("tx-1"))
                .thenReturn(transaction(PixTransactionStatus.SUCCESS));

        mockMvc.perform(get("/pix/tx-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"));
    }

    @Test
    void returns200WhenGettingFailedTransaction() throws Exception {
        when(getPixTransactionUseCase.execute("tx-1"))
                .thenReturn(transaction(PixTransactionStatus.FAILED));

        mockMvc.perform(get("/pix/tx-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("FAILED"));
    }

    @Test
    void returns404WhenTransactionDoesNotExist() throws Exception {
        when(getPixTransactionUseCase.execute("missing"))
                .thenThrow(new TransactionNotFoundException("missing"));

        mockMvc.perform(get("/pix/missing"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("TRANSACTION_NOT_FOUND"))
                .andExpect(jsonPath("$.message").value("Transaction not found."))
                .andExpect(jsonPath("$.transactionId").value("missing"));
    }

    private PixTransaction transaction(PixTransactionStatus status) {
        Instant createdAt = Instant.parse(CREATED_AT);
        return new PixTransaction(
                UUID.randomUUID(),
                "tx-1",
                new BigDecimal("150.75"),
                "cliente@email.com",
                "Pagamento de fatura",
                status,
                "fingerprint",
                createdAt,
                createdAt);
    }
}
