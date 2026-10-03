package com.bullla.pix.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;

@AutoConfigureMockMvc
class PixApiIT extends PostgresIntegrationTestBase {

    private static final String REQUEST = """
            {
              "transactionId": "tx-http",
              "amount": 150.75,
              "pixKey": "cliente@email.com",
              "description": "Pagamento de fatura"
            }
            """;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private io.micrometer.core.instrument.MeterRegistry meterRegistry;

    @Test
    void createsTransactionAndOutboxThroughHttp() throws Exception {
        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(REQUEST))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.transactionId").value("tx-http"))
                .andExpect(jsonPath("$.status").value("PROCESSING"))
                .andExpect(jsonPath("$.createdAt").isString());

        assertThat(count("pix_transaction")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
    }

    @Test
    void countsAcceptedRequestsInMetrics() throws Exception {
        double before = meterRegistry.counter("pix_requests_total", "result", "accepted").count();

        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(REQUEST))
                .andExpect(status().isAccepted());

        assertThat(meterRegistry.counter("pix_requests_total", "result", "accepted").count())
                .isEqualTo(before + 1);
    }

    @Test
    void isIdempotentForTheSameRequest() throws Exception {
        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(REQUEST))
                .andExpect(status().isAccepted());
        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(REQUEST))
                .andExpect(status().isAccepted());

        assertThat(count("pix_transaction")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
    }

    @Test
    void returns200WhenTransactionAlreadyCompleted() throws Exception {
        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(REQUEST))
                .andExpect(status().isAccepted());

        jdbcTemplate.update("UPDATE pix_transaction SET status = 'SUCCESS' WHERE transaction_id = ?", "tx-http");

        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(REQUEST))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"));

        assertThat(count("pix_transaction")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
    }

    @Test
    void returns409ForConflictingPayload() throws Exception {
        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(REQUEST))
                .andExpect(status().isAccepted());

        String conflictingRequest = """
                {
                  "transactionId": "tx-http",
                  "amount": 999.99,
                  "pixKey": "cliente@email.com",
                  "description": "Pagamento de fatura"
                }
                """;

        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(conflictingRequest))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("TRANSACTION_ID_CONFLICT"))
                .andExpect(jsonPath("$.transactionId").value("tx-http"));

        assertThat(count("pix_transaction")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
    }

    @Test
    void returnsTransactionOnGet() throws Exception {
        mockMvc.perform(post("/pix").contentType(APPLICATION_JSON).content(REQUEST))
                .andExpect(status().isAccepted());

        mockMvc.perform(get("/pix/tx-http"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.transactionId").value("tx-http"))
                .andExpect(jsonPath("$.status").value("PROCESSING"))
                .andExpect(jsonPath("$.createdAt").isString());
    }

    @Test
    void returns404WhenGettingUnknownTransaction() throws Exception {
        mockMvc.perform(get("/pix/unknown"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("TRANSACTION_NOT_FOUND"))
                .andExpect(jsonPath("$.message").value("Transaction not found."))
                .andExpect(jsonPath("$.transactionId").value("unknown"));
    }
}
