package com.bullla.pix.partner.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@SpringBootTest(properties = "partner.mock.latency-ms=20")
@AutoConfigureMockMvc
class PartnerControllerTest {

    private static final String REQUEST = """
            {"transactionId":"%s","amount":150.75,"pixKey":"cliente@email.com","description":"Pagamento"}
            """;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void reset() throws Exception {
        scenario("SUCCESS", 20, 2);
    }

    @Test
    void approvesSuccessfulRequests() throws Exception {
        mockMvc.perform(post("/partner/pix").contentType(APPLICATION_JSON)
                        .content(REQUEST.formatted("tx-ok")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.transactionId").value("tx-ok"))
                .andExpect(jsonPath("$.status").value("APPROVED"));

        assertThat(stats().processedCount()).isEqualTo(1);
    }

    @Test
    void failsTransientlyThenApprovesInFlakyScenario() throws Exception {
        scenario("FLAKY", 20, 2);

        mockMvc.perform(post("/partner/pix").contentType(APPLICATION_JSON)
                        .content(REQUEST.formatted("tx-f1")))
                .andExpect(status().isServiceUnavailable());
        mockMvc.perform(post("/partner/pix").contentType(APPLICATION_JSON)
                        .content(REQUEST.formatted("tx-f2")))
                .andExpect(status().isServiceUnavailable());
        mockMvc.perform(post("/partner/pix").contentType(APPLICATION_JSON)
                        .content(REQUEST.formatted("tx-f3")))
                .andExpect(status().isOk());

        assertThat(stats().processedCount()).isEqualTo(1);
    }

    @Test
    void declinesPermanently() throws Exception {
        scenario("ALWAYS_400", 20, 2);

        mockMvc.perform(post("/partner/pix").contentType(APPLICATION_JSON)
                        .content(REQUEST.formatted("tx-bad")))
                .andExpect(status().isBadRequest());

        mockMvc.perform(post("/partner/pix").contentType(APPLICATION_JSON)
                        .content(REQUEST.formatted("tx-bad")))
                .andExpect(status().isBadRequest());

        assertThat(stats().processedCount()).isEqualTo(1);
    }

    @Test
    void alwaysFailsTransientlyWithoutRecording() throws Exception {
        scenario("ALWAYS_500", 20, 2);

        mockMvc.perform(post("/partner/pix").contentType(APPLICATION_JSON)
                        .content(REQUEST.formatted("tx-t")))
                .andExpect(status().isServiceUnavailable());
        mockMvc.perform(post("/partner/pix").contentType(APPLICATION_JSON)
                        .content(REQUEST.formatted("tx-t")))
                .andExpect(status().isServiceUnavailable());

        assertThat(stats().requestCount()).isEqualTo(2);
        assertThat(stats().processedCount()).isZero();
    }

    @Test
    void rejectsInvalidPayload() throws Exception {
        mockMvc.perform(post("/partner/pix").contentType(APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isBadRequest());
    }

    private void scenario(String mode, long latencyMs, int failTimes) throws Exception {
        mockMvc.perform(post("/partner/admin/scenario").contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(
                                Map.of("mode", mode, "latencyMs", latencyMs,
                                        "failTimes", failTimes, "slowDelayMs", 1000))))
                .andExpect(status().isOk());
    }

    @SuppressWarnings("unchecked")
    private Stats stats() throws Exception {
        MvcResult result = mockMvc.perform(get("/partner/admin/stats"))
                .andExpect(status().isOk())
                .andReturn();
        Map<String, Object> body = objectMapper.readValue(result.getResponse().getContentAsString(), Map.class);
        return new Stats((int) body.get("requestCount"), (int) body.get("processedCount"),
                (List<String>) body.get("processedTransactionIds"));
    }

    private record Stats(int requestCount, int processedCount, List<String> processedTransactionIds) {
    }
}
