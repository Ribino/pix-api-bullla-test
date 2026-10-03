package com.bullla.pix.partner.web.admin;

import com.bullla.pix.partner.service.ScenarioMode;

public record ScenarioRequest(
        ScenarioMode mode,
        long latencyMs,
        int failTimes,
        long slowDelayMs) {
}
