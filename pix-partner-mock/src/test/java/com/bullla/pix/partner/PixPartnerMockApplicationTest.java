package com.bullla.pix.partner;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PixPartnerMockApplicationTest {

    @Test
    void applicationEntryPointIsAvailable() throws Exception {
        assertThat(PixPartnerMockApplication.class.getMethod("main", String[].class)).isNotNull();
    }
}
