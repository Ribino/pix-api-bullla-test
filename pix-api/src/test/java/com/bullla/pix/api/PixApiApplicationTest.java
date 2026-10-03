package com.bullla.pix.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PixApiApplicationTest {

    @Test
    void applicationEntryPointIsAvailable() throws Exception {
        assertThat(PixApiApplication.class.getMethod("main", String[].class)).isNotNull();
    }
}
