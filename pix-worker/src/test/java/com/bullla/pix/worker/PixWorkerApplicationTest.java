package com.bullla.pix.worker;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PixWorkerApplicationTest {

    @Test
    void applicationEntryPointIsAvailable() throws Exception {
        assertThat(PixWorkerApplication.class.getMethod("main", String[].class)).isNotNull();
    }
}
