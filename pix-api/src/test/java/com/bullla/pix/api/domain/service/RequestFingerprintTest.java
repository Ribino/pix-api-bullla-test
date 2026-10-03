package com.bullla.pix.api.domain.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

class RequestFingerprintTest {

    private final RequestFingerprint requestFingerprint = new RequestFingerprint();

    @Test
    void isDeterministicForEquivalentRequests() {
        String first = generate("tx-1", "150.75", "cliente@email.com", "Pagamento");
        String second = generate("tx-1", "150.75", "cliente@email.com", "Pagamento");

        assertThat(first).isEqualTo(second);
    }

    @Test
    void isSha256With64HexadecimalCharacters() {
        String fingerprint = generate("tx-1", "150.75", "cliente@email.com", "Pagamento");

        assertThat(fingerprint).hasSize(64).matches("[0-9a-f]{64}");
    }

    @Test
    void normalizesSemanticallyEqualAmounts() {
        String first = generate("tx-1", "150.7", "cliente@email.com", "Pagamento");
        String second = generate("tx-1", "150.70", "cliente@email.com", "Pagamento");

        assertThat(first).isEqualTo(second);
    }

    @Test
    void changesWhenAnyFieldChanges() {
        String reference = generate("tx-1", "150.75", "cliente@email.com", "Pagamento");

        assertThat(generate("tx-2", "150.75", "cliente@email.com", "Pagamento")).isNotEqualTo(reference);
        assertThat(generate("tx-1", "150.76", "cliente@email.com", "Pagamento")).isNotEqualTo(reference);
        assertThat(generate("tx-1", "150.75", "outro@email.com", "Pagamento")).isNotEqualTo(reference);
        assertThat(generate("tx-1", "150.75", "cliente@email.com", "Outro")).isNotEqualTo(reference);
    }

    @Test
    void isNotAmbiguousAcrossFieldBoundaries() {
        String first = generate("ab", "1", "c", null);
        String second = generate("a", "1", "bc", null);

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    void distinguishesNullFromEmptyDescription() {
        String withNull = generate("tx-1", "1", "chave", null);
        String withEmpty = generate("tx-1", "1", "chave", "");

        assertThat(withNull).isNotEqualTo(withEmpty);
    }

    private String generate(String transactionId, String amount, String pixKey, String description) {
        return requestFingerprint.generate(transactionId, new BigDecimal(amount), pixKey, description);
    }
}
