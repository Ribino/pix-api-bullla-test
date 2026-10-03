package com.bullla.pix.api.domain.service;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public class RequestFingerprint {

    private static final char FIELD_SEPARATOR = '|';
    private static final String NULL_FIELD = "-1";

    public String generate(String transactionId, BigDecimal amount, String pixKey, String description) {
        String canonical = canonicalize(transactionId, amount, pixKey, description);
        return sha256Hex(canonical);
    }

    private String canonicalize(String transactionId, BigDecimal amount, String pixKey, String description) {
        StringBuilder builder = new StringBuilder();
        appendField(builder, transactionId);
        appendField(builder, normalizeAmount(amount));
        appendField(builder, pixKey);
        appendField(builder, description);
        return builder.toString();
    }

    private void appendField(StringBuilder builder, String value) {
        if (value == null) {
            builder.append(NULL_FIELD).append(FIELD_SEPARATOR);
            return;
        }
        builder.append(value.length()).append(':').append(value).append(FIELD_SEPARATOR);
    }

    private String normalizeAmount(BigDecimal amount) {
        return amount.stripTrailingZeros().toPlainString();
    }

    private String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 algorithm is not available", exception);
        }
    }
}
