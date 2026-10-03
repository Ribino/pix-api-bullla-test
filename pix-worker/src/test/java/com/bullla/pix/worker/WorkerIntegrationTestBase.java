package com.bullla.pix.worker;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import com.bullla.pix.worker.domain.model.pix.PixTransactionStatus;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

@TestPropertySource(properties = "management.tracing.enabled=false")
public abstract class WorkerIntegrationTestBase {

    private static final String CREATE_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS pix_transaction (
                id UUID PRIMARY KEY,
                transaction_id VARCHAR(100) NOT NULL UNIQUE,
                amount NUMERIC(19, 2) NOT NULL,
                pix_key VARCHAR(255) NOT NULL,
                description VARCHAR(500),
                status VARCHAR(30) NOT NULL,
                request_fingerprint VARCHAR(64) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL
            )
            """;

    protected static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    static {
        POSTGRES.start();
    }

    protected final JdbcTemplate jdbcTemplate = new JdbcTemplate(workerDataSource());

    private static DriverManagerDataSource workerDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @BeforeEach
    void resetDatabase() {
        jdbcTemplate.execute(CREATE_TABLE_SQL);
        jdbcTemplate.execute("TRUNCATE TABLE pix_transaction");
    }

    protected UUID insertPixTransaction(String transactionId, PixTransactionStatus status) {
        UUID id = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        jdbcTemplate.update(
                "INSERT INTO pix_transaction (id, transaction_id, amount, pix_key, description, status, request_fingerprint, created_at, updated_at)"
                        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                id, transactionId, new BigDecimal("150.75"), "cliente@email.com",
                "Pagamento", status.name(), "fingerprint", now, now);
        return id;
    }

    protected String statusOf(String transactionId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM pix_transaction WHERE transaction_id = ?", String.class, transactionId);
    }
}
