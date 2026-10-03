CREATE TABLE pix_transaction (
    id UUID PRIMARY KEY,
    transaction_id VARCHAR(100) NOT NULL,
    amount NUMERIC(19, 2) NOT NULL,
    pix_key VARCHAR(255) NOT NULL,
    description VARCHAR(500),
    status VARCHAR(30) NOT NULL,
    request_fingerprint VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,

    CONSTRAINT uk_pix_transaction_transaction_id
        UNIQUE (transaction_id),

    CONSTRAINT ck_pix_transaction_status
        CHECK (status IN ('PROCESSING', 'SUCCESS', 'FAILED'))
);

CREATE TABLE outbox_event (
    id UUID PRIMARY KEY,
    aggregate_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(30) NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    published_at TIMESTAMP WITH TIME ZONE,

    CONSTRAINT ck_outbox_event_status
        CHECK (status IN ('PENDING', 'PUBLISHED'))
);

CREATE INDEX idx_outbox_event_pending
    ON outbox_event (created_at)
    WHERE status = 'PENDING';
