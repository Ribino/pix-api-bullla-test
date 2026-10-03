ALTER TABLE outbox_event
    DROP CONSTRAINT ck_outbox_event_status;

ALTER TABLE outbox_event
    ADD CONSTRAINT ck_outbox_event_status
        CHECK (status IN ('PENDING', 'PUBLISHING', 'PUBLISHED'));

ALTER TABLE outbox_event
    ADD COLUMN claimed_at TIMESTAMP WITH TIME ZONE;

CREATE INDEX idx_outbox_event_publishing
    ON outbox_event (claimed_at)
    WHERE status = 'PUBLISHING';
