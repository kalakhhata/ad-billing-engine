CREATE TABLE IF NOT EXISTS transaction_log (
    id              BIGSERIAL PRIMARY KEY,
    event_id        VARCHAR(64)     NOT NULL,
    advertiser_id   VARCHAR(64)     NOT NULL,
    campaign_id     VARCHAR(64)     NOT NULL,
    event_type      VARCHAR(16)     NOT NULL,
    cost_micros     BIGINT          NOT NULL,
    status          VARCHAR(24)     NOT NULL,
    reject_reason   VARCHAR(128),
    retry_count     INT             NOT NULL DEFAULT 0,
    processed_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_event_id UNIQUE (event_id)
);
