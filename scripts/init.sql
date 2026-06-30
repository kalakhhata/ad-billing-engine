-- Transaction log: every event processed by the billing engine
CREATE TABLE IF NOT EXISTS transaction_log (
    id              BIGSERIAL PRIMARY KEY,
    event_id        VARCHAR(64)     NOT NULL,
    advertiser_id   VARCHAR(64)     NOT NULL,
    campaign_id     VARCHAR(64)     NOT NULL,
    event_type      VARCHAR(16)     NOT NULL,   -- CLICK | IMPRESSION
    cost_micros     BIGINT          NOT NULL,   -- cost in micro-dollars (1 USD = 1,000,000 micros)
    status          VARCHAR(24)     NOT NULL,   -- SUCCESS | REJECTED | DLQ
    reject_reason   VARCHAR(128),
    retry_count     INT             NOT NULL DEFAULT 0,
    processed_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_event_id UNIQUE (event_id)   -- DB-level idempotency guard
);

CREATE INDEX IF NOT EXISTS idx_txn_advertiser_id ON transaction_log(advertiser_id);
CREATE INDEX IF NOT EXISTS idx_txn_status        ON transaction_log(status);
CREATE INDEX IF NOT EXISTS idx_txn_processed_at  ON transaction_log(processed_at);

-- Daily reconciliation snapshots
CREATE TABLE IF NOT EXISTS reconciliation_report (
    id              BIGSERIAL PRIMARY KEY,
    report_date     DATE            NOT NULL,
    advertiser_id   VARCHAR(64)     NOT NULL,
    expected_spend_micros   BIGINT  NOT NULL,   -- sum from transaction_log
    actual_balance_micros   BIGINT  NOT NULL,   -- snapshot from Redis
    initial_balance_micros  BIGINT  NOT NULL,   -- seeded value
    discrepancy_micros      BIGINT  NOT NULL,   -- expected - (initial - actual)
    status          VARCHAR(16)     NOT NULL,   -- OK | MISMATCH
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE(report_date, advertiser_id)
);

-- Seed advertiser accounts (used by reconciliation to know initial balances)
CREATE TABLE IF NOT EXISTS advertiser_account (
    advertiser_id           VARCHAR(64)  PRIMARY KEY,
    initial_balance_micros  BIGINT       NOT NULL,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

INSERT INTO advertiser_account (advertiser_id, initial_balance_micros) VALUES
    ('adv-001', 10000000000),
    ('adv-002', 5000000000),
    ('adv-003', 20000000000),
    ('adv-004', 1000000000),
    ('adv-005', 50000000000)
ON CONFLICT DO NOTHING;
