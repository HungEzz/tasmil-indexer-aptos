-- This file should undo anything in `up.sql`

-- Drop simplified table
DROP INDEX IF EXISTS idx_apt_data_inserted_at;
DROP INDEX IF EXISTS idx_apt_data_transaction_version;
DROP TABLE IF EXISTS apt_data;

-- Recreate original tables
CREATE TABLE apt_prices (
    id BIGSERIAL PRIMARY KEY,
    transaction_version BIGINT NOT NULL,
    transaction_block_height BIGINT NOT NULL,
    timestamp_seconds BIGINT NOT NULL,
    apt_price_usd DECIMAL(20, 8) NOT NULL,
    amount_in DECIMAL(20, 8) NOT NULL,
    amount_out DECIMAL(20, 8) NOT NULL,
    from_token VARCHAR(300) NOT NULL,
    to_token VARCHAR(300) NOT NULL,
    pool_address VARCHAR(66) NOT NULL,
    event_index BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(transaction_version, event_index)
);

CREATE TABLE apt_volume_24h (
    id BIGSERIAL PRIMARY KEY,
    calculation_time TIMESTAMP NOT NULL DEFAULT NOW(),
    start_timestamp_seconds BIGINT NOT NULL,
    end_timestamp_seconds BIGINT NOT NULL,
    total_volume_usd DECIMAL(20, 8) NOT NULL,
    total_apt_amount DECIMAL(20, 8) NOT NULL,
    avg_price DECIMAL(20, 8) NOT NULL,
    swap_count INTEGER NOT NULL,
    last_updated TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Recreate indexes
CREATE INDEX idx_apt_prices_timestamp ON apt_prices(timestamp_seconds);
CREATE INDEX idx_apt_prices_version ON apt_prices(transaction_version);
CREATE INDEX idx_apt_volume_24h_calculation_time ON apt_volume_24h(calculation_time);
