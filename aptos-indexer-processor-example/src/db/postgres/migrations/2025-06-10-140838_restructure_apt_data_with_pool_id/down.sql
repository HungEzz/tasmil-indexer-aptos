-- This file should undo anything in `up.sql`

-- Drop the new table
DROP TABLE IF EXISTS apt_data;

-- Recreate the old table structure
CREATE TABLE apt_data (
    transaction_version BIGINT NOT NULL PRIMARY KEY,
    apt_price_usdc NUMERIC,
    apt_usdc_volume_24h NUMERIC,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Recreate indexes
CREATE INDEX idx_apt_data_transaction_version ON apt_data(transaction_version);
CREATE INDEX idx_apt_data_inserted_at ON apt_data(inserted_at);
