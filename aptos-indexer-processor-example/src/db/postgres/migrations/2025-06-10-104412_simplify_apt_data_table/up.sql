-- Drop existing tables
DROP TABLE IF EXISTS apt_prices;
DROP TABLE IF EXISTS apt_volume_24h;

-- Create simplified APT data table
CREATE TABLE apt_data (
    transaction_version BIGINT PRIMARY KEY,
    apt_price_usdc DECIMAL(20, 8),
    apt_usdc_volume_24h DECIMAL(20, 8),
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Index for efficient queries
CREATE INDEX idx_apt_data_transaction_version ON apt_data(transaction_version);
CREATE INDEX idx_apt_data_inserted_at ON apt_data(inserted_at);
