-- Your SQL goes here

-- Drop and recreate the apt_data table with protocol_name as primary key
DROP TABLE IF EXISTS apt_data;

CREATE TABLE apt_data (
    protocol_name VARCHAR PRIMARY KEY,
    inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    apt_volume_24h DECIMAL DEFAULT 0,
    usdc_volume_24h DECIMAL DEFAULT 0,
    apt_fee_24h DECIMAL DEFAULT 0,
    usdc_fee_24h DECIMAL DEFAULT 0,
    usdt_volume_24h DECIMAL DEFAULT 0,
    usdt_fee_24h DECIMAL DEFAULT 0
);

-- Insert initial data for the two protocols
INSERT INTO apt_data (protocol_name) VALUES ('cellana'), ('thala');
