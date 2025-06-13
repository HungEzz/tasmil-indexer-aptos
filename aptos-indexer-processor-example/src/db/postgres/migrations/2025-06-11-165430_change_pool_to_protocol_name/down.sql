-- This file should undo anything in `up.sql`

-- Rollback: Restore the old table structure with pool as primary key
DROP TABLE IF EXISTS apt_data;

CREATE TABLE apt_data (
    pool VARCHAR PRIMARY KEY,
    inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    apt_volume_24h DECIMAL,
    usdc_volume_24h DECIMAL,
    apt_fee_24h DECIMAL,
    usdc_fee_24h DECIMAL,
    usdt_volume_24h DECIMAL,
    usdt_fee_24h DECIMAL
);
