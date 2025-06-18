-- Migration to update coin_volume_24h table structure
-- Remove protocol_name and id fields, make coin the primary key

BEGIN;

-- First, aggregate existing data by coin (sum all protocols)
CREATE TEMP TABLE coin_volume_aggregated AS
SELECT 
    coin,
    SUM(COALESCE(buy_volume, 0)) as total_buy_volume,
    SUM(COALESCE(sell_volume, 0)) as total_sell_volume,
    MAX(inserted_at) as latest_inserted_at
FROM coin_volume_24h 
GROUP BY coin;

-- Drop the old table
DROP TABLE IF EXISTS coin_volume_24h CASCADE;

-- Create new table with coin as primary key
CREATE TABLE coin_volume_24h (
    coin VARCHAR NOT NULL PRIMARY KEY,
    buy_volume NUMERIC,
    sell_volume NUMERIC,
    inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Insert aggregated data into new table
INSERT INTO coin_volume_24h (coin, buy_volume, sell_volume, inserted_at)
SELECT 
    coin,
    CASE WHEN total_buy_volume > 0 THEN total_buy_volume ELSE NULL END,
    CASE WHEN total_sell_volume > 0 THEN total_sell_volume ELSE NULL END,
    latest_inserted_at
FROM coin_volume_aggregated;

-- Create indexes for performance
CREATE INDEX idx_coin_volume_24h_inserted_at ON coin_volume_24h(inserted_at);
CREATE INDEX idx_coin_volume_24h_buy_volume ON coin_volume_24h(buy_volume);
CREATE INDEX idx_coin_volume_24h_sell_volume ON coin_volume_24h(sell_volume);

COMMIT; 