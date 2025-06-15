-- Create coin_volume_24h table to track buy/sell volumes for each coin on each protocol
CREATE TABLE coin_volume_24h (
    id SERIAL PRIMARY KEY,
    protocol_name VARCHAR NOT NULL,
    coin VARCHAR NOT NULL,
    buy_volume NUMERIC DEFAULT 0,
    sell_volume NUMERIC DEFAULT 0,
    inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent duplicate entries for same protocol+coin combination
    UNIQUE(protocol_name, coin)
);

-- Create indexes for efficient queries
CREATE INDEX idx_coin_volume_24h_protocol_name ON coin_volume_24h(protocol_name);
CREATE INDEX idx_coin_volume_24h_coin ON coin_volume_24h(coin);
CREATE INDEX idx_coin_volume_24h_inserted_at ON coin_volume_24h(inserted_at);
CREATE INDEX idx_coin_volume_24h_protocol_coin ON coin_volume_24h(protocol_name, coin);
