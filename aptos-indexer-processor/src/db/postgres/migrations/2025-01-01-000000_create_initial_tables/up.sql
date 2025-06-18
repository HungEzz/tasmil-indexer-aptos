-- Initial database setup for Tasmil Aptos Indexer
-- This migration creates all necessary tables for the project

-- Create apt_data table to track volume and fee data by protocol
CREATE TABLE IF NOT EXISTS apt_data (
    protocol_name VARCHAR NOT NULL PRIMARY KEY,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    apt_volume_24h NUMERIC DEFAULT 0,
    usdc_volume_24h NUMERIC DEFAULT 0,
    apt_fee_24h NUMERIC DEFAULT 0,
    usdc_fee_24h NUMERIC DEFAULT 0,
    usdt_volume_24h NUMERIC DEFAULT 0,
    usdt_fee_24h NUMERIC DEFAULT 0,
    weth_volume_24h NUMERIC DEFAULT 0,
    weth_fee_24h NUMERIC DEFAULT 0
);

-- Create coin_volume_24h table to track buy/sell volumes for each coin
CREATE TABLE IF NOT EXISTS coin_volume_24h (
    coin VARCHAR NOT NULL PRIMARY KEY,
    buy_volume NUMERIC DEFAULT 0,
    sell_volume NUMERIC DEFAULT 0,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create coin_volume_buckets table for 2-hour time buckets (GMT+7)
-- Note: Protocol column removed as it's not used in the current implementation
CREATE TABLE IF NOT EXISTS coin_volume_buckets (
    coin VARCHAR NOT NULL,
    bucket_start TIMESTAMP NOT NULL,
    bucket_end TIMESTAMP NOT NULL,
    volume NUMERIC DEFAULT 0,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (coin, bucket_start)
);

-- Create events table for event processing
CREATE TABLE IF NOT EXISTS events (
    sequence_number BIGINT NOT NULL,
    creation_number BIGINT NOT NULL,
    account_address VARCHAR(66) NOT NULL,
    transaction_version BIGINT NOT NULL,
    transaction_block_height BIGINT NOT NULL,
    type TEXT NOT NULL,
    data JSONB NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    event_index BIGINT NOT NULL,
    indexed_type VARCHAR(300) NOT NULL,
    PRIMARY KEY (transaction_version, event_index)
);

-- Create processor_status table for tracking processor status
-- Note: Renamed from backfill_processor_status for consistency
CREATE TABLE IF NOT EXISTS processor_status (
    processor_name VARCHAR(50) NOT NULL PRIMARY KEY,
    last_success_version BIGINT NOT NULL,
    last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
    last_transaction_timestamp TIMESTAMP
);

-- Create ledger_infos table
CREATE TABLE IF NOT EXISTS ledger_infos (
    chain_id BIGINT NOT NULL PRIMARY KEY
);

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_apt_data_inserted_at ON apt_data(inserted_at);
CREATE INDEX IF NOT EXISTS idx_coin_volume_24h_inserted_at ON coin_volume_24h(inserted_at);
CREATE INDEX IF NOT EXISTS idx_coin_volume_buckets_coin ON coin_volume_buckets(coin);
CREATE INDEX IF NOT EXISTS idx_coin_volume_buckets_bucket_start ON coin_volume_buckets(bucket_start);
CREATE INDEX IF NOT EXISTS idx_coin_volume_buckets_bucket_end ON coin_volume_buckets(bucket_end);
CREATE INDEX IF NOT EXISTS idx_coin_volume_buckets_inserted_at ON coin_volume_buckets(inserted_at);
CREATE INDEX IF NOT EXISTS idx_events_transaction_version ON events(transaction_version);
CREATE INDEX IF NOT EXISTS idx_events_account_address ON events(account_address);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
CREATE INDEX IF NOT EXISTS idx_events_indexed_type ON events(indexed_type);
CREATE INDEX IF NOT EXISTS idx_events_inserted_at ON events(inserted_at);
CREATE INDEX IF NOT EXISTS idx_processor_status_last_updated ON processor_status(last_updated); 