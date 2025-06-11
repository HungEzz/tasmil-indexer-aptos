-- Your SQL goes here

-- Add USDT volume and fee columns to apt_data table
ALTER TABLE apt_data 
ADD COLUMN usdt_volume_24h NUMERIC,
ADD COLUMN usdt_fee_24h NUMERIC;
