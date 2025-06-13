-- Your SQL goes here

-- Add aBTC volume and fee fields to apt_data table
ALTER TABLE apt_data 
ADD COLUMN abtc_volume_24h NUMERIC,
ADD COLUMN abtc_fee_24h NUMERIC;
