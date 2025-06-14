-- Your SQL goes here

-- Remove abtc fields from apt_data table
ALTER TABLE apt_data DROP COLUMN IF EXISTS abtc_volume_24h;
ALTER TABLE apt_data DROP COLUMN IF EXISTS abtc_fee_24h;
