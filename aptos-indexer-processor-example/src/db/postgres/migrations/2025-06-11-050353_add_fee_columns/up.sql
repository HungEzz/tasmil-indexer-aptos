-- Your SQL goes here

-- Add fee columns for APT and USDC 24h fees
-- This tracks trading fees earned by the pool in the last 24 hours

-- Add new columns for fees
ALTER TABLE apt_data ADD COLUMN apt_fee_24h NUMERIC DEFAULT 0;
ALTER TABLE apt_data ADD COLUMN usdc_fee_24h NUMERIC DEFAULT 0;

-- Reset all fees to 0 for fresh calculation
UPDATE apt_data SET apt_fee_24h = 0, usdc_fee_24h = 0;
