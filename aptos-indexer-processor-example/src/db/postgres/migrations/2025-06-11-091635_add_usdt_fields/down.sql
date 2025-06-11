-- This file should undo anything in `up.sql`

-- Remove USDT volume and fee columns from apt_data table
ALTER TABLE apt_data 
DROP COLUMN usdt_volume_24h,
DROP COLUMN usdt_fee_24h;
