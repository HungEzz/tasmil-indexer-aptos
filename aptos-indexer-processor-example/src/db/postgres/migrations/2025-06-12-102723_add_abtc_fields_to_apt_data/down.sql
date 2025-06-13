-- This file should undo anything in `up.sql`

-- Remove aBTC volume and fee fields from apt_data table
ALTER TABLE apt_data 
DROP COLUMN IF EXISTS abtc_volume_24h,
DROP COLUMN IF EXISTS abtc_fee_24h;
