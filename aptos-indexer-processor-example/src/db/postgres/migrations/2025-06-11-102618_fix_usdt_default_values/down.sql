-- This file should undo anything in `up.sql`

-- Remove default values for USDT columns
ALTER TABLE apt_data ALTER COLUMN usdt_volume_24h DROP DEFAULT;
ALTER TABLE apt_data ALTER COLUMN usdt_fee_24h DROP DEFAULT;
