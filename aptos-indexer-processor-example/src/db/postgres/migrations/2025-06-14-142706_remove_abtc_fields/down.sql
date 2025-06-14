-- This file should undo anything in `up.sql`

-- Add back abtc fields to apt_data table (rollback)
ALTER TABLE apt_data ADD COLUMN abtc_volume_24h NUMERIC;
ALTER TABLE apt_data ADD COLUMN abtc_fee_24h NUMERIC;
