-- This file should undo anything in `up.sql`

-- Remove stAPT volume and fee columns from apt_data table

ALTER TABLE apt_data 
DROP COLUMN IF EXISTS stapt_volume_24h,
DROP COLUMN IF EXISTS stapt_fee_24h;
