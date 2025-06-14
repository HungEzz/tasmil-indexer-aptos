-- This file should undo anything in `up.sql`

-- Drop WETH volume and fee columns from apt_data table

ALTER TABLE apt_data 
DROP COLUMN IF EXISTS weth_volume_24h,
DROP COLUMN IF EXISTS weth_fee_24h;
