-- This file should undo anything in `up.sql`

-- Remove fee columns
-- This reverts the addition of fee tracking columns

-- Drop the fee columns
ALTER TABLE apt_data DROP COLUMN apt_fee_24h;
ALTER TABLE apt_data DROP COLUMN usdc_fee_24h;
