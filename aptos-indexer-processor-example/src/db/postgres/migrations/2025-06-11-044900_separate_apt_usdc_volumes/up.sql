-- Separate APT and USDC volumes into individual columns
-- This migration replaces total_apt_usdc_volume_24h with apt_volume_24h and usdc_volume_24h

-- Add new columns
ALTER TABLE apt_data ADD COLUMN apt_volume_24h NUMERIC DEFAULT 0;
ALTER TABLE apt_data ADD COLUMN usdc_volume_24h NUMERIC DEFAULT 0;

-- Drop the old column
ALTER TABLE apt_data DROP COLUMN total_apt_usdc_volume_24h;

-- Reset all volumes to 0 for fresh calculation
UPDATE apt_data SET apt_volume_24h = 0, usdc_volume_24h = 0; 