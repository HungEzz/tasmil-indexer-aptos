-- Revert back to combined volume column
-- This reverses the separation of APT and USDC volumes

-- Add back the old column
ALTER TABLE apt_data ADD COLUMN total_apt_usdc_volume_24h NUMERIC DEFAULT 0;

-- Optionally, combine the values (though this will lose precision)
-- UPDATE apt_data SET total_apt_usdc_volume_24h = COALESCE(apt_volume_24h, 0) + COALESCE(usdc_volume_24h, 0);

-- Drop the new columns
ALTER TABLE apt_data DROP COLUMN apt_volume_24h;
ALTER TABLE apt_data DROP COLUMN usdc_volume_24h; 