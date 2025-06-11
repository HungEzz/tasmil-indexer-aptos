-- Your SQL goes here

-- Set default values for USDT columns
ALTER TABLE apt_data ALTER COLUMN usdt_volume_24h SET DEFAULT 0;
ALTER TABLE apt_data ALTER COLUMN usdt_fee_24h SET DEFAULT 0;

-- Update existing NULL values to 0
UPDATE apt_data SET usdt_volume_24h = 0 WHERE usdt_volume_24h IS NULL;
UPDATE apt_data SET usdt_fee_24h = 0 WHERE usdt_fee_24h IS NULL;
