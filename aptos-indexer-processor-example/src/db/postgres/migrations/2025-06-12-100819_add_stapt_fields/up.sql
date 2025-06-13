-- Your SQL goes here

-- Add stAPT volume and fee columns to apt_data table for APT/stAPT trading pair support

ALTER TABLE apt_data 
ADD COLUMN stapt_volume_24h NUMERIC DEFAULT 0,
ADD COLUMN stapt_fee_24h NUMERIC DEFAULT 0;
