-- Your SQL goes here

-- Add WETH volume and fee columns to apt_data table for izWETH trading pair support

ALTER TABLE apt_data 
ADD COLUMN weth_volume_24h NUMERIC DEFAULT 0,
ADD COLUMN weth_fee_24h NUMERIC DEFAULT 0;
