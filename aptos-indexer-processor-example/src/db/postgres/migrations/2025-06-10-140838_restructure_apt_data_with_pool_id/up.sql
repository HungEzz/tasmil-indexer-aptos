-- Drop the old table
DROP TABLE IF EXISTS apt_data;

-- Create new table with pool as primary key
CREATE TABLE apt_data (
    pool VARCHAR NOT NULL PRIMARY KEY,
    total_apt_usdc_volume_24h NUMERIC DEFAULT 0,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create index for performance
CREATE INDEX idx_apt_data_inserted_at ON apt_data(inserted_at);

-- Insert initial data for known pools
INSERT INTO apt_data (pool, total_apt_usdc_volume_24h) VALUES 
('0x71c6ae634bd3c36470eb7e7f4fb0912973bb31543dfdb7d7fb6863d886d81d67', 0);
