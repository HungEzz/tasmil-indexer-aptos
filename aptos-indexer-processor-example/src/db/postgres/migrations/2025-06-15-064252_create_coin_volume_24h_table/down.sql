-- This file should undo anything in `up.sql`

-- Drop indexes first
DROP INDEX IF EXISTS idx_coin_volume_24h_protocol_coin;
DROP INDEX IF EXISTS idx_coin_volume_24h_inserted_at;
DROP INDEX IF EXISTS idx_coin_volume_24h_coin;
DROP INDEX IF EXISTS idx_coin_volume_24h_protocol_name;

-- Drop the table
DROP TABLE IF EXISTS coin_volume_24h;
