-- Drop all tables created by the initial migration
DROP TABLE IF EXISTS apt_data CASCADE;
DROP TABLE IF EXISTS coin_volume_24h CASCADE;
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS backfill_processor_status CASCADE;
DROP TABLE IF EXISTS ledger_infos CASCADE; 