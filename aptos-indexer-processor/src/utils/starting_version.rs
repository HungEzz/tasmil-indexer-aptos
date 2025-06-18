use super::database::ArcDbPool;
use crate::config::indexer_processor_config::IndexerProcessorConfig;
use anyhow::Result;
use tracing::info;

/// Get the appropriate starting version for the processor (simplified for Tasmil).
/// This will return the `starting_version` from the config, or 0 if not set.
pub async fn get_starting_version(
    indexer_processor_config: &IndexerProcessorConfig,
    _conn_pool: ArcDbPool,
) -> Result<u64> {
    let starting_version = indexer_processor_config
        .transaction_stream_config
        .starting_version
        .unwrap_or(0);
    
    info!("🚀 Using starting version: {} for Tasmil indexer", starting_version);
    Ok(starting_version)
}
