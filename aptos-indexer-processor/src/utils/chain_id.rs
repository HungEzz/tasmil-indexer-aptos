use super::database::ArcDbPool;
use anyhow::Result;
use tracing::info;

/// Verify the chain id from GRPC (simplified for Tasmil project).
pub async fn check_or_update_chain_id(grpc_chain_id: i64, _db_pool: ArcDbPool) -> Result<u64> {
    info!("✅ Using chain ID: {} for Tasmil indexer", grpc_chain_id);
    Ok(grpc_chain_id as u64)
}
