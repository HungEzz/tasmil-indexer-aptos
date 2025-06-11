use crate::{
    config::indexer_processor_config::IndexerProcessorConfig,
    utils::database::ArcDbPool,
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    common_steps::ProcessorStatusSaver,
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use tracing::info;

/// Get a simplified processor status saver that only logs versions.
pub fn get_processor_status_saver(
    _db_pool: ArcDbPool,
    config: IndexerProcessorConfig,
) -> SimpleTasmilProcessorStatusSaver {
    SimpleTasmilProcessorStatusSaver {
        processor_name: config.processor_config.name().to_string(),
    }
}

pub struct SimpleTasmilProcessorStatusSaver {
    processor_name: String,
}

#[async_trait]
impl ProcessorStatusSaver for SimpleTasmilProcessorStatusSaver {
    async fn save_processor_status(
        &self,
        last_success_batch: &TransactionContext<()>,
    ) -> Result<(), ProcessorError> {
        // Simple logging-based status tracking for Tasmil project
        info!(
            "🔄 {} processed successfully up to version: {} (batch size: {})",
            self.processor_name,
            last_success_batch.metadata.end_version,
            last_success_batch.metadata.end_version - last_success_batch.metadata.start_version + 1
        );
        
        Ok(())
    }
}
