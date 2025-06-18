use crate::{
    common::processor_status_saver::get_processor_status_saver,
    config::indexer_processor_config::IndexerProcessorConfig,
    processors::tasmil_processor::TasmilProcessor,
    utils::{
        chain_id::check_or_update_chain_id,
        database::{new_db_pool, run_migrations, ArcDbPool},
        starting_version::get_starting_version,
    },
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::{TransactionStream, TransactionStreamConfig},
    builder::ProcessorBuilder,
    common_steps::{
        TransactionStreamStep, VersionTrackerStep, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
    },
    traits::IntoRunnableStep,
};
use std::sync::mpsc;
use tracing::{info, warn};

pub struct SwapProcessor {
    pub config: IndexerProcessorConfig,
    pub db_pool: ArcDbPool,
}

impl SwapProcessor {
    pub async fn new(config: IndexerProcessorConfig) -> Result<Self> {
        info!("🚀 Initializing SwapProcessor for Cellana and Thala");
        info!("📊 Processor type: {}", config.processor_config.name());
        
        let conn_pool = new_db_pool(
            &config.db_config.postgres_connection_string,
            Some(config.db_config.db_pool_size),
        )
        .await
        .expect("Failed to create connection pool");
        
        info!("🔌 Database connection pool created with size: {}", config.db_config.db_pool_size);

        Ok(Self {
            config,
            db_pool: conn_pool,
        })
    }

    pub async fn run_processor(self) -> Result<()> {
        info!("▶️ Starting SwapProcessor for multi-protocol indexing");
        
        // Run migrations
        info!("🔄 Running database migrations");
        run_migrations(
            self.config.db_config.postgres_connection_string.clone(),
            self.db_pool.clone(),
        )
        .await;

        // Merge the starting version from config and the latest processed version from the DB
        let starting_version = get_starting_version(&self.config, self.db_pool.clone()).await?;
        info!("📌 Starting from version: {}", starting_version);

        // Check and update the ledger chain id to ensure we're indexing the correct chain
        info!("🔍 Verifying chain ID from gRPC service");
        let grpc_chain_id = TransactionStream::new(self.config.transaction_stream_config.clone())
            .await?
            .get_chain_id()
            .await?;
        info!("⛓️ Chain ID from gRPC: {}", grpc_chain_id);
        
        check_or_update_chain_id(grpc_chain_id as i64, self.db_pool.clone()).await?;

        // Define processor steps
        let transaction_stream_config = self.config.transaction_stream_config.clone();
        info!("🌐 Connecting to gRPC service: {}", transaction_stream_config.indexer_grpc_data_service_address);
        
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version: Some(starting_version),
            ..transaction_stream_config
        })
        .await?;

        // Create notification channel
        let (notification_sender, notification_receiver) = mpsc::channel();

        // Create TasmilProcessor
        info!("🔧 Setting up TasmilProcessor for Cellana and Thala");
        let tasmil_processor = TasmilProcessor::new(self.db_pool.clone(), notification_sender);
        
        let version_tracker = VersionTrackerStep::new(
            get_processor_status_saver(self.db_pool.clone(), self.config.clone()),
            DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
        );
        info!("⏱️ Version tracker update interval: {} seconds", DEFAULT_UPDATE_PROCESSOR_STATUS_SECS);

        // Connect processor steps together
        info!("🔗 Connecting processing pipeline components");
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(tasmil_processor.into_runnable_step(), 10)
        .connect_to(version_tracker.into_runnable_step(), 10)
        .end_and_return_output_receiver(10);

        info!("✅ Processing pipeline setup complete");
        info!("🔄 Starting continuous processing loop");

        // Process results
        loop {
            // Check for notifications
            while let Ok(notification) = notification_receiver.try_recv() {
                info!("📨 {}", notification);
            }

            match buffer_receiver.recv().await {
                Ok(txn_context) => {
                    info!(
                        "✅ Processed versions [{:?}, {:?}] successfully",
                        txn_context.metadata.start_version, txn_context.metadata.end_version,
                    );
                }
                Err(e) => {
                    warn!("❌ Channel error: {}", e);
                    info!("🛑 Processing loop terminated");
                    return Ok(());
                }
            }
        }
    }
} 