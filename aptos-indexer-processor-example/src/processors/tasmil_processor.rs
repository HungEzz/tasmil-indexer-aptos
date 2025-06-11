use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncStep, NamedStep, processable::Processable, AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use bigdecimal::{BigDecimal, Zero};
use chrono::{Utc, Duration, DateTime};
use diesel::{ExpressionMethods, QueryDsl, upsert::excluded, OptionalExtension};
use diesel_async::RunQueryDsl;
use std::sync::mpsc;
use tracing::{error, info, warn};

use crate::db::common::models::apt_models::{AptData, NewAptData};
use crate::schema::apt_data;
use crate::processors::events::volume_calculator::VolumeCalculator;
use crate::utils::database::ArcDbPool;

pub struct TasmilProcessor {
    connection_pool: ArcDbPool,
    volume_calculator: VolumeCalculator,
    sender: mpsc::Sender<String>,
}

impl TasmilProcessor {
    pub fn new(connection_pool: ArcDbPool, sender: mpsc::Sender<String>) -> Self {
        info!("🚀 Creating TasmilProcessor with Rolling 24h Volume Logic");
        
        let processor = Self {
            connection_pool: connection_pool.clone(),
            volume_calculator: VolumeCalculator::new(),
            sender,
        };

        // Reset volume on startup for fresh calculation
        let pool = connection_pool.clone();
        tokio::spawn(async move {
            if let Ok(mut conn) = pool.get().await {
                info!("🔄 Resetting volume to 0 on startup for fresh 24h calculation...");
                
                match diesel::update(apt_data::table)
                    .set((
                        apt_data::total_apt_usdc_volume_24h.eq(Some(BigDecimal::zero())),
                        apt_data::inserted_at.eq(diesel::dsl::now)
                    ))
                    .execute(&mut conn)
                    .await
                {
                    Ok(updated_count) => {
                        info!("✅ Reset {} pool volumes to 0", updated_count);
                    },
                    Err(e) => {
                        error!("❌ Failed to reset volumes: {}", e);
                    }
                }
            }
        });
        
        processor
    }

    async fn get_current_volume(&self, pool_address: &str) -> Result<BigDecimal, ProcessorError> {
        let mut conn = self.connection_pool.get().await.map_err(|e| {
            ProcessorError::ProcessError {
                message: format!("Failed to get database connection: {}", e),
            }
        })?;

        let current_volume: Option<BigDecimal> = apt_data::table
            .filter(apt_data::pool.eq(pool_address))
            .select(apt_data::total_apt_usdc_volume_24h)
            .first(&mut conn)
            .await
            .optional()
            .map_err(|e| {
                ProcessorError::ProcessError {
                    message: format!("Failed to get current volume: {}", e),
                }
            })?
            .flatten();

        Ok(current_volume.unwrap_or_else(|| BigDecimal::zero()))
    }

    async fn upsert_pool_volumes(&self, volume_data: Vec<NewAptData>) -> Result<(), ProcessorError> {
        if volume_data.is_empty() {
            info!("📊 No volume data to process");
            return Ok(());
        }

        let mut conn = self.connection_pool.get().await.map_err(|e| {
            ProcessorError::ProcessError {
                message: format!("Failed to get database connection: {}", e),
            }
        })?;

        info!("💾 Processing {} pool volume records with PROPER UPSERT", volume_data.len());

        for record in &volume_data {
            let batch_volume = record.total_apt_usdc_volume_24h.as_ref().unwrap();
            
            // Get current volume first
            let current_volume = self.get_current_volume(&record.pool).await?;
            let new_volume = &current_volume + batch_volume;
            
            // UPSERT: INSERT or UPDATE if pool exists
            match diesel::insert_into(apt_data::table)
                .values(&NewAptData {
                    pool: record.pool.clone(),
                    total_apt_usdc_volume_24h: Some(new_volume.clone()),
                })
                .on_conflict(apt_data::pool)
                .do_update()
                .set((
                    apt_data::total_apt_usdc_volume_24h.eq(excluded(apt_data::total_apt_usdc_volume_24h)),
                    apt_data::inserted_at.eq(diesel::dsl::now)
                ))
                .execute(&mut conn)
                .await
            {
                Ok(_) => {
                    info!("✅ Updated rolling volume for pool {}: +{} USD (total: {} USD)", 
                        record.pool, batch_volume, new_volume);
                },
                Err(e) => {
                    error!("❌ Failed to update volume for pool {}: {}", record.pool, e);
                    return Err(ProcessorError::ProcessError {
                        message: format!("Volume update failed: {}", e),
                    });
                }
            }
        }

        info!("✅ Successfully processed {} pool volume records", volume_data.len());
        Ok(())
    }

    async fn cleanup_old_data(&self) -> Result<(), ProcessorError> {
        let mut conn = self.connection_pool.get().await.map_err(|e| {
            ProcessorError::ProcessError {
                message: format!("Failed to get database connection for cleanup: {}", e),
            }
        })?;

        // Calculate cutoff time (24 hours ago)
        let now = Utc::now();
        let cutoff_time = now - Duration::hours(24);
        
        info!("🧹 Checking for volume reset (24h cutoff: {})", cutoff_time.format("%Y-%m-%d %H:%M:%S UTC"));

        // Get all records to check if we need to reset the rolling window
        let current_records: Vec<AptData> = apt_data::table
            .load(&mut conn)
            .await
            .map_err(|e| {
                ProcessorError::ProcessError {
                    message: format!("Failed to load current records: {}", e),
                }
            })?;

        if current_records.is_empty() {
            info!("📝 No existing records found");
            return Ok(());
        }

        // Check if the last update was more than 24 hours ago
        // Since we update inserted_at on every upsert, if it's old, it means no new data
        let latest_update = current_records
            .iter()
            .map(|r| r.inserted_at)
            .max();

        if let Some(latest) = latest_update {
            let latest_utc = DateTime::<Utc>::from_naive_utc_and_offset(latest, Utc);
            
            if latest_utc < cutoff_time {
                info!("🔄 Last update was {} (>24h ago), resetting volumes for new window", 
                    latest_utc.format("%Y-%m-%d %H:%M:%S UTC"));
                
                match diesel::update(apt_data::table)
                    .set((
                        apt_data::total_apt_usdc_volume_24h.eq(Some(BigDecimal::zero())),
                        apt_data::inserted_at.eq(diesel::dsl::now)
                    ))
                    .execute(&mut conn)
                    .await
                {
                    Ok(updated_count) => {
                        info!("✅ Reset {} pool volumes for new 24h window", updated_count);
                    },
                    Err(e) => {
                        error!("❌ Failed to reset volumes: {}", e);
                    }
                }
            } else {
                info!("✅ Volume data is recent (last update: {}), continuing accumulation", 
                    latest_utc.format("%Y-%m-%d %H:%M:%S UTC"));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Processable for TasmilProcessor {
    type Input = Vec<Transaction>;
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        info!(
            "🔥 TasmilProcessor processing batch: versions [{}, {}], {} transactions",
            item.metadata.start_version, item.metadata.end_version, item.data.len()
        );

        // Calculate volume data using VolumeCalculator (with 24h filtering)
        let volume_context = match self.volume_calculator.process(item.clone()).await? {
            Some(ctx) => ctx,
            None => {
                info!("📊 No volume data calculated");
                return Ok(Some(TransactionContext {
                    data: (),
                    metadata: item.metadata,
                }));
            }
        };

        // Insert new volume records to database
        self.upsert_pool_volumes(volume_context.data).await?;

        // Cleanup old data (older than 24 hours) 
        self.cleanup_old_data().await?;

        // Send notification
        if let Err(e) = self.sender.send(format!(
            "Processed {} transactions (versions {}-{})",
            item.data.len(),
            item.metadata.start_version,
            item.metadata.end_version
        )) {
            warn!("📨 Failed to send notification: {}", e);
        }

        Ok(Some(TransactionContext {
            data: (),
            metadata: item.metadata,
        }))
    }
}

impl AsyncStep for TasmilProcessor {}

impl NamedStep for TasmilProcessor {
    fn name(&self) -> String {
        "TasmilProcessor".to_string()
    }
}