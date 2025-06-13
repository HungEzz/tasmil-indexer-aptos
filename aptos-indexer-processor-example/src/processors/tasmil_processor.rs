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
use tracing::{error, info, warn, debug};

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
                        apt_data::apt_volume_24h.eq(Some(BigDecimal::zero())),
                        apt_data::usdc_volume_24h.eq(Some(BigDecimal::zero())),
                        apt_data::usdt_volume_24h.eq(Some(BigDecimal::zero())),
                        apt_data::stapt_volume_24h.eq(Some(BigDecimal::zero())),
                        apt_data::abtc_volume_24h.eq(Some(BigDecimal::zero())),
                        apt_data::apt_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::usdc_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::usdt_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::stapt_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::abtc_fee_24h.eq(Some(BigDecimal::zero())),
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

    async fn get_current_volumes(&self, protocol_name: &str) -> Result<(BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal), ProcessorError> {
        let mut conn = self.connection_pool.get().await.map_err(|e| {
            ProcessorError::ProcessError {
                message: format!("Failed to get database connection: {}", e),
            }
        })?;

        let current_data: Option<AptData> = apt_data::table
            .filter(apt_data::protocol_name.eq(protocol_name))
            .first(&mut conn)
            .await
            .optional()
            .map_err(|e| {
                ProcessorError::ProcessError {
                    message: format!("Failed to query current data: {}", e),
                }
            })?;

        let zero_decimal = BigDecimal::zero();
        match current_data {
            Some(data) => {
                let current_apt_volume = data.apt_volume_24h.unwrap_or_else(|| zero_decimal.clone());
                let current_usdc_volume = data.usdc_volume_24h.unwrap_or_else(|| zero_decimal.clone());
                let current_usdt_volume = data.usdt_volume_24h.unwrap_or_else(|| zero_decimal.clone());
                let current_stapt_volume = data.stapt_volume_24h.unwrap_or_else(|| zero_decimal.clone());
                let current_abtc_volume = data.abtc_volume_24h.unwrap_or_else(|| zero_decimal.clone());
                let current_apt_fee = data.apt_fee_24h.unwrap_or_else(|| zero_decimal.clone());
                let current_usdc_fee = data.usdc_fee_24h.unwrap_or_else(|| zero_decimal.clone());
                let current_usdt_fee = data.usdt_fee_24h.unwrap_or_else(|| zero_decimal.clone());
                let current_stapt_fee = data.stapt_fee_24h.unwrap_or_else(|| zero_decimal.clone());
                let current_abtc_fee = data.abtc_fee_24h.unwrap_or_else(|| zero_decimal.clone());
                
                debug!("📊 Current volumes for {}: APT={}, USDC={}, USDT={}, stAPT={}, aBTC={}, APT_fee={}, USDC_fee={}, USDT_fee={}, stAPT_fee={}, aBTC_fee={}", 
                    protocol_name, current_apt_volume, current_usdc_volume, current_usdt_volume, current_stapt_volume, current_abtc_volume,
                    current_apt_fee, current_usdc_fee, current_usdt_fee, current_stapt_fee, current_abtc_fee);
                    
                Ok((current_apt_volume, current_usdc_volume, current_usdt_volume, current_stapt_volume, current_abtc_volume, current_apt_fee, current_usdc_fee, current_usdt_fee, current_stapt_fee, current_abtc_fee))
            },
            None => {
                debug!("📊 No existing data for protocol {}, starting from zero", protocol_name);
                Ok((zero_decimal.clone(), zero_decimal.clone(), zero_decimal.clone(), zero_decimal.clone(), zero_decimal.clone(), zero_decimal.clone(), zero_decimal.clone(), zero_decimal.clone(), zero_decimal.clone(), zero_decimal))
            }
        }
    }

    async fn upsert_pool_volumes(&self, volume_data: Vec<NewAptData>) -> Result<(), ProcessorError> {
        if volume_data.is_empty() {
            info!("📊 No volume data to update");
            return Ok(());
        }

        let mut conn = self.connection_pool.get().await.map_err(|e| {
            ProcessorError::ProcessError {
                message: format!("Failed to get database connection: {}", e),
            }
        })?;

        for record in &volume_data {
            let zero_decimal = BigDecimal::zero();
            let batch_apt_volume = record.apt_volume_24h.as_ref().unwrap_or(&zero_decimal);
            let batch_usdc_volume = record.usdc_volume_24h.as_ref().unwrap_or(&zero_decimal);
            let batch_usdt_volume = record.usdt_volume_24h.as_ref().unwrap_or(&zero_decimal);
            let batch_stapt_volume = record.stapt_volume_24h.as_ref().unwrap_or(&zero_decimal);
            let batch_abtc_volume = record.abtc_volume_24h.as_ref().unwrap_or(&zero_decimal);
            let batch_apt_fee = record.apt_fee_24h.as_ref().unwrap_or(&zero_decimal);
            let batch_usdc_fee = record.usdc_fee_24h.as_ref().unwrap_or(&zero_decimal);
            let batch_usdt_fee = record.usdt_fee_24h.as_ref().unwrap_or(&zero_decimal);
            let batch_stapt_fee = record.stapt_fee_24h.as_ref().unwrap_or(&zero_decimal);
            let batch_abtc_fee = record.abtc_fee_24h.as_ref().unwrap_or(&zero_decimal);
            
            // Get current volumes and fees first
            let (current_apt_volume, current_usdc_volume, current_usdt_volume, current_stapt_volume, current_abtc_volume, current_apt_fee, current_usdc_fee, current_usdt_fee, current_stapt_fee, current_abtc_fee) = 
                self.get_current_volumes(&record.protocol_name).await?;
            
            // Accumulate both volumes and fees
            let new_apt_volume = &current_apt_volume + batch_apt_volume;
            let new_usdc_volume = &current_usdc_volume + batch_usdc_volume;
            let new_usdt_volume = &current_usdt_volume + batch_usdt_volume;
            let new_stapt_volume = &current_stapt_volume + batch_stapt_volume;
            let new_abtc_volume = &current_abtc_volume + batch_abtc_volume;
            let new_apt_fee = &current_apt_fee + batch_apt_fee;
            let new_usdc_fee = &current_usdc_fee + batch_usdc_fee;
            let new_usdt_fee = &current_usdt_fee + batch_usdt_fee;
            let new_stapt_fee = &current_stapt_fee + batch_stapt_fee;
            let new_abtc_fee = &current_abtc_fee + batch_abtc_fee;
            
            // UPSERT: INSERT or UPDATE if protocol exists
            match diesel::insert_into(apt_data::table)
                .values(&NewAptData {
                    protocol_name: record.protocol_name.clone(),
                    apt_volume_24h: Some(new_apt_volume.clone()),
                    usdc_volume_24h: Some(new_usdc_volume.clone()),
                    usdt_volume_24h: Some(new_usdt_volume.clone()),
                    stapt_volume_24h: Some(new_stapt_volume.clone()),
                    abtc_volume_24h: Some(new_abtc_volume.clone()),
                    apt_fee_24h: Some(new_apt_fee.clone()),
                    usdc_fee_24h: Some(new_usdc_fee.clone()),
                    usdt_fee_24h: Some(new_usdt_fee.clone()),
                    stapt_fee_24h: Some(new_stapt_fee.clone()),
                    abtc_fee_24h: Some(new_abtc_fee.clone()),
                })
                .on_conflict(apt_data::protocol_name)
                .do_update()
                .set((
                    apt_data::apt_volume_24h.eq(excluded(apt_data::apt_volume_24h)),
                    apt_data::usdc_volume_24h.eq(excluded(apt_data::usdc_volume_24h)),
                    apt_data::usdt_volume_24h.eq(excluded(apt_data::usdt_volume_24h)),
                    apt_data::stapt_volume_24h.eq(excluded(apt_data::stapt_volume_24h)),
                    apt_data::abtc_volume_24h.eq(excluded(apt_data::abtc_volume_24h)),
                    apt_data::apt_fee_24h.eq(excluded(apt_data::apt_fee_24h)),
                    apt_data::usdc_fee_24h.eq(excluded(apt_data::usdc_fee_24h)),
                    apt_data::usdt_fee_24h.eq(excluded(apt_data::usdt_fee_24h)),
                    apt_data::stapt_fee_24h.eq(excluded(apt_data::stapt_fee_24h)),
                    apt_data::abtc_fee_24h.eq(excluded(apt_data::abtc_fee_24h)),
                    apt_data::inserted_at.eq(diesel::dsl::now)
                ))
                .execute(&mut conn)
                .await
            {
                Ok(_) => {
                    info!("✅ Updated rolling data for protocol {}: APT vol +{} (total: {}), USDC vol +{} (total: {}), USDT vol +{} (total: {}), stAPT vol +{} (total: {}), aBTC vol +{} (total: {}), APT fee +{} (total: {}), USDC fee +{} (total: {}), USDT fee +{} (total: {}), stAPT fee +{} (total: {}), aBTC fee +{} (total: {})", 
                        record.protocol_name, 
                        batch_apt_volume, new_apt_volume, 
                        batch_usdc_volume, new_usdc_volume,
                        batch_usdt_volume, new_usdt_volume,
                        batch_stapt_volume, new_stapt_volume,
                        batch_abtc_volume, new_abtc_volume,
                        batch_apt_fee, new_apt_fee,
                        batch_usdc_fee, new_usdc_fee,
                        batch_usdt_fee, new_usdt_fee,
                        batch_stapt_fee, new_stapt_fee,
                        batch_abtc_fee, new_abtc_fee);
                },
                Err(e) => {
                    error!("❌ Failed to update data for protocol {}: {}", record.protocol_name, e);
                    return Err(ProcessorError::ProcessError {
                        message: format!("Data update failed: {}", e),
                    });
                }
            }
        }

        info!("✅ Successfully processed {} pool records", volume_data.len());
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
                        apt_data::apt_volume_24h.eq(Some(BigDecimal::zero())),
                        apt_data::usdc_volume_24h.eq(Some(BigDecimal::zero())),
                        apt_data::usdt_volume_24h.eq(Some(BigDecimal::zero())),
                        apt_data::stapt_volume_24h.eq(Some(BigDecimal::zero())),
                        apt_data::abtc_volume_24h.eq(Some(BigDecimal::zero())),
                        apt_data::apt_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::usdc_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::usdt_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::stapt_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::abtc_fee_24h.eq(Some(BigDecimal::zero())),
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

        // Cleanup old data (older than 24 hours) FIRST before processing new data
        self.cleanup_old_data().await?;

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