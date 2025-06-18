use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncStep, NamedStep, processable::Processable, AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use bigdecimal::{BigDecimal, Zero};
use chrono::{Utc, Duration, DateTime, NaiveDateTime};
use diesel::{ExpressionMethods, QueryDsl, upsert::excluded, OptionalExtension};
use diesel_async::RunQueryDsl;
use std::sync::mpsc;
use tracing::{error, info, warn, debug};

use crate::{
    db::{
        common::models::{
            apt_models::{AptData, NewAptData},
            coin_volume_models::{NewCoinVolume24h, CoinVolume24h, NewCoinVolumeBucket, CoinVolumeBucket},
        },
        postgres::schema::{apt_data, coin_volume_24h, coin_volume_buckets},
    },
    processors::events::{
        volume_calculator::VolumeCalculator,
    },
    utils::{
        database::ArcDbPool,
    },
};

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
                        apt_data::weth_volume_24h.eq(Some(BigDecimal::zero())),
                        apt_data::apt_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::usdc_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::usdt_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::weth_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::inserted_at.eq(diesel::dsl::now)
                    ))
                    .execute(&mut conn)
                    .await
                {
                    Ok(updated_count) => {
                        info!("✅ Reset {} pool volumes to 0 (including 'aptos' aggregated data)", updated_count);
                    },
                    Err(e) => {
                        error!("❌ Failed to reset volumes: {}", e);
                    }
                }

                // Also reset coin volumes
                match diesel::update(coin_volume_24h::table)
                    .set((
                        coin_volume_24h::buy_volume.eq(Some(BigDecimal::zero())),
                        coin_volume_24h::sell_volume.eq(Some(BigDecimal::zero())),
                        coin_volume_24h::inserted_at.eq(diesel::dsl::now)
                    ))
                    .execute(&mut conn)
                    .await
                {
                    Ok(updated_count) => {
                        info!("✅ Reset {} coin volumes to 0", updated_count);
                    },
                    Err(e) => {
                        error!("❌ Failed to reset coin volumes: {}", e);
                    }
                }

                // Reset coin volume buckets on startup
                match diesel::delete(coin_volume_buckets::table)
                    .execute(&mut conn)
                    .await
                {
                    Ok(deleted_count) => {
                        info!("✅ Deleted {} coin volume bucket records on startup for fresh calculation", deleted_count);
                    },
                    Err(e) => {
                        error!("❌ Failed to reset coin volume buckets on startup: {}", e);
                    }
                }
            }
        });
        
        processor
    }

    async fn get_current_volumes(&self, protocol_name: &str) -> Result<(BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal), ProcessorError> {
        let mut conn = self.connection_pool.get().await.map_err(|e| {
            ProcessorError::ProcessError {
                message: format!("Failed to get database connection: {}", e),
            }
        })?;

        let zero_decimal = BigDecimal::zero();
        
        let data = apt_data::table
            .filter(apt_data::protocol_name.eq(protocol_name))
            .first::<AptData>(&mut conn)
            .await
            .optional()
            .map_err(|e| ProcessorError::ProcessError {
                message: format!("Failed to get current volumes for {}: {}", protocol_name, e),
            })?;

        let (current_apt_volume, current_usdc_volume, current_usdt_volume, current_weth_volume, current_apt_fee, current_usdc_fee, current_usdt_fee, current_weth_fee) = if let Some(data) = data {
            let current_apt_volume = data.apt_volume_24h.unwrap_or_else(|| zero_decimal.clone());
            let current_usdc_volume = data.usdc_volume_24h.unwrap_or_else(|| zero_decimal.clone());
            let current_usdt_volume = data.usdt_volume_24h.unwrap_or_else(|| zero_decimal.clone());
            let current_weth_volume = data.weth_volume_24h.unwrap_or_else(|| zero_decimal.clone());
            let current_apt_fee = data.apt_fee_24h.unwrap_or_else(|| zero_decimal.clone());
            let current_usdc_fee = data.usdc_fee_24h.unwrap_or_else(|| zero_decimal.clone());
            let current_usdt_fee = data.usdt_fee_24h.unwrap_or_else(|| zero_decimal.clone());
            let current_weth_fee = data.weth_fee_24h.unwrap_or_else(|| zero_decimal.clone());

            debug!("📊 Current volumes for {}: APT={}, USDC={}, USDT={}, WETH={}, APT_fee={}, USDC_fee={}, USDT_fee={}, WETH_fee={}",
                protocol_name, current_apt_volume, current_usdc_volume, current_usdt_volume, current_weth_volume,
                current_apt_fee, current_usdc_fee, current_usdt_fee, current_weth_fee);

            (current_apt_volume, current_usdc_volume, current_usdt_volume, current_weth_volume, current_apt_fee, current_usdc_fee, current_usdt_fee, current_weth_fee)
        } else {
            (zero_decimal.clone(), zero_decimal.clone(), zero_decimal.clone(), zero_decimal.clone(), zero_decimal.clone(), zero_decimal.clone(), zero_decimal.clone(), zero_decimal.clone())
        };

        Ok((current_apt_volume, current_usdc_volume, current_usdt_volume, current_weth_volume, current_apt_fee, current_usdc_fee, current_usdt_fee, current_weth_fee))
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
            let batch_weth_volume = record.weth_volume_24h.as_ref().unwrap_or(&zero_decimal);
            let batch_apt_fee = record.apt_fee_24h.as_ref().unwrap_or(&zero_decimal);
            let batch_usdc_fee = record.usdc_fee_24h.as_ref().unwrap_or(&zero_decimal);
            let batch_usdt_fee = record.usdt_fee_24h.as_ref().unwrap_or(&zero_decimal);
            let batch_weth_fee = record.weth_fee_24h.as_ref().unwrap_or(&zero_decimal);
            
            // Get current volumes and fees first
            let (current_apt_volume, current_usdc_volume, current_usdt_volume, current_weth_volume, current_apt_fee, current_usdc_fee, current_usdt_fee, current_weth_fee) = 
                self.get_current_volumes(&record.protocol_name).await?;
            
            // Accumulate both volumes and fees
            let new_apt_volume = &current_apt_volume + batch_apt_volume;
            let new_usdc_volume = &current_usdc_volume + batch_usdc_volume;
            let new_usdt_volume = &current_usdt_volume + batch_usdt_volume;
            let new_weth_volume = &current_weth_volume + batch_weth_volume;
            let new_apt_fee = &current_apt_fee + batch_apt_fee;
            let new_usdc_fee = &current_usdc_fee + batch_usdc_fee;
            let new_usdt_fee = &current_usdt_fee + batch_usdt_fee;
            let new_weth_fee = &current_weth_fee + batch_weth_fee;
            
            // UPSERT: INSERT or UPDATE if protocol exists
            match diesel::insert_into(apt_data::table)
                .values(&NewAptData {
                    protocol_name: record.protocol_name.clone(),
                    apt_volume_24h: Some(new_apt_volume.clone()),
                    usdc_volume_24h: Some(new_usdc_volume.clone()),
                    usdt_volume_24h: Some(new_usdt_volume.clone()),
                    weth_volume_24h: Some(new_weth_volume.clone()),
                    apt_fee_24h: Some(new_apt_fee.clone()),
                    usdc_fee_24h: Some(new_usdc_fee.clone()),
                    usdt_fee_24h: Some(new_usdt_fee.clone()),
                    weth_fee_24h: Some(new_weth_fee.clone()),
                })
                .on_conflict(apt_data::protocol_name)
                .do_update()
                .set((
                    apt_data::apt_volume_24h.eq(excluded(apt_data::apt_volume_24h)),
                    apt_data::usdc_volume_24h.eq(excluded(apt_data::usdc_volume_24h)),
                    apt_data::usdt_volume_24h.eq(excluded(apt_data::usdt_volume_24h)),
                    apt_data::weth_volume_24h.eq(excluded(apt_data::weth_volume_24h)),
                    apt_data::apt_fee_24h.eq(excluded(apt_data::apt_fee_24h)),
                    apt_data::usdc_fee_24h.eq(excluded(apt_data::usdc_fee_24h)),
                    apt_data::usdt_fee_24h.eq(excluded(apt_data::usdt_fee_24h)),
                    apt_data::weth_fee_24h.eq(excluded(apt_data::weth_fee_24h)),
                    apt_data::inserted_at.eq(diesel::dsl::now)
                ))
                .execute(&mut conn)
                .await
            {
                Ok(_) => {
                    info!("✅ Updated rolling data for protocol {}: APT vol +{} (total: {}), USDC vol +{} (total: {}), USDT vol +{} (total: {}), WETH vol +{} (total: {}), APT fee +{} (total: {}), USDC fee +{} (total: {}), USDT fee +{} (total: {}), WETH fee +{} (total: {})", 
                        record.protocol_name, 
                        batch_apt_volume, new_apt_volume, 
                        batch_usdc_volume, new_usdc_volume,
                        batch_usdt_volume, new_usdt_volume,
                        batch_weth_volume, new_weth_volume,
                        batch_apt_fee, new_apt_fee,
                        batch_usdc_fee, new_usdc_fee,
                        batch_usdt_fee, new_usdt_fee,
                        batch_weth_fee, new_weth_fee);
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
        
        // After updating individual protocols, calculate and update the aggregated "aptos" total
        self.upsert_aptos_aggregated_data().await?;
        
        Ok(())
    }

    async fn upsert_aptos_aggregated_data(&self) -> Result<(), ProcessorError> {
        let mut conn = self.connection_pool.get().await.map_err(|e| {
            ProcessorError::ProcessError {
                message: format!("Failed to get database connection for aptos aggregation: {}", e),
            }
        })?;

        info!("🔄 Calculating aggregated data for 'aptos' protocol from dapps...");

        // Define the dapps to aggregate
        let dapp_names = vec!["sushiswap", "cellana", "thala", "liquidswap", "hyperion"];
        
        // Get data for all dapps
        let dapp_data: Vec<AptData> = apt_data::table
            .filter(apt_data::protocol_name.eq_any(&dapp_names))
            .load(&mut conn)
            .await
            .map_err(|e| ProcessorError::ProcessError {
                message: format!("Failed to load dapp data for aggregation: {}", e),
            })?;

        if dapp_data.is_empty() {
            info!("📊 No dapp data found for aggregation");
            return Ok(());
        }

        // Calculate totals
        let zero_decimal = BigDecimal::zero();
        let mut total_apt_volume = zero_decimal.clone();
        let mut total_usdc_volume = zero_decimal.clone();
        let mut total_usdt_volume = zero_decimal.clone();
        let mut total_weth_volume = zero_decimal.clone();
        let mut total_apt_fee = zero_decimal.clone();
        let mut total_usdc_fee = zero_decimal.clone();
        let mut total_usdt_fee = zero_decimal.clone();
        let mut total_weth_fee = zero_decimal.clone();

        for data in &dapp_data {
            total_apt_volume += data.apt_volume_24h.as_ref().unwrap_or(&zero_decimal);
            total_usdc_volume += data.usdc_volume_24h.as_ref().unwrap_or(&zero_decimal);
            total_usdt_volume += data.usdt_volume_24h.as_ref().unwrap_or(&zero_decimal);
            total_weth_volume += data.weth_volume_24h.as_ref().unwrap_or(&zero_decimal);
            total_apt_fee += data.apt_fee_24h.as_ref().unwrap_or(&zero_decimal);
            total_usdc_fee += data.usdc_fee_24h.as_ref().unwrap_or(&zero_decimal);
            total_usdt_fee += data.usdt_fee_24h.as_ref().unwrap_or(&zero_decimal);
            total_weth_fee += data.weth_fee_24h.as_ref().unwrap_or(&zero_decimal);
        }

        info!("📊 Aggregated totals: APT vol={}, USDC vol={}, USDT vol={}, WETH vol={}, APT fee={}, USDC fee={}, USDT fee={}, WETH fee={}", 
            total_apt_volume, total_usdc_volume, total_usdt_volume, total_weth_volume,
            total_apt_fee, total_usdc_fee, total_usdt_fee, total_weth_fee);

        // Upsert the aggregated "aptos" record
        match diesel::insert_into(apt_data::table)
            .values(&NewAptData {
                protocol_name: "aptos".to_string(),
                apt_volume_24h: Some(total_apt_volume.clone()),
                usdc_volume_24h: Some(total_usdc_volume.clone()),
                usdt_volume_24h: Some(total_usdt_volume.clone()),
                weth_volume_24h: Some(total_weth_volume.clone()),
                apt_fee_24h: Some(total_apt_fee.clone()),
                usdc_fee_24h: Some(total_usdc_fee.clone()),
                usdt_fee_24h: Some(total_usdt_fee.clone()),
                weth_fee_24h: Some(total_weth_fee.clone()),
            })
            .on_conflict(apt_data::protocol_name)
            .do_update()
            .set((
                apt_data::apt_volume_24h.eq(excluded(apt_data::apt_volume_24h)),
                apt_data::usdc_volume_24h.eq(excluded(apt_data::usdc_volume_24h)),
                apt_data::usdt_volume_24h.eq(excluded(apt_data::usdt_volume_24h)),
                apt_data::weth_volume_24h.eq(excluded(apt_data::weth_volume_24h)),
                apt_data::apt_fee_24h.eq(excluded(apt_data::apt_fee_24h)),
                apt_data::usdc_fee_24h.eq(excluded(apt_data::usdc_fee_24h)),
                apt_data::usdt_fee_24h.eq(excluded(apt_data::usdt_fee_24h)),
                apt_data::weth_fee_24h.eq(excluded(apt_data::weth_fee_24h)),
                apt_data::inserted_at.eq(diesel::dsl::now)
            ))
            .execute(&mut conn)
            .await
        {
            Ok(_) => {
                info!("✅ Updated aggregated 'aptos' protocol data: APT vol={}, USDC vol={}, USDT vol={}, WETH vol={}, APT fee={}, USDC fee={}, USDT fee={}, WETH fee={}", 
                    total_apt_volume, total_usdc_volume, total_usdt_volume, total_weth_volume,
                    total_apt_fee, total_usdc_fee, total_usdt_fee, total_weth_fee);
            },
            Err(e) => {
                error!("❌ Failed to update aggregated 'aptos' data: {}", e);
                return Err(ProcessorError::ProcessError {
                    message: format!("Aptos aggregation failed: {}", e),
                });
            }
        }

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

        // Clean up old bucket data first (older than 24 hours)
        self.cleanup_old_buckets(cutoff_time).await?;

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
                        apt_data::weth_volume_24h.eq(Some(BigDecimal::zero())),
                        apt_data::apt_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::usdc_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::usdt_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::weth_fee_24h.eq(Some(BigDecimal::zero())),
                        apt_data::inserted_at.eq(diesel::dsl::now)
                    ))
                    .execute(&mut conn)
                    .await
                {
                    Ok(updated_count) => {
                        info!("✅ Reset {} pool volumes for new 24h window (including 'aptos' aggregated data)", updated_count);
                    },
                    Err(e) => {
                        error!("❌ Failed to reset volumes: {}", e);
                    }
                }

                // Also reset coin volumes for new 24h window
                match diesel::update(coin_volume_24h::table)
                    .set((
                        coin_volume_24h::buy_volume.eq(Some(BigDecimal::zero())),
                        coin_volume_24h::sell_volume.eq(Some(BigDecimal::zero())),
                        coin_volume_24h::inserted_at.eq(diesel::dsl::now)
                    ))
                    .execute(&mut conn)
                    .await
                {
                    Ok(updated_count) => {
                        info!("✅ Reset {} coin volumes for new 24h window", updated_count);
                    },
                    Err(e) => {
                        error!("❌ Failed to reset coin volumes: {}", e);
                    }
                }

                // Reset coin volume buckets
                match diesel::delete(coin_volume_buckets::table)
                    .execute(&mut conn)
                    .await
                {
                    Ok(deleted_count) => {
                        info!("✅ Deleted {} coin volume bucket records for fresh start", deleted_count);
                    },
                    Err(e) => {
                        error!("❌ Failed to delete coin volume buckets: {}", e);
                    }
                }
            } else {
                info!("✅ Volume data is recent (last update: {}), continuing accumulation", 
                    latest_utc.format("%Y-%m-%d %H:%M:%S UTC"));
            }
        } else {
            // Reset coin volume buckets on startup
            match diesel::delete(coin_volume_buckets::table)
                .execute(&mut conn)
                .await
            {
                Ok(deleted_count) => {
                    info!("✅ Deleted {} coin volume bucket records on startup", deleted_count);
                },
                Err(e) => {
                    error!("❌ Failed to delete coin volume buckets on startup: {}", e);
                }
            }
            
            // Reset coin volumes on startup
            match diesel::update(coin_volume_24h::table)
                .set((
                    coin_volume_24h::buy_volume.eq(Some(BigDecimal::zero())),
                    coin_volume_24h::sell_volume.eq(Some(BigDecimal::zero())),
                    coin_volume_24h::inserted_at.eq(diesel::dsl::now)
                ))
                .execute(&mut conn)
                .await
            {
                Ok(updated_count) => {
                    info!("✅ Reset {} coin volumes on startup", updated_count);
                },
                Err(e) => {
                    error!("❌ Failed to reset coin volumes on startup: {}", e);
                }
            }
        }

        Ok(())
    }
    
    /// Clean up old bucket data that is older than 24 hours
    async fn cleanup_old_buckets(&self, cutoff_time: DateTime<Utc>) -> Result<(), ProcessorError> {
        let mut conn = self.connection_pool.get().await.map_err(|e| {
            ProcessorError::ProcessError {
                message: format!("Failed to get database connection for bucket cleanup: {}", e),
            }
        })?;
        
        // Convert cutoff_time to NaiveDateTime for comparison
        let cutoff_naive = cutoff_time.naive_utc();
        
        // Delete buckets older than cutoff time
        match diesel::delete(coin_volume_buckets::table)
            .filter(coin_volume_buckets::bucket_end.lt(cutoff_naive))
            .execute(&mut conn)
            .await
        {
            Ok(deleted_count) => {
                info!("🧹 Deleted {} old bucket records (older than 24h)", deleted_count);
            },
            Err(e) => {
                error!("❌ Failed to delete old bucket records: {}", e);
                return Err(ProcessorError::ProcessError {
                    message: format!("Failed to delete old bucket records: {}", e),
                });
            }
        }
        
        // Keep only the latest 12 buckets per coin (for 24h chart with 2h buckets)
        let coins: Vec<String> = coin_volume_buckets::table
            .select(coin_volume_buckets::coin)
            .distinct()
            .load(&mut conn)
            .await
            .map_err(|e| ProcessorError::ProcessError {
                message: format!("Failed to get distinct coins: {}", e),
            })?;
            
        let mut total_deleted = 0;
        
        for coin in coins {
            // Get all buckets for this coin, ordered by newest first
            let buckets: Vec<(String, NaiveDateTime)> = coin_volume_buckets::table
                .filter(coin_volume_buckets::coin.eq(&coin))
                .select((
                    coin_volume_buckets::coin,
                    coin_volume_buckets::bucket_start
                ))
                .order_by(coin_volume_buckets::bucket_start.desc())
                .load(&mut conn)
                .await
                .map_err(|e| ProcessorError::ProcessError {
                    message: format!("Failed to get buckets for coin {}: {}", coin, e),
                })?;
                
            // If we have more than 12 buckets, delete the oldest ones
            if buckets.len() > 12 {
                // Keep only the newest 12 buckets
                let buckets_to_keep = buckets.iter().take(12).cloned().collect::<Vec<_>>();
                
                // Get the oldest bucket start time that we want to keep
                let oldest_bucket_to_keep = buckets_to_keep.last().map(|(_coin, start)| start).unwrap();
                
                // Delete all buckets older than the oldest one we want to keep
                match diesel::delete(coin_volume_buckets::table)
                    .filter(coin_volume_buckets::coin.eq(&coin))
                    .filter(coin_volume_buckets::bucket_start.lt(oldest_bucket_to_keep))
                    .execute(&mut conn)
                    .await
                {
                    Ok(deleted_count) => {
                        info!("🧹 Deleted {} excess bucket records for coin {} (keeping latest 12)", deleted_count, coin);
                        total_deleted += deleted_count;
                    },
                    Err(e) => {
                        error!("❌ Failed to delete excess bucket records for coin {}: {}", coin, e);
                    }
                }
            }
        }
        
        if total_deleted > 0 {
            info!("✅ Total {} excess bucket records deleted to maintain 12 buckets per coin", total_deleted);
        }
        
        Ok(())
    }

    async fn upsert_coin_volumes(&self, coin_volume_data: Vec<NewCoinVolume24h>) -> Result<(), ProcessorError> {
        if coin_volume_data.is_empty() {
            return Ok(());
        }

        let mut conn = self.connection_pool.get().await.map_err(|e| {
            ProcessorError::ProcessError {
                message: format!("Failed to get database connection for coin volumes: {}", e),
            }
        })?;

        info!("🪙 Upserting {} aggregated coin volume records", coin_volume_data.len());

        for record in &coin_volume_data {
            let zero_decimal = BigDecimal::zero();
            let batch_buy_volume = record.buy_volume.as_ref().unwrap_or(&zero_decimal);
            let batch_sell_volume = record.sell_volume.as_ref().unwrap_or(&zero_decimal);
            
            // Get current volumes first
            let current_data = coin_volume_24h::table
                .filter(coin_volume_24h::coin.eq(&record.coin))
                .first::<CoinVolume24h>(&mut conn)
                .await
                .optional()
                .map_err(|e| ProcessorError::ProcessError {
                    message: format!("Failed to get current coin volumes for {}: {}", record.coin, e),
                })?;

            let (current_buy_volume, current_sell_volume) = if let Some(data) = current_data {
                let current_buy = data.buy_volume.unwrap_or_else(|| zero_decimal.clone());
                let current_sell = data.sell_volume.unwrap_or_else(|| zero_decimal.clone());
                (current_buy, current_sell)
            } else {
                (zero_decimal.clone(), zero_decimal.clone())
            };
            
            // Accumulate volumes
            let new_buy_volume = &current_buy_volume + batch_buy_volume;
            let new_sell_volume = &current_sell_volume + batch_sell_volume;
            
            // UPSERT: INSERT or UPDATE if coin exists
            match diesel::insert_into(coin_volume_24h::table)
                .values(&NewCoinVolume24h {
                    coin: record.coin.clone(),
                    buy_volume: Some(new_buy_volume.clone()),
                    sell_volume: Some(new_sell_volume.clone()),
                })
                .on_conflict(coin_volume_24h::coin)
                .do_update()
                .set((
                    coin_volume_24h::buy_volume.eq(excluded(coin_volume_24h::buy_volume)),
                    coin_volume_24h::sell_volume.eq(excluded(coin_volume_24h::sell_volume)),
                    coin_volume_24h::inserted_at.eq(diesel::dsl::now)
                ))
                .execute(&mut conn)
                .await
            {
                Ok(_) => {
                    info!("✅ Updated aggregated coin volume for {}: buy +{} (total: {}), sell +{} (total: {})", 
                        record.coin,
                        batch_buy_volume, new_buy_volume, 
                        batch_sell_volume, new_sell_volume);
                },
                Err(e) => {
                    error!("❌ Failed to update coin volume for {}: {}", record.coin, e);
                    return Err(ProcessorError::ProcessError {
                        message: format!("Coin volume update failed: {}", e),
                    });
                }
            }
        }

        info!("✅ Successfully processed {} aggregated coin volume records", coin_volume_data.len());
        
        Ok(())
    }

    async fn upsert_coin_volume_buckets(&self, bucket_data: Vec<NewCoinVolumeBucket>) -> Result<(), ProcessorError> {
        if bucket_data.is_empty() {
            return Ok(());
        }

        let mut conn = self.connection_pool.get().await.map_err(|e| {
            ProcessorError::ProcessError {
                message: format!("Failed to get database connection for bucket data: {}", e),
            }
        })?;

        info!("🪣 Upserting {} bucket records", bucket_data.len());

        for record in &bucket_data {
            let zero_decimal = BigDecimal::zero();
            let batch_volume = record.volume.as_ref().unwrap_or(&zero_decimal);
            
            // Get current volume first
            let current_data = coin_volume_buckets::table
                .filter(coin_volume_buckets::coin.eq(&record.coin))
                .filter(coin_volume_buckets::bucket_start.eq(&record.bucket_start))
                .first::<crate::db::common::models::coin_volume_models::CoinVolumeBucket>(&mut conn)
                .await
                .optional()
                .map_err(|e| ProcessorError::ProcessError {
                    message: format!("Failed to get current bucket data for {}: {}", record.coin, e),
                })?;

            let current_volume = if let Some(data) = current_data {
                data.volume.unwrap_or_else(|| zero_decimal.clone())
            } else {
                zero_decimal.clone()
            };
            
            // Accumulate volume
            let new_volume = &current_volume + batch_volume;
            
            match diesel::insert_into(coin_volume_buckets::table)
                .values(&NewCoinVolumeBucket {
                    coin: record.coin.clone(),
                    bucket_start: record.bucket_start,
                    bucket_end: record.bucket_end,
                    volume: Some(new_volume.clone()),
                })
                .on_conflict((coin_volume_buckets::coin, coin_volume_buckets::bucket_start))
                .do_update()
                .set((
                    coin_volume_buckets::volume.eq(excluded(coin_volume_buckets::volume)),
                    coin_volume_buckets::bucket_end.eq(excluded(coin_volume_buckets::bucket_end)),
                    coin_volume_buckets::inserted_at.eq(diesel::dsl::now)
                ))
                .execute(&mut conn)
                .await
            {
                Ok(_) => {
                    info!("✅ Updated bucket: {} {} - {} (batch: +{}, total: {})", 
                        record.coin,
                        record.bucket_start.format("%Y-%m-%d %H:%M:%S"), 
                        record.bucket_end.format("%Y-%m-%d %H:%M:%S"),
                        batch_volume, new_volume);
                },
                Err(e) => {
                    error!("❌ Failed to upsert bucket for {}: {}", record.coin, e);
                    return Err(ProcessorError::ProcessError {
                        message: format!("Bucket upsert failed: {}", e),
                    });
                }
            }
        }

        info!("✅ Successfully processed {} bucket records", bucket_data.len());
        
        Ok(())
    }

    /// Query coin volume buckets with proper ordering
    pub async fn get_coin_volume_buckets_ordered(&self) -> Result<Vec<CoinVolumeBucket>, ProcessorError> {
        let mut conn = self.connection_pool.get().await.map_err(|e| {
            ProcessorError::ProcessError {
                message: format!("Failed to get database connection: {}", e),
            }
        })?;

        let buckets = coin_volume_buckets::table
            .order_by((
                coin_volume_buckets::coin.asc(),
                coin_volume_buckets::bucket_start.asc()
            ))
            .load::<CoinVolumeBucket>(&mut conn)
            .await
            .map_err(|e| ProcessorError::ProcessError {
                message: format!("Failed to query coin volume buckets: {}", e),
            })?;

        info!("📊 Retrieved {} coin volume buckets (ordered by coin, bucket_start)", buckets.len());
        
        Ok(buckets)
    }

    /// Query coin volume buckets for a specific coin with proper ordering
    pub async fn get_coin_volume_buckets_for_coin(&self, coin_name: &str) -> Result<Vec<CoinVolumeBucket>, ProcessorError> {
        let mut conn = self.connection_pool.get().await.map_err(|e| {
            ProcessorError::ProcessError {
                message: format!("Failed to get database connection: {}", e),
            }
        })?;

        let buckets = coin_volume_buckets::table
            .filter(coin_volume_buckets::coin.eq(coin_name))
            .order_by(coin_volume_buckets::bucket_start.asc())
            .load::<CoinVolumeBucket>(&mut conn)
            .await
            .map_err(|e| ProcessorError::ProcessError {
                message: format!("Failed to query coin volume buckets for {}: {}", coin_name, e),
            })?;

        info!("📊 Retrieved {} coin volume buckets for {} (ordered by bucket_start)", buckets.len(), coin_name);
        
        Ok(buckets)
    }

    /// Query recent coin volume buckets (last N hours) with proper ordering
    pub async fn get_recent_coin_volume_buckets(&self, hours: i32) -> Result<Vec<CoinVolumeBucket>, ProcessorError> {
        let mut conn = self.connection_pool.get().await.map_err(|e| {
            ProcessorError::ProcessError {
                message: format!("Failed to get database connection: {}", e),
            }
        })?;

        let cutoff_time = Utc::now() - Duration::hours(hours as i64);
        let cutoff_naive = cutoff_time.naive_utc();

        let buckets = coin_volume_buckets::table
            .filter(coin_volume_buckets::bucket_start.ge(cutoff_naive))
            .order_by((
                coin_volume_buckets::coin.asc(),
                coin_volume_buckets::bucket_start.asc()
            ))
            .load::<CoinVolumeBucket>(&mut conn)
            .await
            .map_err(|e| ProcessorError::ProcessError {
                message: format!("Failed to query recent coin volume buckets: {}", e),
            })?;

        info!("📊 Retrieved {} recent coin volume buckets (last {}h, ordered by coin, bucket_start)", 
            buckets.len(), hours);
        
        Ok(buckets)
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

        // Insert APT data
        self.upsert_pool_volumes(volume_context.data.apt_data).await?;

        // Insert coin volume data
        if !volume_context.data.coin_volume_data.is_empty() {
            self.upsert_coin_volumes(volume_context.data.coin_volume_data).await?;
        }

        // Insert bucket data
        if !volume_context.data.coin_volume_buckets.is_empty() {
            self.upsert_coin_volume_buckets(volume_context.data.coin_volume_buckets).await?;
        }

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