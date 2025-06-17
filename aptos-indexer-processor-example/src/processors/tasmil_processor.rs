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

use crate::{
    db::{
        common::models::{
            apt_models::{AptData, NewAptData},
            coin_volume_models::{NewCoinVolume24h, CoinVolume24h},
        },
        postgres::schema::{apt_data, coin_volume_24h},
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
            } else {
                info!("✅ Volume data is recent (last update: {}), continuing accumulation", 
                    latest_utc.format("%Y-%m-%d %H:%M:%S UTC"));
            }
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

async fn insert_coin_volume_to_db(
    conn: ArcDbPool,
    items_to_insert: Vec<NewCoinVolume24h>,
) -> Result<(), diesel::result::Error> {
    use crate::db::postgres::schema::coin_volume_24h::dsl::*;

    let mut conn = conn.get().await.expect("Failed to get connection");

    for item in items_to_insert {
        diesel::insert_into(coin_volume_24h)
            .values(&item)
            .on_conflict(coin)
            .do_update()
            .set((
                buy_volume.eq(excluded(buy_volume)),
                sell_volume.eq(excluded(sell_volume)),
                inserted_at.eq(diesel::dsl::now),
            ))
            .execute(&mut conn)
            .await?;
    }
    Ok(())
}