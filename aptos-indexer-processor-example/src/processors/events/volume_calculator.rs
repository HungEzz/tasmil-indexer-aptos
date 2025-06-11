use crate::db::common::models::apt_models::NewAptData;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{transaction::TxnData, Transaction},
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use bigdecimal::{BigDecimal, Zero, FromPrimitive};
use chrono::{DateTime, Utc, Duration};
use serde_json;
use std::{collections::HashMap, str::FromStr};
use tracing::{info, warn, error, debug};

const CELLANA_SWAP_EVENT_TYPE: &str = "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::SwapEvent";

    // Only track the APT/USDC pool
const TARGET_POOL_ADDRESS: &str = "0x71c6ae634bd3c36470eb7e7f4fb0912973bb31543dfdb7d7fb6863d886d81d67";

const APT_COIN_TYPE: &str = "0x1::aptos_coin::AptosCoin";
const USDC_COIN_TYPE: &str = "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b";

// Decimal places
const APT_DECIMALS: u8 = 8;
const USDC_DECIMALS: u8 = 6;

// Helper function to check if a transaction is within the last 24 hours
fn is_within_24h(txn_timestamp_seconds: i64) -> bool {
    let now = Utc::now();
    let cutoff_time = now - Duration::hours(24);
    let txn_time = DateTime::from_timestamp(txn_timestamp_seconds, 0)
        .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
    
    txn_time >= cutoff_time
}

#[derive(Debug)]
struct SwapData {
    amount_in: String,
    amount_out: String,
    from_token: String,
    to_token: String,
    pool: String,
}

#[derive(Debug)]
#[allow(dead_code)] // pool field is used to create NewAptData records
struct PoolVolume {
    pool: String,
    total_apt_amount: BigDecimal, // Total APT volume in this batch
    apt_price: Option<BigDecimal>, // Latest APT price from swaps
}

/// VolumeCalculator calculates real-time 24h rolling volume for each pool
pub struct VolumeCalculator {
    _placeholder: (),
}

impl VolumeCalculator {
    pub fn new() -> Self {
        info!("🚀 Initializing VolumeCalculator");
        info!("📊 Configured for pool-based volume tracking");
        Self {
            _placeholder: (),
        }
    }

    fn extract_swap_data(&self, event_data: &serde_json::Value) -> Result<SwapData> {
        debug!("🔍 Extracting swap data from event");
        
        let amount_in = event_data
            .get("amount_in")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing amount_in"))?;
            
        let amount_out = event_data
            .get("amount_out")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing amount_out"))?;
            
        let from_token = event_data
            .get("from_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing from_token"))?;
            
        let to_token = event_data
            .get("to_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing to_token"))?;

        let pool = event_data
            .get("pool")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing pool"))?;

        debug!("✅ Extracted swap: {} {} -> {} {}", 
            amount_in, from_token, amount_out, to_token);

        Ok(SwapData {
            amount_in: amount_in.to_string(),
            amount_out: amount_out.to_string(),
            from_token: from_token.to_string(),
            to_token: to_token.to_string(),
            pool: pool.to_string(),
        })
    }

    fn calculate_apt_price(&self, swap_data: &SwapData) -> Option<BigDecimal> {
        let raw_amount_in = BigDecimal::from_str(&swap_data.amount_in).ok()?;
        let raw_amount_out = BigDecimal::from_str(&swap_data.amount_out).ok()?;

        // Calculate APT price only for APT/USDC pairs
        if swap_data.from_token == APT_COIN_TYPE && swap_data.to_token == USDC_COIN_TYPE {
            // APT -> USDC: price = USDC amount / APT amount
            let apt_amount = &raw_amount_in / BigDecimal::from(10_u64.pow(APT_DECIMALS as u32));
            let usdc_amount = &raw_amount_out / BigDecimal::from(10_u64.pow(USDC_DECIMALS as u32));
            let price = &usdc_amount / &apt_amount;
            
            info!("💰 APT->USDC swap: {} APT for {} USDC, price: {} USD/APT", apt_amount, usdc_amount, price);
            Some(price)
        } else if swap_data.from_token == USDC_COIN_TYPE && swap_data.to_token == APT_COIN_TYPE {
            // USDC -> APT: price = USDC amount / APT amount
            let usdc_amount = &raw_amount_in / BigDecimal::from(10_u64.pow(USDC_DECIMALS as u32));
            let apt_amount = &raw_amount_out / BigDecimal::from(10_u64.pow(APT_DECIMALS as u32));
            let price = &usdc_amount / &apt_amount;
            
            info!("💰 USDC->APT swap: {} USDC for {} APT, price: {} USD/APT", usdc_amount, apt_amount, price);
            Some(price)
        } else {
            None
        }
    }

    fn calculate_apt_amount(&self, swap_data: &SwapData) -> BigDecimal {
        let raw_amount_in = BigDecimal::from_str(&swap_data.amount_in).unwrap_or_else(|_| BigDecimal::zero());
        let raw_amount_out = BigDecimal::from_str(&swap_data.amount_out).unwrap_or_else(|_| BigDecimal::zero());

        if swap_data.from_token == APT_COIN_TYPE {
            // APT -> other: use input APT amount
            &raw_amount_in / BigDecimal::from(10_u64.pow(APT_DECIMALS as u32))
        } else if swap_data.to_token == APT_COIN_TYPE {
            // other -> APT: use output APT amount
            &raw_amount_out / BigDecimal::from(10_u64.pow(APT_DECIMALS as u32))
        } else {
            BigDecimal::zero()
        }
    }
}

#[async_trait]
impl Processable for VolumeCalculator {
    type Input = Vec<Transaction>;
    type Output = Vec<NewAptData>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<Vec<NewAptData>>>, ProcessorError> {
        info!("📊 Processing batch for volume calculation: versions [{}, {}], {} transactions", 
            item.metadata.start_version, 
            item.metadata.end_version,
            item.data.len()
        );
        
        // Aggregate volume by pool
        let mut pool_volumes: HashMap<String, PoolVolume> = HashMap::new();

        for txn in &item.data {
            info!("🔍 Processing transaction version: {:?}", txn);
            let transaction_version = txn.version as i64;

            // Extract transaction timestamp for 24h filtering
            let timestamp_seconds = txn.timestamp.as_ref()
                .map(|ts| ts.seconds)
                .unwrap_or(0);

            // Filter: Only process transactions within the last 24 hours
            if !is_within_24h(timestamp_seconds) {
                debug!("⏰ Skipping transaction {} - older than 24 hours (timestamp: {})", 
                    transaction_version, timestamp_seconds);
                continue;
            }

            debug!("⏰ Processing transaction {} within 24h window (timestamp: {})", 
                transaction_version, timestamp_seconds);

            let txn_data = match txn.txn_data.as_ref() {
                Some(data) => data,
                None => {
                    debug!("⚠️ No txn_data for version {}", transaction_version);
                    continue;
                }
            };

            let default = vec![];
            let raw_events = match txn_data {
                TxnData::BlockMetadata(tx_inner) => &tx_inner.events,
                TxnData::Genesis(tx_inner) => &tx_inner.events,
                TxnData::User(tx_inner) => &tx_inner.events,
                _ => &default,
            };

            for (event_idx, event) in raw_events.iter().enumerate() {
                if event.type_str != CELLANA_SWAP_EVENT_TYPE {
                    continue;
                }

                info!("🎉 Found swap event at txn {}, event {}", transaction_version, event_idx);

                let event_data: serde_json::Value = match serde_json::from_str(&event.data) {
                    Ok(data) => data,
                    Err(e) => {
                        error!("❌ Failed to parse event data: {}", e);
                        continue;
                    }
                };

                // Extract swap data
                let swap_data = match self.extract_swap_data(&event_data) {
                    Ok(data) => data,
                    Err(e) => {
                        warn!("⚠️ Failed to extract swap data: {:?}", e);
                        continue;
                    }
                };

                // Filter: Only process swaps from our target APT/USDC pool
                if swap_data.pool != TARGET_POOL_ADDRESS {
                    debug!("⚠️ Skipping swap from pool {}, not our target pool", swap_data.pool);
                    continue;
                }

                info!("🎯 Processing swap from target APT/USDC pool: {}", swap_data.pool);

                // Calculate APT amount for this swap
                let apt_amount = self.calculate_apt_amount(&swap_data);
                if apt_amount.is_zero() {
                    debug!("⚠️ No APT amount in swap");
                    continue;
                }

                // Calculate APT price if this is an APT/USDC swap
                let apt_price = self.calculate_apt_price(&swap_data);

                // Only process swaps that involve APT (for volume calculation)
                if !((swap_data.from_token == APT_COIN_TYPE) || (swap_data.to_token == APT_COIN_TYPE)) {
                    debug!("⚠️ Swap doesn't involve APT token, skipping");
                    continue;
                }

                // Get or create pool volume entry
                let pool_entry = pool_volumes.entry(swap_data.pool.clone()).or_insert_with(|| {
                    PoolVolume {
                        pool: swap_data.pool.clone(),
                        total_apt_amount: BigDecimal::zero(),
                        apt_price: None,
                    }
                });

                // Add APT amount to total
                pool_entry.total_apt_amount += apt_amount.clone();
                
                // Update price only if we have real APT/USDC price data
                if let Some(price) = apt_price {
                    pool_entry.apt_price = Some(price.clone());
                    info!("💰 Pool {}: +{} APT (total: {}), price updated: {} USD/APT", 
                        swap_data.pool, apt_amount, pool_entry.total_apt_amount, price);
                } else {
                    info!("💰 Pool {}: +{} APT (total: {}), no price data from this swap", 
                        swap_data.pool, apt_amount, pool_entry.total_apt_amount);
                }
            }
        }

        // Calculate final volume and choose best price
        let mut best_apt_price = None;

        // Find best price from pools with actual trading data
        for (pool_id, pool_data) in pool_volumes.iter() {
            // Use price from pools with actual trading data, prefer APT/USDC directly
            if let Some(price) = &pool_data.apt_price {
                if best_apt_price.is_none() || pool_id.contains("usdc") {
                    best_apt_price = Some(price.clone());
                }
            }
            
            info!("🏊 Pool {}: {} APT volume, price: {:?}", 
                pool_id, pool_data.total_apt_amount, pool_data.apt_price);
        }

        // Use best available price or default
        let final_apt_price = best_apt_price.unwrap_or_else(|| {
            warn!("No APT price data available from any pool, using default 5.0 USD");
            BigDecimal::from_f64(5.0).unwrap()
        });

        info!("📊 Final APT price: {} USD", final_apt_price);

        // Convert pool volumes to NewAptData records
        let volume_records: Vec<NewAptData> = pool_volumes
            .into_iter()
            .map(|(pool_id, pool_data)| {
                let final_volume = if let Some(price) = &pool_data.apt_price {
                    &pool_data.total_apt_amount * price
                } else {
                    &pool_data.total_apt_amount * &final_apt_price
                };
                
                NewAptData {
                    pool: pool_id,
                    total_apt_usdc_volume_24h: Some(final_volume),
                }
            })
            .collect();

        debug!("Generated {} volume records", volume_records.len());
        
        // Return all records for this transaction batch
                Ok(Some(TransactionContext {
            data: volume_records,
                    metadata: item.metadata,
                }))
    }
}

impl AsyncStep for VolumeCalculator {}

impl NamedStep for VolumeCalculator {
    fn name(&self) -> String {
        "VolumeCalculator".to_string()
    }
} 