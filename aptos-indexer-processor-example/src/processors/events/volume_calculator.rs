use crate::db::common::models::apt_models::NewAptData;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{transaction::TxnData, Transaction, WriteSetChange},
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use bigdecimal::{BigDecimal, Zero};
use chrono::{DateTime, Utc, Duration};
use serde_json;
use std::{collections::HashMap, str::FromStr};
use tracing::{info, warn, error, debug};

const CELLANA_SWAP_EVENT_TYPE: &str = "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::SwapEvent";
const CELLANA_LIQUIDITY_POOL_TYPE: &str = "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::LiquidityPool";

// Track APT/USDC, USDT/USDC, and APT/USDT pools
const APT_USDC_POOL_ADDRESS: &str = "0x71c6ae634bd3c36470eb7e7f4fb0912973bb31543dfdb7d7fb6863d886d81d67";
const USDT_USDC_POOL_ADDRESS: &str = "0xac21d74053633a030281bd0311361442eb2c4f2f95b19c4599b741c439cff77f";
const APT_USDT_POOL_ADDRESS: &str = "0xe8797d7adaf48abd7becddc9c4494f1309d9a83a6ea0c4be708482cbc54e36af";

const APT_COIN_TYPE: &str = "0x1::aptos_coin::AptosCoin";
const USDC_COIN_TYPE: &str = "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b";
const USDT_COIN_TYPE: &str = "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b";

// Decimal places
const APT_DECIMALS: u8 = 8;
const USDC_DECIMALS: u8 = 6;
const USDT_DECIMALS: u8 = 6;

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
    swap_fee_bps: u32, // Fee in basis points
}

#[derive(Debug)]
#[allow(dead_code)] // pool field is used to create NewAptData records
struct PoolVolume {
    pool: String,
    apt_volume_24h: BigDecimal, // Total APT traded in this batch
    usdc_volume_24h: BigDecimal, // Total USDC traded in this batch (aggregated from both pools)
    usdt_volume_24h: BigDecimal, // Total USDT traded in this batch
    apt_fee_24h: BigDecimal, // Total APT fees collected in this batch
    usdc_fee_24h: BigDecimal, // Total USDC fees collected in this batch (aggregated from both pools)
    usdt_fee_24h: BigDecimal, // Total USDT fees collected in this batch
}

/// VolumeCalculator calculates real-time 24h rolling volume for APT/USDC, USDT/USDC, and APT/USDT pools
pub struct VolumeCalculator {
    _placeholder: (),
}

impl VolumeCalculator {
    pub fn new() -> Self {
        info!("🚀 Initializing VolumeCalculator");
        info!("📊 Configured for APT/USDC, USDT/USDC, and APT/USDT pool volume tracking");
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
            swap_fee_bps: 0, // Will be filled from transaction changes
        })
    }

    fn extract_swap_fee_bps(&self, txn: &Transaction, pool_address: &str) -> u32 {
        // Try to get changes from the transaction info field
        let changes = match &txn.info {
            Some(info) => &info.changes,
            None => return 0,
        };

        for change in changes {
            if let WriteSetChange { change: Some(aptos_indexer_processor_sdk::aptos_protos::transaction::v1::write_set_change::Change::WriteResource(resource)), .. } = change {
                // Check if this is the LiquidityPool resource for our target pool
                if resource.address == pool_address && resource.type_str == CELLANA_LIQUIDITY_POOL_TYPE {
                    if let Ok(pool_data) = serde_json::from_str::<serde_json::Value>(&resource.data) {
                        if let Some(swap_fee_bps) = pool_data.get("swap_fee_bps")
                            .and_then(|v| v.as_str())
                            .and_then(|v| v.parse::<u32>().ok()) {
                            debug!("🔧 Found swap_fee_bps: {} for pool {}", swap_fee_bps, pool_address);
                            return swap_fee_bps;
                        }
                    }
                }
            }
        }

        debug!("⚠️ No swap_fee_bps found for pool {}, returning default 0", pool_address);
        0
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
                let mut swap_data = match self.extract_swap_data(&event_data) {
                    Ok(data) => data,
                    Err(e) => {
                        warn!("⚠️ Failed to extract swap data: {:?}", e);
                        continue;
                    }
                };

                // Extract swap_fee_bps from transaction changes
                let swap_fee_bps = self.extract_swap_fee_bps(txn, &swap_data.pool);
                swap_data.swap_fee_bps = swap_fee_bps;

                // Filter: Only process swaps from our target APT/USDC pool
                if swap_data.pool != APT_USDC_POOL_ADDRESS && swap_data.pool != USDT_USDC_POOL_ADDRESS && swap_data.pool != APT_USDT_POOL_ADDRESS {
                    debug!("⚠️ Skipping swap from pool {}, not our target pool", swap_data.pool);
                    continue;
                }

                info!("🎯 Processing swap from target pool: {}", swap_data.pool);

                // Get or create pool volume entry
                let pool_entry = pool_volumes.entry(swap_data.pool.clone()).or_insert_with(|| {
                    PoolVolume {
                        pool: swap_data.pool.clone(),
                        apt_volume_24h: BigDecimal::zero(),
                        usdc_volume_24h: BigDecimal::zero(),
                        usdt_volume_24h: BigDecimal::zero(),
                        apt_fee_24h: BigDecimal::zero(),
                        usdc_fee_24h: BigDecimal::zero(),
                        usdt_fee_24h: BigDecimal::zero(),
                    }
                });

                // Determine swap direction and calculate volumes and fees
                let raw_amount_in = BigDecimal::from_str(&swap_data.amount_in).unwrap_or_else(|_| BigDecimal::zero());
                let raw_amount_out = BigDecimal::from_str(&swap_data.amount_out).unwrap_or_else(|_| BigDecimal::zero());
                let fee_rate = BigDecimal::from(swap_fee_bps) / BigDecimal::from(10000); // Convert bps to decimal

                // Handle APT/USDC pool swaps
                if swap_data.pool == APT_USDC_POOL_ADDRESS {
                    if swap_data.from_token == APT_COIN_TYPE && swap_data.to_token == USDC_COIN_TYPE {
                        // APT -> USDC: User sells APT, buys USDC
                        let apt_amount = &raw_amount_in / BigDecimal::from(10_u64.pow(APT_DECIMALS as u32));
                        let usdc_amount = &raw_amount_out / BigDecimal::from(10_u64.pow(USDC_DECIMALS as u32));
                        
                        // Calculate fee (fee is charged on amount_in, which is APT)
                        let apt_fee = &apt_amount * &fee_rate;
                        
                        // Calculate net volume (amount_in - fee) for APT
                        let apt_net_volume = &apt_amount - &apt_fee;
                        
                        // Volume calculations: APT uses net amount (without fee), USDC uses amount_out
                        pool_entry.apt_volume_24h += apt_net_volume.clone();
                        pool_entry.usdc_volume_24h += usdc_amount.clone();
                        pool_entry.apt_fee_24h += apt_fee.clone();
                        
                        info!("📈 APT->USDC: {} APT sold, {} USDC bought, {} APT fee ({}bps) | Net APT vol: {}, USDC vol: {}, APT fee: {}", 
                            apt_amount, usdc_amount, apt_fee, swap_fee_bps, 
                            apt_net_volume, usdc_amount, apt_fee);
                            
                    } else if swap_data.from_token == USDC_COIN_TYPE && swap_data.to_token == APT_COIN_TYPE {
                        // USDC -> APT: User sells USDC, buys APT
                        let usdc_amount = &raw_amount_in / BigDecimal::from(10_u64.pow(USDC_DECIMALS as u32));
                        let apt_amount = &raw_amount_out / BigDecimal::from(10_u64.pow(APT_DECIMALS as u32));
                        
                        // Calculate fee (fee is charged on amount_in, which is USDC)
                        let usdc_fee = &usdc_amount * &fee_rate;
                        
                        // Calculate net volume (amount_in - fee) for USDC
                        let usdc_net_volume = &usdc_amount - &usdc_fee;
                        
                        // Volume calculations: USDC uses net amount (without fee), APT uses amount_out
                        pool_entry.apt_volume_24h += apt_amount.clone();
                        pool_entry.usdc_volume_24h += usdc_net_volume.clone();
                        pool_entry.usdc_fee_24h += usdc_fee.clone();
                        
                        info!("📉 USDC->APT: {} USDC sold, {} APT bought, {} USDC fee ({}bps) | Net USDC vol: {}, APT vol: {}, USDC fee: {}", 
                            usdc_amount, apt_amount, usdc_fee, swap_fee_bps,
                            usdc_net_volume, apt_amount, usdc_fee);
                    } else {
                        debug!("⚠️ Swap doesn't involve APT/USDC pair in APT/USDC pool, skipping: {} -> {}", 
                            swap_data.from_token, swap_data.to_token);
                        continue;
                    }
                }
                // Handle USDT/USDC pool swaps
                else if swap_data.pool == USDT_USDC_POOL_ADDRESS {
                    if swap_data.from_token == USDT_COIN_TYPE && swap_data.to_token == USDC_COIN_TYPE {
                        // USDT -> USDC: User sells USDT, buys USDC
                        let usdt_amount = &raw_amount_in / BigDecimal::from(10_u64.pow(USDT_DECIMALS as u32));
                        let usdc_amount = &raw_amount_out / BigDecimal::from(10_u64.pow(USDC_DECIMALS as u32));
                        
                        // Calculate fee (fee is charged on amount_in, which is USDT)
                        let usdt_fee = &usdt_amount * &fee_rate;
                        
                        // Calculate net volume (amount_in - fee) for USDT
                        let usdt_net_volume = &usdt_amount - &usdt_fee;
                        
                        // Volume calculations: USDT uses net amount (without fee), USDC uses amount_out
                        pool_entry.usdt_volume_24h += usdt_net_volume.clone();
                        pool_entry.usdc_volume_24h += usdc_amount.clone();
                        pool_entry.usdt_fee_24h += usdt_fee.clone();
                        
                        info!("💰 USDT->USDC: {} USDT sold, {} USDC bought, {} USDT fee ({}bps) | Net USDT vol: {}, USDC vol: {}, USDT fee: {}", 
                            usdt_amount, usdc_amount, usdt_fee, swap_fee_bps,
                            usdt_net_volume, usdc_amount, usdt_fee);
                            
                    } else if swap_data.from_token == USDC_COIN_TYPE && swap_data.to_token == USDT_COIN_TYPE {
                        // USDC -> USDT: User sells USDC, buys USDT
                        let usdc_amount = &raw_amount_in / BigDecimal::from(10_u64.pow(USDC_DECIMALS as u32));
                        let usdt_amount = &raw_amount_out / BigDecimal::from(10_u64.pow(USDT_DECIMALS as u32));
                        
                        // Calculate fee (fee is charged on amount_in, which is USDC)
                        let usdc_fee = &usdc_amount * &fee_rate;
                        
                        // Calculate net volume (amount_in - fee) for USDC
                        let usdc_net_volume = &usdc_amount - &usdc_fee;
                        
                        // Volume calculations: USDC uses net amount (without fee), USDT uses amount_out
                        pool_entry.usdt_volume_24h += usdt_amount.clone();
                        pool_entry.usdc_volume_24h += usdc_net_volume.clone();
                        pool_entry.usdc_fee_24h += usdc_fee.clone();
                        
                        info!("💸 USDC->USDT: {} USDC sold, {} USDT bought, {} USDC fee ({}bps) | Net USDC vol: {}, USDT vol: {}, USDC fee: {}", 
                            usdc_amount, usdt_amount, usdc_fee, swap_fee_bps,
                            usdc_net_volume, usdt_amount, usdc_fee);
                    } else {
                        debug!("⚠️ Swap doesn't involve USDT/USDC pair in USDT/USDC pool, skipping: {} -> {}", 
                            swap_data.from_token, swap_data.to_token);
                        continue;
                    }
                }
                // Handle APT/USDT pool swaps
                else if swap_data.pool == APT_USDT_POOL_ADDRESS {
                    if swap_data.from_token == APT_COIN_TYPE && swap_data.to_token == USDT_COIN_TYPE {
                        // APT -> USDT: User sells APT, buys USDT
                        let apt_amount = &raw_amount_in / BigDecimal::from(10_u64.pow(APT_DECIMALS as u32));
                        let usdt_amount = &raw_amount_out / BigDecimal::from(10_u64.pow(USDT_DECIMALS as u32));
                        
                        // Calculate fee (fee is charged on amount_in, which is APT)
                        let apt_fee = &apt_amount * &fee_rate;
                        
                        // Calculate net volume (amount_in - fee) for APT
                        let apt_net_volume = &apt_amount - &apt_fee;
                        
                        // Volume calculations: APT uses net amount (without fee), USDT uses amount_out
                        pool_entry.apt_volume_24h += apt_net_volume.clone();
                        pool_entry.usdt_volume_24h += usdt_amount.clone();
                        pool_entry.apt_fee_24h += apt_fee.clone();
                        
                        info!("📈 APT->USDT: {} APT sold, {} USDT bought, {} APT fee ({}bps) | Net APT vol: {}, USDT vol: {}, APT fee: {}", 
                            apt_amount, usdt_amount, apt_fee, swap_fee_bps, 
                            apt_net_volume, usdt_amount, apt_fee);
                            
                    } else if swap_data.from_token == USDT_COIN_TYPE && swap_data.to_token == APT_COIN_TYPE {
                        // USDT -> APT: User sells USDT, buys APT
                        let usdt_amount = &raw_amount_in / BigDecimal::from(10_u64.pow(USDT_DECIMALS as u32));
                        let apt_amount = &raw_amount_out / BigDecimal::from(10_u64.pow(APT_DECIMALS as u32));
                        
                        // Calculate fee (fee is charged on amount_in, which is USDT)
                        let usdt_fee = &usdt_amount * &fee_rate;
                        
                        // Calculate net volume (amount_in - fee) for USDT
                        let usdt_net_volume = &usdt_amount - &usdt_fee;
                        
                        // Volume calculations: USDT uses net amount (without fee), APT uses amount_out
                        pool_entry.apt_volume_24h += apt_amount.clone();
                        pool_entry.usdt_volume_24h += usdt_net_volume.clone();
                        pool_entry.usdt_fee_24h += usdt_fee.clone();
                        
                        info!("📉 USDT->APT: {} USDT sold, {} APT bought, {} USDT fee ({}bps) | Net USDT vol: {}, APT vol: {}, USDT fee: {}", 
                            usdt_amount, apt_amount, usdt_fee, swap_fee_bps,
                            usdt_net_volume, apt_amount, usdt_fee);
                    } else {
                        debug!("⚠️ Swap doesn't involve APT/USDT pair in APT/USDT pool, skipping: {} -> {}", 
                            swap_data.from_token, swap_data.to_token);
                        continue;
                    }
                }
            }
        }

        // Aggregate volumes from all pools into a single record
        // USDC volumes and fees will be combined from both pools
        let mut aggregated_apt_volume = BigDecimal::zero();
        let mut aggregated_usdc_volume = BigDecimal::zero();
        let mut aggregated_usdt_volume = BigDecimal::zero();
        let mut aggregated_apt_fee = BigDecimal::zero();
        let mut aggregated_usdc_fee = BigDecimal::zero();
        let mut aggregated_usdt_fee = BigDecimal::zero();

        for pool_volume in pool_volumes.values() {
            aggregated_apt_volume += &pool_volume.apt_volume_24h;
            aggregated_usdc_volume += &pool_volume.usdc_volume_24h;
            aggregated_usdt_volume += &pool_volume.usdt_volume_24h;
            aggregated_apt_fee += &pool_volume.apt_fee_24h;
            aggregated_usdc_fee += &pool_volume.usdc_fee_24h;
            aggregated_usdt_fee += &pool_volume.usdt_fee_24h;
        }

        // Create a single aggregated record using the APT/USDC pool address as the identifier
        // Always create a record to ensure reset logic works properly
        let new_apt_data = vec![NewAptData {
            pool: APT_USDC_POOL_ADDRESS.to_string(), // Use APT/USDC pool address as the identifier
            apt_volume_24h: Some(aggregated_apt_volume.clone()),
            usdc_volume_24h: Some(aggregated_usdc_volume.clone()),
            usdt_volume_24h: Some(aggregated_usdt_volume.clone()),
            apt_fee_24h: Some(aggregated_apt_fee.clone()),
            usdc_fee_24h: Some(aggregated_usdc_fee.clone()),
            usdt_fee_24h: Some(aggregated_usdt_fee.clone()),
        }];

        info!("✅ Volume Calculator processed {} pools in this batch", pool_volumes.len());
        
        if !new_apt_data.is_empty() {
            let data = &new_apt_data[0];
            info!("📊 Aggregated volumes: APT vol: {:?}, USDC vol: {:?}, USDT vol: {:?}, APT fee: {:?}, USDC fee: {:?}, USDT fee: {:?}", 
                data.apt_volume_24h, 
                data.usdc_volume_24h,
                data.usdt_volume_24h,
                data.apt_fee_24h,
                data.usdc_fee_24h,
                data.usdt_fee_24h);
        }

        Ok(Some(TransactionContext {
            data: new_apt_data,
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