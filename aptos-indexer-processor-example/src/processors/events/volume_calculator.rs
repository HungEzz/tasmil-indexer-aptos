use crate::db::common::models::apt_models::NewAptData;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{transaction::TxnData, Transaction},
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use bigdecimal::{BigDecimal, Zero};
use chrono::{DateTime, Utc, Duration};
use serde_json;
use std::collections::HashMap;
use tracing::{info, debug};

// Import the new modular processors
use super::cellana::{CellanaProcessor, constants::CELLANA_SWAP_EVENT_TYPE};
use super::thala::{ThalaProcessor, constants::THALA_SWAP_EVENT_TYPE};
use super::sushiswap::SushiSwapProcessor;

// Re-export the processor types for internal use
pub use super::cellana::processor::PoolVolume as CellanaPoolVolume;
pub use super::thala::processor::PoolVolume as ThalaPoolVolume;
pub use super::sushiswap::processor::SushiPoolVolume;

// Helper function to check if a transaction is within the last 24 hours
fn is_within_24h(txn_timestamp_seconds: i64) -> bool {
    let now = Utc::now();
    let cutoff_time = now - Duration::hours(24);
    let txn_time = DateTime::from_timestamp(txn_timestamp_seconds, 0)
        .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
    
    txn_time >= cutoff_time
}

/// VolumeCalculator calculates real-time 24h rolling volume for APT/USDC, USDT/USDC, and APT/USDT pools
pub struct VolumeCalculator {
    cellana_processor: CellanaProcessor,
    thala_processor: ThalaProcessor,
    sushi_swap_processor: SushiSwapProcessor,
}

impl VolumeCalculator {
    pub fn new() -> Self {
        info!("🚀 Initializing VolumeCalculator with modular architecture");
        info!("📊 Configured for Cellana, Thala, and SushiSwap volume tracking");
        Self {
            cellana_processor: CellanaProcessor::new(),
            thala_processor: ThalaProcessor::new(),
            sushi_swap_processor: SushiSwapProcessor::new(),
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
        let transactions = item.data;
        info!("---------------------    ");

        info!("🔄 Processing batch of {:?} transactions", transactions);
        info!("---------------------    ");
        
        if transactions.is_empty() {
            debug!("📭 No transactions to process");
            return Ok(Some(TransactionContext {
                data: vec![],
                metadata: item.metadata,
            }));
        }

        // Track all pool volumes by protocol and pool
        let mut cellana_volumes: HashMap<String, CellanaPoolVolume> = HashMap::new();
        let mut thala_volumes: HashMap<String, ThalaPoolVolume> = HashMap::new();
        let mut sushi_volumes: HashMap<String, SushiPoolVolume> = HashMap::new();

        for txn in &transactions {
            // Skip transactions not within 24h
            if !is_within_24h(txn.timestamp.as_ref().unwrap().seconds) {
                continue;
            }

            if let Some(TxnData::User(user_txn)) = &txn.txn_data {
                for event in &user_txn.events {
                    let event_type = &event.type_str;
                    
                    // Log ALL events to help debug SushiSwap detection
                    tracing::info!("🔍 Processing event: {}", event_type);
                    
                    // Add debug logging for all events
                    if event_type.contains("swap") || event_type.contains("Swap") {
                        tracing::info!("🎯 Found swap event: {}", event_type);
                    }
                    
                    // Check specifically for SushiSwap patterns
                    if event_type.contains("31a6675cbe84365bf2b0cbce617ece6c47023ef70826533bde5203d32171dc3c") {
                        tracing::info!("🍣 Found event matching SushiSwap contract: {}", event_type);
                    }
                    
                    // Process Cellana events
                    if event_type == CELLANA_SWAP_EVENT_TYPE {
                        tracing::debug!("🟢 Processing Cellana event: {}", event_type);
                        if let Ok(event_data) = serde_json::from_str::<serde_json::Value>(&event.data) {
                            if let Ok(mut swap_data) = self.cellana_processor.extract_swap_data(&event_data) {
                                // Fill fee information
                                swap_data.swap_fee_bps = self.cellana_processor.extract_swap_fee_bps(txn, &swap_data.pool);
                                
                                // Only process swaps from our target pools
                                if self.cellana_processor.is_target_pool(&swap_data.pool) {
                                    self.cellana_processor.process_swap(&mut cellana_volumes, swap_data).await;
                                }
                            }
                        }
                    }
                    
                    // Process Thala events
                    else if event_type == THALA_SWAP_EVENT_TYPE {
                        tracing::debug!("🔵 Processing Thala event: {}", event_type);
                        if let Ok(event_data) = serde_json::from_str::<serde_json::Value>(&event.data) {
                            if let Ok(swap_data) = self.thala_processor.extract_swap_data(&event_data) {
                                // Only process swaps from our target pools
                                if self.thala_processor.is_target_pool(&swap_data.pool) {
                                    self.thala_processor.process_swap(&mut thala_volumes, swap_data).await;
                                }
                            }
                        }
                    }
                    
                    // Process SushiSwap events
                    else if self.sushi_swap_processor.is_sushiswap_event(event_type) {
                        tracing::info!("🟠 FOUND SUSHISWAP EVENT: {}", event_type);
                        
                        if let Ok(event_data) = serde_json::from_str::<serde_json::Value>(&event.data) {
                            match self.sushi_swap_processor.extract_sushiswap_data(&event_data, event_type) {
                                Ok(swap_data) => {
                                    tracing::info!("🔄 Processing SushiSwap swap: {:?}", swap_data);
                                    self.sushi_swap_processor.process_sushiswap(&mut sushi_volumes, swap_data).await;
                                    tracing::info!("✅ SushiSwap swap processed successfully");
                                }
                                Err(e) => {
                                    tracing::error!("❌ Error extracting SushiSwap data: {}", e);
                                }
                            }
                        }
                    }
                    else {
                        // Log non-matching events to help debug
                        if event_type.contains("swap") || event_type.contains("Swap") {
                            tracing::info!("❓ Unknown swap event (not Cellana/Thala/SushiSwap): {}", event_type);
                        }
                    }
                }
            }
        }

        // Create results for each protocol - aggregate all pools per protocol
        let mut results = Vec::new();

        // Aggregate Cellana volumes across all pools
        let mut cellana_total_apt_volume = BigDecimal::zero();
        let mut cellana_total_usdc_volume = BigDecimal::zero();
        let mut cellana_total_usdt_volume = BigDecimal::zero();
        let mut cellana_total_stapt_volume = BigDecimal::zero();
        let mut cellana_total_abtc_volume = BigDecimal::zero();
        let mut cellana_total_apt_fee = BigDecimal::zero();
        let mut cellana_total_usdc_fee = BigDecimal::zero();
        let mut cellana_total_usdt_fee = BigDecimal::zero();
        let mut cellana_total_stapt_fee = BigDecimal::zero();
        let mut cellana_total_abtc_fee = BigDecimal::zero();

        for (_, pool_volume) in cellana_volumes {
            cellana_total_apt_volume += &pool_volume.apt_volume_24h;
            cellana_total_usdc_volume += &pool_volume.usdc_volume_24h;
            cellana_total_usdt_volume += &pool_volume.usdt_volume_24h;
            cellana_total_stapt_volume += &pool_volume.stapt_volume_24h;
            cellana_total_abtc_volume += &pool_volume.abtc_volume_24h;
            cellana_total_apt_fee += &pool_volume.apt_fee_24h;
            cellana_total_usdc_fee += &pool_volume.usdc_fee_24h;
            cellana_total_usdt_fee += &pool_volume.usdt_fee_24h;
            cellana_total_stapt_fee += &pool_volume.stapt_fee_24h;
            cellana_total_abtc_fee += &pool_volume.abtc_fee_24h;
        }

        // Create Cellana result if there's any volume
        if cellana_total_apt_volume > BigDecimal::zero() || 
           cellana_total_usdc_volume > BigDecimal::zero() || 
           cellana_total_usdt_volume > BigDecimal::zero() ||
           cellana_total_stapt_volume > BigDecimal::zero() ||
           cellana_total_abtc_volume > BigDecimal::zero() {
            
            let apt_data = NewAptData {
                protocol_name: "cellana".to_string(),
                apt_volume_24h: Some(cellana_total_apt_volume.clone()),
                usdc_volume_24h: Some(cellana_total_usdc_volume.clone()),
                usdt_volume_24h: Some(cellana_total_usdt_volume.clone()),
                stapt_volume_24h: Some(cellana_total_stapt_volume.clone()),
                abtc_volume_24h: Some(cellana_total_abtc_volume.clone()),
                weth_volume_24h: None, // Cellana doesn't support WETH yet
                apt_fee_24h: Some(cellana_total_apt_fee.clone()),
                usdc_fee_24h: Some(cellana_total_usdc_fee.clone()),
                usdt_fee_24h: Some(cellana_total_usdt_fee.clone()),
                stapt_fee_24h: Some(cellana_total_stapt_fee.clone()),
                abtc_fee_24h: Some(cellana_total_abtc_fee.clone()),
                weth_fee_24h: None, // Cellana doesn't support WETH yet
            };
            
            info!("💾 Created Cellana aggregated record: APT={:?}, USDC={:?}, USDT={:?}, stAPT={:?}, aBTC={:?}", 
                apt_data.apt_volume_24h, apt_data.usdc_volume_24h, apt_data.usdt_volume_24h, apt_data.stapt_volume_24h, apt_data.abtc_volume_24h);
            
            results.push(apt_data);
        }

        // Aggregate Thala volumes across all pools
        let mut thala_total_apt_volume = BigDecimal::zero();
        let mut thala_total_usdc_volume = BigDecimal::zero();
        let mut thala_total_usdt_volume = BigDecimal::zero();
        let thala_total_stapt_volume = BigDecimal::zero();
        let mut thala_total_apt_fee = BigDecimal::zero();
        let mut thala_total_usdc_fee = BigDecimal::zero();
        let mut thala_total_usdt_fee = BigDecimal::zero();
        let thala_total_stapt_fee = BigDecimal::zero();

        for (_, pool_volume) in thala_volumes {
            thala_total_apt_volume += &pool_volume.apt_volume_24h;
            thala_total_usdc_volume += &pool_volume.usdc_volume_24h;
            thala_total_usdt_volume += &pool_volume.usdt_volume_24h;
            thala_total_apt_fee += &pool_volume.apt_fee_24h;
            thala_total_usdc_fee += &pool_volume.usdc_fee_24h;
            thala_total_usdt_fee += &pool_volume.usdt_fee_24h;
        }

        // Create Thala result if there's any volume
        if thala_total_apt_volume > BigDecimal::zero() || 
           thala_total_usdc_volume > BigDecimal::zero() || 
           thala_total_usdt_volume > BigDecimal::zero() {
            
            let apt_data = NewAptData {
                protocol_name: "thala".to_string(),
                apt_volume_24h: Some(thala_total_apt_volume.clone()),
                usdc_volume_24h: Some(thala_total_usdc_volume.clone()),
                usdt_volume_24h: Some(thala_total_usdt_volume.clone()),
                stapt_volume_24h: Some(thala_total_stapt_volume.clone()),
                abtc_volume_24h: None, // Thala doesn't support aBTC yet
                weth_volume_24h: None, // Thala doesn't support WETH yet
                apt_fee_24h: Some(thala_total_apt_fee.clone()),
                usdc_fee_24h: Some(thala_total_usdc_fee.clone()),
                usdt_fee_24h: Some(thala_total_usdt_fee.clone()),
                stapt_fee_24h: Some(thala_total_stapt_fee.clone()),
                abtc_fee_24h: None, // Thala doesn't support aBTC yet
                weth_fee_24h: None, // Thala doesn't support WETH yet
            };
            
            info!("💾 Created Thala aggregated record: APT={:?}, USDC={:?}, USDT={:?}", 
                apt_data.apt_volume_24h, apt_data.usdc_volume_24h, apt_data.usdt_volume_24h);
            
            results.push(apt_data);
        }

        // Aggregate SushiSwap volumes across all pools
        let mut sushi_total_apt_volume = BigDecimal::zero();
        let mut sushi_total_usdt_volume = BigDecimal::zero();
        let mut sushi_total_usdc_volume = BigDecimal::zero();
        let mut sushi_total_weth_volume = BigDecimal::zero();

        for (_, pool_volume) in sushi_volumes {
            sushi_total_apt_volume += &pool_volume.apt_volume_24h;
            sushi_total_usdt_volume += &pool_volume.usdt_volume_24h;
            sushi_total_usdc_volume += &pool_volume.usdc_volume_24h;
            sushi_total_weth_volume += &pool_volume.weth_volume_24h;
        }

        // Create SushiSwap result if there's any volume
        if sushi_total_apt_volume > BigDecimal::zero() || 
           sushi_total_usdt_volume > BigDecimal::zero() ||
           sushi_total_usdc_volume > BigDecimal::zero() ||
           sushi_total_weth_volume > BigDecimal::zero() {
            
            let apt_data = NewAptData {
                protocol_name: "sushiswap".to_string(),
                apt_volume_24h: Some(sushi_total_apt_volume.clone()),
                usdc_volume_24h: Some(sushi_total_usdc_volume.clone()),  // Include USDC volume
                usdt_volume_24h: Some(sushi_total_usdt_volume.clone()),
                stapt_volume_24h: None, // SushiSwap doesn't trade stAPT
                abtc_volume_24h: None, // SushiSwap doesn't trade aBTC
                weth_volume_24h: Some(sushi_total_weth_volume.clone()),  // Include WETH volume
                apt_fee_24h: None, // SushiSwap doesn't have fees
                usdc_fee_24h: None, // SushiSwap doesn't have fees
                usdt_fee_24h: None, // SushiSwap doesn't have fees
                stapt_fee_24h: None, // SushiSwap doesn't have fees
                abtc_fee_24h: None, // SushiSwap doesn't have fees
                weth_fee_24h: None, // SushiSwap doesn't have fees
            };
            
            info!("💾 Created SushiSwap aggregated record: APT={:?}, USDT={:?}, USDC={:?}, WETH={:?}", 
                apt_data.apt_volume_24h, apt_data.usdt_volume_24h, apt_data.usdc_volume_24h, apt_data.weth_volume_24h);
            
            results.push(apt_data);
        }

        info!("✅ Successfully processed {} records in batch", results.len());

        Ok(Some(TransactionContext {
            data: results,
            metadata: item.metadata,
        }))
    }
}

#[async_trait]
impl AsyncStep for VolumeCalculator {}

impl NamedStep for VolumeCalculator {
    fn name(&self) -> String {
        "VolumeCalculator".to_string()
    }
} 