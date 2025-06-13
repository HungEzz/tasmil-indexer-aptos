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

// Re-export the processor types for internal use
pub use super::cellana::processor::PoolVolume as CellanaPoolVolume;
pub use super::thala::processor::PoolVolume as ThalaPoolVolume;

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
}

impl VolumeCalculator {
    pub fn new() -> Self {
        info!("🚀 Initializing VolumeCalculator with modular architecture");
        info!("📊 Configured for Cellana and Thala APT/USDC, USDT/USDC, and APT/USDT pool volume tracking");
        Self {
            cellana_processor: CellanaProcessor::new(),
            thala_processor: ThalaProcessor::new(),
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

        info!("🔄 Processing batch of {} transactions", transactions.len());
        
        // Track all pool volumes by protocol and pool
        let mut cellana_volumes: HashMap<String, CellanaPoolVolume> = HashMap::new();
        let mut thala_volumes: HashMap<String, ThalaPoolVolume> = HashMap::new();

        for txn in &transactions {
            // Skip transactions not within 24h
            if !is_within_24h(txn.timestamp.as_ref().unwrap().seconds) {
                continue;
            }

            if let Some(TxnData::User(user_txn)) = &txn.txn_data {
                for event in &user_txn.events {
                    let event_type = &event.type_str;
                    
                    // Process Cellana events
                    if event_type == CELLANA_SWAP_EVENT_TYPE {
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
                        if let Ok(event_data) = serde_json::from_str::<serde_json::Value>(&event.data) {
                            if let Ok(swap_data) = self.thala_processor.extract_swap_data(&event_data) {
                                // Only process swaps from our target pools
                                if self.thala_processor.is_target_pool(&swap_data.pool) {
                                    self.thala_processor.process_swap(&mut thala_volumes, swap_data).await;
                                }
                            }
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
                apt_fee_24h: Some(cellana_total_apt_fee.clone()),
                usdc_fee_24h: Some(cellana_total_usdc_fee.clone()),
                usdt_fee_24h: Some(cellana_total_usdt_fee.clone()),
                stapt_fee_24h: Some(cellana_total_stapt_fee.clone()),
                abtc_fee_24h: Some(cellana_total_abtc_fee.clone()),
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
                apt_fee_24h: Some(thala_total_apt_fee.clone()),
                usdc_fee_24h: Some(thala_total_usdc_fee.clone()),
                usdt_fee_24h: Some(thala_total_usdt_fee.clone()),
                stapt_fee_24h: Some(thala_total_stapt_fee.clone()),
                abtc_fee_24h: None, // Thala doesn't support aBTC yet
            };
            
            info!("💾 Created Thala aggregated record: APT={:?}, USDC={:?}, USDT={:?}", 
                apt_data.apt_volume_24h, apt_data.usdc_volume_24h, apt_data.usdt_volume_24h);
            
            results.push(apt_data);
        }

        info!("✅ Successfully processed {} pools in batch", results.len());

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