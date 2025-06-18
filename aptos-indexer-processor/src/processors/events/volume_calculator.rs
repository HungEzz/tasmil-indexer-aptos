use std::collections::HashMap;
use crate::db::common::models::{
    apt_models::NewAptData, 
    coin_volume_models::{NewCoinVolume24h, NewCoinVolumeBucket}
};
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
use std::str::FromStr;
use tracing::{info, debug};

// Import the new modular processors
use super::cellana::{CellanaProcessor, constants::CELLANA_SWAP_EVENT_TYPE};
use super::thala::{ThalaProcessor, constants::THALA_SWAP_EVENT_TYPE};
use super::sushiswap::SushiSwapProcessor;
use super::liquidswap::LiquidSwapProcessor;
use super::hyperion::{HyperionProcessor, constants::HYPERION_SWAP_EVENT_TYPE};
use super::bucket_calculator::{BucketCalculator, SwapEventData, CoinVolumeData};

// Re-export the processor types for internal use
pub use super::cellana::processor::PoolVolume as CellanaPoolVolume;
pub use super::thala::processor::PoolVolume as ThalaPoolVolume;
pub use super::sushiswap::processor::SushiPoolVolume;
pub use super::liquidswap::processor::LiquidPoolVolume;
pub use super::hyperion::processor::PoolVolume as HyperionPoolVolume;

// Helper function to check if a transaction is within the last 24 hours
fn is_within_24h(txn_timestamp_seconds: i64) -> bool {
    let now = Utc::now();
    let cutoff_time = now - Duration::hours(24);
    let txn_time = DateTime::from_timestamp(txn_timestamp_seconds, 0)
        .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
    
    txn_time >= cutoff_time
}

/// VolumeCalculator calculates real-time 24h rolling volume and 2-hour buckets for chart data
pub struct VolumeCalculator {
    cellana_processor: CellanaProcessor,
    thala_processor: ThalaProcessor,
    sushi_swap_processor: SushiSwapProcessor,
    liquid_swap_processor: LiquidSwapProcessor,
    hyperion_processor: HyperionProcessor,
    bucket_calculator: BucketCalculator,
}

impl VolumeCalculator {
    pub fn new() -> Self {
        info!("🚀 Initializing VolumeCalculator with modular architecture and bucket support");
        info!("📊 Configured for Cellana, Thala, SushiSwap, LiquidSwap, and Hyperion volume tracking");
        info!("🕐 Configured for 2-hour GMT+7 buckets for chart data");
        Self {
            cellana_processor: CellanaProcessor::new(),
            thala_processor: ThalaProcessor::new(),
            sushi_swap_processor: SushiSwapProcessor::new(),
            liquid_swap_processor: LiquidSwapProcessor::new(),
            hyperion_processor: HyperionProcessor::new(),
            bucket_calculator: BucketCalculator::new(),
        }
    }
}

#[derive(Debug)]
pub struct VolumeData {
    pub apt_data: Vec<NewAptData>,
    pub coin_volume_data: Vec<NewCoinVolume24h>,
    pub coin_volume_buckets: Vec<NewCoinVolumeBucket>,
}

#[async_trait]
impl Processable for VolumeCalculator {
    type Input = Vec<Transaction>;
    type Output = VolumeData;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<VolumeData>>, ProcessorError> {
        let transactions = item.data;
        if transactions.is_empty() {
            debug!("📭 No transactions to process");
            return Ok(Some(TransactionContext {
                data: VolumeData {
                    apt_data: vec![],
                    coin_volume_data: vec![],
                    coin_volume_buckets: vec![],
                },
                metadata: item.metadata,
            }));
        }

        // Track all pool volumes by protocol and pool
        let mut cellana_volumes: HashMap<String, CellanaPoolVolume> = HashMap::new();
        let mut thala_volumes: HashMap<String, ThalaPoolVolume> = HashMap::new();
        let mut sushi_volumes: HashMap<String, SushiPoolVolume> = HashMap::new();
        let mut liquid_volumes: HashMap<String, LiquidPoolVolume> = HashMap::new();
        let mut hyperion_volumes: HashMap<String, HyperionPoolVolume> = HashMap::new();

        // Collect swap events for bucket processing
        let mut swap_events: Vec<SwapEventData> = Vec::new();
        let current_timestamp = Utc::now().timestamp();

        for txn in &transactions {
            let txn_timestamp = txn.timestamp.as_ref().unwrap().seconds;
            
            // Skip transactions not within 24h
            if !is_within_24h(txn_timestamp) {
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
                                
                                // Collect Cellana for bucket processing (aggregated as "aptos")
                                let coin_volumes = self.extract_coin_volumes_from_cellana(&swap_data);
                                if !coin_volumes.is_empty() {
                                    swap_events.push(SwapEventData {
                                        timestamp_seconds: txn_timestamp,
                                        coin_volumes,
                                    });
                                }
                                
                                // Process all Cellana swaps (removed target pool filter)
                                self.cellana_processor.process_swap(&mut cellana_volumes, swap_data).await;
                            }
                        }
                    }
                    
                    // Process Thala events
                    else if event_type == THALA_SWAP_EVENT_TYPE {
                        tracing::debug!("🔵 Processing Thala event: {}", event_type);
                        if let Ok(event_data) = serde_json::from_str::<serde_json::Value>(&event.data) {
                            if let Ok(swap_data) = self.thala_processor.extract_swap_data(&event_data) {
                                // Collect Thala for bucket processing (aggregated as "aptos")
                                let coin_volumes = self.extract_coin_volumes_from_thala(&swap_data);
                                if !coin_volumes.is_empty() {
                                    swap_events.push(SwapEventData {
                                        timestamp_seconds: txn_timestamp,
                                        coin_volumes,
                                    });
                                }
                                
                                // Process all Thala swaps (removed target pool filter)
                                self.thala_processor.process_swap(&mut thala_volumes, swap_data).await;
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
                                    
                                    // Collect SushiSwap for bucket processing (aggregated as "aptos")
                                    let coin_volumes = self.extract_coin_volumes_from_sushiswap(&swap_data);
                                    if !coin_volumes.is_empty() {
                                        swap_events.push(SwapEventData {
                                            timestamp_seconds: txn_timestamp,
                                            coin_volumes,
                                        });
                                    }
                                    
                                    self.sushi_swap_processor.process_sushiswap(&mut sushi_volumes, swap_data).await;
                                    tracing::info!("✅ SushiSwap swap processed successfully");
                                }
                                Err(e) => {
                                    tracing::error!("❌ Error extracting SushiSwap data: {}", e);
                                }
                            }
                        }
                    }
                    
                    // Process LiquidSwap events
                    else if self.liquid_swap_processor.is_liquidswap_event(event_type) {
                        tracing::info!("🔵 FOUND LIQUIDSWAP EVENT: {}", event_type);
                        
                        if let Ok(event_data) = serde_json::from_str::<serde_json::Value>(&event.data) {
                            match self.liquid_swap_processor.extract_liquidswap_data(&event_data, event_type) {
                                Ok(swap_data) => {
                                    tracing::info!("🔄 Processing LiquidSwap swap: {:?}", swap_data);
                                    
                                    // Collect LiquidSwap for bucket processing (aggregated as "aptos")
                                    let coin_volumes = self.extract_coin_volumes_from_liquidswap(&swap_data);
                                    if !coin_volumes.is_empty() {
                                        swap_events.push(SwapEventData {
                                            timestamp_seconds: txn_timestamp,
                                            coin_volumes,
                                        });
                                    }
                                    
                                    self.liquid_swap_processor.process_liquidswap(&mut liquid_volumes, swap_data).await;
                                    tracing::info!("✅ LiquidSwap swap processed successfully");
                                }
                                Err(e) => {
                                    tracing::error!("❌ Error extracting LiquidSwap data: {}", e);
                                }
                            }
                        }
                    }
                    
                    // Process Hyperion events
                    else if event_type == HYPERION_SWAP_EVENT_TYPE {
                        tracing::info!("🟡 FOUND HYPERION EVENT: {}", event_type);
                        
                        if let Ok(event_data) = serde_json::from_str::<serde_json::Value>(&event.data) {
                            match self.hyperion_processor.extract_swap_data(&event_data) {
                                Ok(swap_data) => {
                                    tracing::info!("🔄 Processing Hyperion swap: {:?}", swap_data);
                                    
                                    // Collect Hyperion for bucket processing (aggregated as "aptos")
                                    let coin_volumes = self.extract_coin_volumes_from_hyperion(&swap_data);
                                    if !coin_volumes.is_empty() {
                                        swap_events.push(SwapEventData {
                                            timestamp_seconds: txn_timestamp,
                                            coin_volumes,
                                        });
                                    }
                                    
                                    // Process all Hyperion swaps (removed target pool filter)
                                    self.hyperion_processor.process_swap(&mut hyperion_volumes, swap_data).await;
                                    tracing::info!("✅ Hyperion swap processed successfully");
                                }
                                Err(e) => {
                                    tracing::error!("❌ Error extracting Hyperion data: {}", e);
                                }
                            }
                        }
                    }
                    else {
                        // Log non-matching events to help debug
                        if event_type.contains("swap") || event_type.contains("Swap") {
                            tracing::info!("❓ Unknown swap event (not Cellana/Thala/SushiSwap/LiquidSwap/Hyperion): {}", event_type);
                        }
                    }
                }
            }
        }

        // Process bucket data
        info!("🪣 Processing {} swap events into 2-hour buckets", swap_events.len());
        let coin_volume_buckets = self.bucket_calculator.group_swaps_into_buckets(swap_events.clone(), current_timestamp);
        info!("✅ Created {} bucket records", coin_volume_buckets.len());

        // Calculate 24h coin volume data from swap events
        let coin_volume_data = self.calculate_24h_coin_volumes(&swap_events);
        info!("📊 Generated {} coin volume 24h records", coin_volume_data.len());

        // Create results for each protocol - aggregate all pools per protocol
        let mut results = Vec::new();

        // Aggregate Cellana volumes across all pools
        let mut cellana_total_apt_volume = BigDecimal::zero();
        let mut cellana_total_usdc_volume = BigDecimal::zero();
        let mut cellana_total_usdt_volume = BigDecimal::zero();
        let mut cellana_total_apt_fee = BigDecimal::zero();
        let mut cellana_total_usdc_fee = BigDecimal::zero();
        let mut cellana_total_usdt_fee = BigDecimal::zero();

        for (_, pool_volume) in &cellana_volumes {
            cellana_total_apt_volume += &pool_volume.apt_volume_24h;
            cellana_total_usdc_volume += &pool_volume.usdc_volume_24h;
            cellana_total_usdt_volume += &pool_volume.usdt_volume_24h;
            cellana_total_apt_fee += &pool_volume.apt_fee_24h;
            cellana_total_usdc_fee += &pool_volume.usdc_fee_24h;
            cellana_total_usdt_fee += &pool_volume.usdt_fee_24h;
        }

        // Create Cellana result if there's any volume
        if cellana_total_apt_volume > BigDecimal::zero() || 
           cellana_total_usdc_volume > BigDecimal::zero() || 
           cellana_total_usdt_volume > BigDecimal::zero() {
            
            let apt_data = NewAptData {
                protocol_name: "cellana".to_string(),
                apt_volume_24h: Some(cellana_total_apt_volume.clone()),
                usdc_volume_24h: Some(cellana_total_usdc_volume.clone()),
                usdt_volume_24h: Some(cellana_total_usdt_volume.clone()),
                weth_volume_24h: None, // Cellana doesn't support WETH yet
                apt_fee_24h: Some(cellana_total_apt_fee.clone()),
                usdc_fee_24h: Some(cellana_total_usdc_fee.clone()),
                usdt_fee_24h: Some(cellana_total_usdt_fee.clone()),
                weth_fee_24h: None, // Cellana doesn't support WETH yet
            };
            
            info!("💾 Created Cellana aggregated record: APT={:?}, USDC={:?}, USDT={:?}", 
                apt_data.apt_volume_24h, apt_data.usdc_volume_24h, apt_data.usdt_volume_24h);
            
            results.push(apt_data);
        }

        // Aggregate Thala volumes across all pools
        let mut thala_total_apt_volume = BigDecimal::zero();
        let mut thala_total_usdc_volume = BigDecimal::zero();
        let mut thala_total_usdt_volume = BigDecimal::zero();
        let mut thala_total_apt_fee = BigDecimal::zero();
        let mut thala_total_usdc_fee = BigDecimal::zero();
        let mut thala_total_usdt_fee = BigDecimal::zero();

        for (_, pool_volume) in &thala_volumes {
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
                weth_volume_24h: None, // Thala doesn't support WETH yet
                apt_fee_24h: Some(thala_total_apt_fee.clone()),
                usdc_fee_24h: Some(thala_total_usdc_fee.clone()),
                usdt_fee_24h: Some(thala_total_usdt_fee.clone()),
                weth_fee_24h: None, // Thala doesn't support WETH yet
            };
            
            info!("💾 Created Thala aggregated record: APT={:?}, USDC={:?}, USDT={:?}", 
                apt_data.apt_volume_24h, apt_data.usdc_volume_24h, apt_data.usdt_volume_24h);
            
            results.push(apt_data);
        }

        // Aggregate SushiSwap volumes across all pools
        let mut sushi_total_apt_volume = BigDecimal::zero();
        let mut sushi_total_usdc_volume = BigDecimal::zero();
        let mut sushi_total_usdt_volume = BigDecimal::zero();
        let mut sushi_total_weth_volume = BigDecimal::zero();

        for (_, pool_volume) in &sushi_volumes {
            sushi_total_apt_volume += &pool_volume.apt_volume_24h;
            sushi_total_usdc_volume += &pool_volume.usdc_volume_24h;
            sushi_total_usdt_volume += &pool_volume.usdt_volume_24h;
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
                usdc_volume_24h: Some(sushi_total_usdc_volume.clone()),
                usdt_volume_24h: Some(sushi_total_usdt_volume.clone()),
                weth_volume_24h: Some(sushi_total_weth_volume.clone()),
                apt_fee_24h: None,
                usdc_fee_24h: None,
                usdt_fee_24h: None,
                weth_fee_24h: None,
            };
            
            info!("💾 Created SushiSwap aggregated record: APT={:?}, USDT={:?}, USDC={:?}, WETH={:?}", 
                apt_data.apt_volume_24h, apt_data.usdt_volume_24h, apt_data.usdc_volume_24h, apt_data.weth_volume_24h);
            
            results.push(apt_data);
        }

        // Aggregate LiquidSwap volumes across all pools
        let mut liquid_total_apt_volume = BigDecimal::zero();
        let mut liquid_total_usdc_volume = BigDecimal::zero();
        let mut liquid_total_usdt_volume = BigDecimal::zero();
        let mut liquid_total_weth_volume = BigDecimal::zero();

        for (_, pool_volume) in &liquid_volumes {
            liquid_total_apt_volume += &pool_volume.apt_volume_24h;
            liquid_total_usdc_volume += &pool_volume.usdc_volume_24h;
            liquid_total_usdt_volume += &pool_volume.usdt_volume_24h;
            liquid_total_weth_volume += &pool_volume.weth_volume_24h;
        }

        // Create LiquidSwap result if there's any volume
        if liquid_total_apt_volume > BigDecimal::zero() || 
           liquid_total_usdc_volume > BigDecimal::zero() ||
           liquid_total_usdt_volume > BigDecimal::zero() ||
           liquid_total_weth_volume > BigDecimal::zero() {
            
            let apt_data = NewAptData {
                protocol_name: "liquidswap".to_string(),
                apt_volume_24h: Some(liquid_total_apt_volume.clone()),
                usdc_volume_24h: Some(liquid_total_usdc_volume.clone()),
                usdt_volume_24h: Some(liquid_total_usdt_volume.clone()),
                weth_volume_24h: Some(liquid_total_weth_volume.clone()),
                apt_fee_24h: None,
                usdc_fee_24h: None,
                usdt_fee_24h: None,
                weth_fee_24h: None,
            };
            
            info!("💾 Created LiquidSwap aggregated record: APT={:?}, USDC={:?}, USDT={:?}, WETH={:?}", 
                apt_data.apt_volume_24h, apt_data.usdc_volume_24h, apt_data.usdt_volume_24h, apt_data.weth_volume_24h);
            
            results.push(apt_data);
        }

        // Aggregate Hyperion volumes
        let mut hyperion_total_apt_volume = BigDecimal::zero();
        let mut hyperion_total_usdc_volume = BigDecimal::zero();
        let mut hyperion_total_usdt_volume = BigDecimal::zero();
        let mut hyperion_total_apt_fee = BigDecimal::zero();
        let mut hyperion_total_usdc_fee = BigDecimal::zero();
        let mut hyperion_total_usdt_fee = BigDecimal::zero();

        for (_, pool_volume) in &hyperion_volumes {
            hyperion_total_apt_volume += &pool_volume.apt_volume_24h;
            hyperion_total_usdc_volume += &pool_volume.usdc_volume_24h;
            hyperion_total_usdt_volume += &pool_volume.usdt_volume_24h;
            hyperion_total_apt_fee += &pool_volume.apt_fee_24h;
            hyperion_total_usdc_fee += &pool_volume.usdc_fee_24h;
            hyperion_total_usdt_fee += &pool_volume.usdt_fee_24h;
        }

        // Create Hyperion result if there's any volume
        if hyperion_total_apt_volume > BigDecimal::zero() || 
           hyperion_total_usdc_volume > BigDecimal::zero() || 
           hyperion_total_usdt_volume > BigDecimal::zero() {
            
            let apt_data = NewAptData {
                protocol_name: "hyperion".to_string(),
                apt_volume_24h: Some(hyperion_total_apt_volume.clone()),
                usdc_volume_24h: Some(hyperion_total_usdc_volume.clone()),
                usdt_volume_24h: Some(hyperion_total_usdt_volume.clone()),
                weth_volume_24h: None, // Hyperion doesn't support WETH
                apt_fee_24h: Some(hyperion_total_apt_fee.clone()),
                usdc_fee_24h: Some(hyperion_total_usdc_fee.clone()),
                usdt_fee_24h: Some(hyperion_total_usdt_fee.clone()),
                weth_fee_24h: None, // Hyperion doesn't support WETH
            };
            
            info!("💾 Created Hyperion aggregated record: APT={:?}, USDC={:?}, USDT={:?}, APT_fee={:?}, USDC_fee={:?}, USDT_fee={:?}", 
                apt_data.apt_volume_24h, apt_data.usdc_volume_24h, apt_data.usdt_volume_24h, 
                apt_data.apt_fee_24h, apt_data.usdc_fee_24h, apt_data.usdt_fee_24h);
            
            results.push(apt_data);
        }

        info!("✅ Successfully processed {} records in batch", results.len());

        Ok(Some(TransactionContext {
            data: VolumeData {
                apt_data: results,
                coin_volume_data: coin_volume_data,
                coin_volume_buckets,
            },
            metadata: item.metadata,
        }))
    }
}

impl VolumeCalculator {
    /// Extract coin volumes from Cellana swap data for bucket processing
    fn extract_coin_volumes_from_cellana(&self, swap_data: &super::cellana::processor::SwapData) -> Vec<CoinVolumeData> {
        let mut coin_volumes = Vec::new();
        
        // Parse amounts using from_str
        if let (Ok(amount_in), Ok(amount_out)) = (
            BigDecimal::from_str(&swap_data.amount_in),
            BigDecimal::from_str(&swap_data.amount_out)
        ) {
            // Add volume for input token
            if let Some(coin) = self.token_type_to_coin(&swap_data.from_token) {
                coin_volumes.push(CoinVolumeData {
                    coin,
                    volume: self.normalize_token_amount(&swap_data.from_token, &amount_in),
                });
            }
            
            // Add volume for output token
            if let Some(coin) = self.token_type_to_coin(&swap_data.to_token) {
                coin_volumes.push(CoinVolumeData {
                    coin,
                    volume: self.normalize_token_amount(&swap_data.to_token, &amount_out),
                });
            }
        }
        
        coin_volumes
    }

    /// Extract coin volumes from Thala swap data for bucket processing
    fn extract_coin_volumes_from_thala(&self, swap_data: &super::thala::processor::SwapData) -> Vec<CoinVolumeData> {
        let mut coin_volumes = Vec::new();
        
        // Parse amounts using from_str
        if let (Ok(amount_in), Ok(amount_out)) = (
            BigDecimal::from_str(&swap_data.amount_in),
            BigDecimal::from_str(&swap_data.amount_out)
        ) {
            // Add volume for input token
            if let Some(coin) = self.token_type_to_coin(&swap_data.from_token) {
                coin_volumes.push(CoinVolumeData {
                    coin,
                    volume: self.normalize_token_amount(&swap_data.from_token, &amount_in),
                });
            }
            
            // Add volume for output token
            if let Some(coin) = self.token_type_to_coin(&swap_data.to_token) {
                coin_volumes.push(CoinVolumeData {
                    coin,
                    volume: self.normalize_token_amount(&swap_data.to_token, &amount_out),
                });
            }
        }
        
        coin_volumes
    }

    /// Extract coin volumes from SushiSwap swap data for bucket processing
    fn extract_coin_volumes_from_sushiswap(&self, swap_data: &super::sushiswap::processor::SushiSwapData) -> Vec<CoinVolumeData> {
        let mut coin_volumes = Vec::new();
        
        // Parse amounts using from_str
        if let (Ok(amount_x_in), Ok(amount_x_out), Ok(amount_y_in), Ok(amount_y_out)) = (
            BigDecimal::from_str(&swap_data.amount_x_in),
            BigDecimal::from_str(&swap_data.amount_x_out),
            BigDecimal::from_str(&swap_data.amount_y_in),
            BigDecimal::from_str(&swap_data.amount_y_out)
        ) {
            // Add volume for token X
            if let Some(coin) = self.token_type_to_coin(&swap_data.token_x) {
                let volume = if amount_x_in > BigDecimal::zero() { amount_x_in } else { amount_x_out };
                coin_volumes.push(CoinVolumeData {
                    coin,
                    volume: self.normalize_token_amount(&swap_data.token_x, &volume),
                });
            }
            
            // Add volume for token Y
            if let Some(coin) = self.token_type_to_coin(&swap_data.token_y) {
                let volume = if amount_y_in > BigDecimal::zero() { amount_y_in } else { amount_y_out };
                coin_volumes.push(CoinVolumeData {
                    coin,
                    volume: self.normalize_token_amount(&swap_data.token_y, &volume),
                });
            }
        }
        
        coin_volumes
    }

    /// Extract coin volumes from LiquidSwap swap data for bucket processing
    fn extract_coin_volumes_from_liquidswap(&self, swap_data: &super::liquidswap::processor::LiquidSwapData) -> Vec<CoinVolumeData> {
        let mut coin_volumes = Vec::new();
        
        // Parse amounts using from_str
        if let (Ok(x_in), Ok(x_out), Ok(y_in), Ok(y_out)) = (
            BigDecimal::from_str(&swap_data.x_in),
            BigDecimal::from_str(&swap_data.x_out),
            BigDecimal::from_str(&swap_data.y_in),
            BigDecimal::from_str(&swap_data.y_out)
        ) {
            // Add volume for token X
            if let Some(coin) = self.token_type_to_coin(&swap_data.token_x) {
                let volume = if x_in > BigDecimal::zero() { x_in } else { x_out };
                coin_volumes.push(CoinVolumeData {
                    coin,
                    volume: self.normalize_token_amount(&swap_data.token_x, &volume),
                });
            }
            
            // Add volume for token Y
            if let Some(coin) = self.token_type_to_coin(&swap_data.token_y) {
                let volume = if y_in > BigDecimal::zero() { y_in } else { y_out };
                coin_volumes.push(CoinVolumeData {
                    coin,
                    volume: self.normalize_token_amount(&swap_data.token_y, &volume),
                });
            }
        }
        
        coin_volumes
    }

    /// Extract coin volumes from Hyperion swap data for bucket processing
    fn extract_coin_volumes_from_hyperion(&self, swap_data: &super::hyperion::processor::SwapData) -> Vec<CoinVolumeData> {
        let mut coin_volumes = Vec::new();
        
        // Parse amounts using from_str
        if let (Ok(amount_in), Ok(amount_out)) = (
            BigDecimal::from_str(&swap_data.amount_in),
            BigDecimal::from_str(&swap_data.amount_out)
        ) {
            // Add volume for input token
            if let Some(coin) = self.token_type_to_coin(&swap_data.from_token) {
                coin_volumes.push(CoinVolumeData {
                    coin,
                    volume: self.normalize_token_amount(&swap_data.from_token, &amount_in),
                });
            }
            
            // Add volume for output token
            if let Some(coin) = self.token_type_to_coin(&swap_data.to_token) {
                coin_volumes.push(CoinVolumeData {
                    coin,
                    volume: self.normalize_token_amount(&swap_data.to_token, &amount_out),
                });
            }
        }

        coin_volumes
    }

    /// Convert token type to standardized coin name
    fn token_type_to_coin(&self, token_type: &str) -> Option<String> {
        // APT coin from all DEXes
        if token_type == super::cellana::constants::APT_COIN_TYPE || 
           token_type == super::thala::constants::APT_COIN_TYPE ||
           token_type == super::hyperion::constants::APT_COIN_TYPE ||
           token_type == super::liquidswap::constants::APT_COIN_TYPE ||
           token_type == super::sushiswap::constants::APT_COIN_TYPE {
            Some("APT".to_string())
        }
        // USDC and equivalent tokens
        else if token_type.contains("USDC") || 
                token_type == super::cellana::constants::USDC_COIN_TYPE ||
                token_type == super::thala::constants::USDC_COIN_TYPE ||
                token_type == super::hyperion::constants::USDC_COIN_TYPE ||
                token_type == super::sushiswap::constants::IZUSDC_COIN_TYPE ||
                token_type == super::sushiswap::constants::WHUSDC_COIN_TYPE ||
                token_type == super::liquidswap::constants::IZUSDC_COIN_TYPE ||
                token_type == super::liquidswap::constants::WHUSDC_COIN_TYPE {
            Some("USDC".to_string())
        }
        // USDT and equivalent tokens
        else if token_type.contains("USDT") || 
                token_type == super::cellana::constants::USDT_COIN_TYPE ||
                token_type == super::thala::constants::USDT_COIN_TYPE ||
                token_type == super::hyperion::constants::USDT_COIN_TYPE ||
                token_type == super::sushiswap::constants::IZUSDT_COIN_TYPE ||
                token_type == super::liquidswap::constants::IZUSDT_COIN_TYPE ||
                token_type == super::liquidswap::constants::WHUSDT_COIN_TYPE {
            Some("USDT".to_string())
        }
        // WETH and equivalent tokens
        else if token_type.contains("WETH") || 
                token_type == super::sushiswap::constants::IZWETH_COIN_TYPE ||
                token_type == super::liquidswap::constants::IZWETH_COIN_TYPE ||
                token_type == super::liquidswap::constants::WHWETH_COIN_TYPE {
            Some("WETH".to_string())
        }
        else {
            None
        }
    }

    /// Normalize token amount based on decimals
    fn normalize_token_amount(&self, token_type: &str, raw_amount: &BigDecimal) -> BigDecimal {
        // Use the same token detection logic as token_type_to_coin
        let divisor = if token_type == super::cellana::constants::APT_COIN_TYPE || 
           token_type == super::thala::constants::APT_COIN_TYPE ||
           token_type == super::hyperion::constants::APT_COIN_TYPE ||
           token_type == super::liquidswap::constants::APT_COIN_TYPE ||
           token_type == super::sushiswap::constants::APT_COIN_TYPE {
            // APT has 8 decimals
            BigDecimal::from(10_u64.pow(8))
        } else if token_type.contains("USDC") || 
                token_type == super::cellana::constants::USDC_COIN_TYPE ||
                token_type == super::thala::constants::USDC_COIN_TYPE ||
                token_type == super::hyperion::constants::USDC_COIN_TYPE ||
                token_type == super::sushiswap::constants::IZUSDC_COIN_TYPE ||
                token_type == super::sushiswap::constants::WHUSDC_COIN_TYPE ||
                token_type == super::liquidswap::constants::IZUSDC_COIN_TYPE ||
                token_type == super::liquidswap::constants::WHUSDC_COIN_TYPE {
            // USDC has 6 decimals
            BigDecimal::from(10_u64.pow(6))
        } else if token_type.contains("USDT") || 
                token_type == super::cellana::constants::USDT_COIN_TYPE ||
                token_type == super::thala::constants::USDT_COIN_TYPE ||
                token_type == super::hyperion::constants::USDT_COIN_TYPE ||
                token_type == super::sushiswap::constants::IZUSDT_COIN_TYPE ||
                token_type == super::liquidswap::constants::IZUSDT_COIN_TYPE ||
                token_type == super::liquidswap::constants::WHUSDT_COIN_TYPE {
            // USDT has 6 decimals
            BigDecimal::from(10_u64.pow(6))
        } else if token_type.contains("WETH") || 
                token_type == super::sushiswap::constants::IZWETH_COIN_TYPE ||
                token_type == super::liquidswap::constants::IZWETH_COIN_TYPE ||
                token_type == super::liquidswap::constants::WHWETH_COIN_TYPE {
            // WETH has 6 decimals
            BigDecimal::from(10_u64.pow(6))
        } else {
            // Default to no normalization
            BigDecimal::from(1)
        };
        
        raw_amount / divisor
    }

    /// Calculate 24h coin volume data from swap events
    fn calculate_24h_coin_volumes(&self, swap_events: &Vec<SwapEventData>) -> Vec<NewCoinVolume24h> {
        let mut coin_volumes: HashMap<String, BigDecimal> = HashMap::new();
        
        // Aggregate volumes by coin
        for event in swap_events {
            for coin_volume in &event.coin_volumes {
                let current_volume = coin_volumes.entry(coin_volume.coin.clone())
                    .or_insert_with(|| BigDecimal::zero());
                *current_volume += &coin_volume.volume;
            }
        }
        
        // Convert to NewCoinVolume24h records
        let mut coin_volume_data = Vec::new();
        for (coin, volume) in coin_volumes {
            coin_volume_data.push(NewCoinVolume24h {
                coin,
                buy_volume: Some(volume.clone()),
                sell_volume: Some(volume), // For now, treat all volume as both buy and sell
            });
        }
        
        coin_volume_data
    }
}

#[async_trait]
impl AsyncStep for VolumeCalculator {}

impl NamedStep for VolumeCalculator {
    fn name(&self) -> String {
        "VolumeCalculator".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigdecimal::FromPrimitive;
    use crate::processors::events::cellana::constants as cellana_constants;
    use crate::processors::events::thala::constants as thala_constants;
    use crate::processors::events::liquidswap::constants as liquidswap_constants;
    
    #[test]
    fn test_normalize_token_amount() {
        // Create a VolumeCalculator instance
        let calculator = VolumeCalculator::new();
        
        // Test APT normalization (8 decimals)
        let apt_raw = BigDecimal::from_u64(100_000_000).unwrap(); // 1 APT in raw form
        let apt_normalized = calculator.normalize_token_amount(cellana_constants::APT_COIN_TYPE, &apt_raw);
        assert_eq!(apt_normalized, BigDecimal::from_u64(1).unwrap(), "APT normalization failed");
        
        // Test APT normalization with Thala format
        let apt_raw = BigDecimal::from_u64(100_000_000).unwrap(); // 1 APT in raw form
        let apt_normalized = calculator.normalize_token_amount(thala_constants::APT_COIN_TYPE, &apt_raw);
        assert_eq!(apt_normalized, BigDecimal::from_u64(1).unwrap(), "APT (Thala) normalization failed");
        
        // Test USDC normalization (6 decimals)
        let usdc_raw = BigDecimal::from_u64(1_000_000).unwrap(); // 1 USDC in raw form
        let usdc_normalized = calculator.normalize_token_amount(cellana_constants::USDC_COIN_TYPE, &usdc_raw);
        assert_eq!(usdc_normalized, BigDecimal::from_u64(1).unwrap(), "USDC normalization failed");
        
        // Test USDT normalization (6 decimals)
        let usdt_raw = BigDecimal::from_u64(1_000_000).unwrap(); // 1 USDT in raw form
        let usdt_normalized = calculator.normalize_token_amount(cellana_constants::USDT_COIN_TYPE, &usdt_raw);
        assert_eq!(usdt_normalized, BigDecimal::from_u64(1).unwrap(), "USDT normalization failed");
        
        // Test WETH normalization (6 decimals)
        let weth_raw = BigDecimal::from_u64(1_000_000).unwrap(); // 1 WETH in raw form
        let weth_normalized = calculator.normalize_token_amount(liquidswap_constants::WHWETH_COIN_TYPE, &weth_raw);
        assert_eq!(weth_normalized, BigDecimal::from_u64(1).unwrap(), "WETH normalization failed");
        
        // Test unknown token (should not normalize)
        let unknown_raw = BigDecimal::from_u64(1_000_000).unwrap();
        let unknown_normalized = calculator.normalize_token_amount("0xunknown::coin::Type", &unknown_raw);
        assert_eq!(unknown_normalized, unknown_raw, "Unknown token should not be normalized");
        
        // Test with large numbers - kiểm tra APT với số lớn
        let large_apt_raw = BigDecimal::from_str("12345678900000000").unwrap(); // 123,456.789 APT in raw form
        let large_apt_normalized = calculator.normalize_token_amount(cellana_constants::APT_COIN_TYPE, &large_apt_raw);
        
        // Kiểm tra kết quả bằng cách tính toán thủ công
        let expected_value = large_apt_raw.clone() / BigDecimal::from(10_u64.pow(8));
        
        // In ra các giá trị để debug
        println!("Large APT normalized: {}", large_apt_normalized);
        println!("Expected manual calculation: {}", expected_value);
        
        // So sánh với kết quả tính toán thủ công
        assert_eq!(large_apt_normalized, expected_value, "Large APT normalization failed");
        
        // Test with large numbers - kiểm tra WETH với số lớn
        let large_weth_raw = BigDecimal::from_str("5432100000000").unwrap(); // 5,432.1 WETH in raw form
        let large_weth_normalized = calculator.normalize_token_amount(liquidswap_constants::WHWETH_COIN_TYPE, &large_weth_raw);
        
        // Kiểm tra kết quả bằng cách tính toán thủ công
        let expected_weth_value = large_weth_raw.clone() / BigDecimal::from(10_u64.pow(6));
        
        // In ra các giá trị để debug
        println!("Large WETH raw: {}", large_weth_raw);
        println!("Large WETH normalized: {}", large_weth_normalized);
        println!("Expected WETH calculation: {}", expected_weth_value);
        
        // So sánh với kết quả tính toán thủ công
        assert_eq!(large_weth_normalized, expected_weth_value, "Large WETH normalization failed");
        
        println!("✅ All token normalization tests passed!");
    }
} 