use super::constants::*;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{Transaction, WriteSetChange},
};
use bigdecimal::{BigDecimal, Zero, FromPrimitive};
use serde_json;
use std::{collections::HashMap, str::FromStr};
use tracing::{info, debug};

#[derive(Debug)]
pub struct SwapData {
    pub amount_in: String,
    pub amount_out: String,
    pub from_token: String,
    pub to_token: String,
    pub pool: String,
    pub swap_fee_bps: u32,
}

#[derive(Debug)]
pub struct PoolVolume {
    pub pool: String,
    pub apt_volume_24h: BigDecimal,
    pub usdc_volume_24h: BigDecimal,
    pub usdt_volume_24h: BigDecimal,
    pub stapt_volume_24h: BigDecimal,
    pub abtc_volume_24h: BigDecimal,
    pub apt_fee_24h: BigDecimal,
    pub usdc_fee_24h: BigDecimal,
    pub usdt_fee_24h: BigDecimal,
    pub stapt_fee_24h: BigDecimal,
    pub abtc_fee_24h: BigDecimal,
}

// Cached decimal divisors for performance
struct DecimalDivisors {
    apt: BigDecimal,
    usdc: BigDecimal,
    usdt: BigDecimal,
    stapt: BigDecimal,
    abtc: BigDecimal,
}

impl DecimalDivisors {
    fn new() -> Self {
        Self {
            apt: BigDecimal::from_u64(10_u64.pow(APT_DECIMALS as u32)).unwrap(),
            usdc: BigDecimal::from_u64(10_u64.pow(USDC_DECIMALS as u32)).unwrap(),
            usdt: BigDecimal::from_u64(10_u64.pow(USDT_DECIMALS as u32)).unwrap(),
            stapt: BigDecimal::from_u64(10_u64.pow(STAPT_DECIMALS as u32)).unwrap(),
            abtc: BigDecimal::from_u64(10_u64.pow(ABTC_DECIMALS as u32)).unwrap(),
        }
    }
}

// Pool addresses as a constant array for efficient lookup
pub const TARGET_POOLS: &[&str] = &[
    APT_USDC_POOL_ADDRESS,
    USDT_USDC_POOL_ADDRESS,
    APT_USDT_POOL_ADDRESS,
    APT_STAPT_POOL_ADDRESS,
    ABTC_APT_POOL_ADDRESS,
];

impl Default for PoolVolume {
    fn default() -> Self {
        Self {
            pool: String::new(),
            apt_volume_24h: BigDecimal::from(0),
            usdc_volume_24h: BigDecimal::from(0),
            usdt_volume_24h: BigDecimal::from(0),
            stapt_volume_24h: BigDecimal::from(0),
            abtc_volume_24h: BigDecimal::from(0),
            apt_fee_24h: BigDecimal::from(0),
            usdc_fee_24h: BigDecimal::from(0),
            usdt_fee_24h: BigDecimal::from(0),
            stapt_fee_24h: BigDecimal::from(0),
            abtc_fee_24h: BigDecimal::from(0),
        }
    }
}

pub struct CellanaProcessor {
    divisors: DecimalDivisors,
}

impl CellanaProcessor {
    pub fn new() -> Self {
        Self {
            divisors: DecimalDivisors::new(),
        }
    }

    pub fn extract_swap_data(&self, event_data: &serde_json::Value) -> Result<SwapData> {
        debug!("🔍 Extracting Cellana swap data from event");
        
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

        debug!("✅ Extracted Cellana swap: {} {} -> {} {}", 
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

    pub fn extract_swap_fee_bps(&self, txn: &Transaction, pool_address: &str) -> u32 {
        // Try to get changes from the transaction info field
        let changes = match &txn.info {
            Some(info) => &info.changes,
            None => return 30, // Default fee for Cellana (0.3%)
        };

        for change in changes {
            if let WriteSetChange { 
                change: Some(aptos_indexer_processor_sdk::aptos_protos::transaction::v1::write_set_change::Change::WriteResource(resource)), 
                .. 
            } = change {
                if resource.address == pool_address && resource.type_str.contains("liquidity_pool::LiquidityPool") {
                    if let Ok(pool_data) = serde_json::from_str::<serde_json::Value>(&resource.data) {
                        if let Some(swap_fee_bps) = pool_data.get("swap_fee_bps")
                            .and_then(|v| v.as_str())
                            .and_then(|v| v.parse::<u32>().ok()) {
                            debug!("🔧 Found swap_fee_bps: {} for pool {}", swap_fee_bps, pool_address);
                            return swap_fee_bps;
                        }
                        if let Some(fee_rate) = pool_data.get("fee_rate")
                            .and_then(|v| v.as_str())
                            .and_then(|v| v.parse::<u32>().ok()) {
                            debug!("🔧 Found fee_rate: {} for pool {}", fee_rate, pool_address);
                            return fee_rate;
                        }
                    }
                }
            }
        }

        debug!("⚠️ No fee_rate found in transaction for pool {}, using default 30 bps", pool_address);
        30 // Default fee for Cellana (0.3%)
    }

    pub async fn process_swap(&self, pool_volumes: &mut HashMap<String, PoolVolume>, swap_data: SwapData) {
        // Get or create pool volume entry with optimized default
        let pool_entry = pool_volumes.entry(swap_data.pool.clone()).or_insert_with(|| {
            PoolVolume {
                pool: swap_data.pool.clone(),
                ..Default::default()
            }
        });

        let raw_amount_in = BigDecimal::from_str(&swap_data.amount_in).unwrap_or_else(|_| BigDecimal::zero());
        let raw_amount_out = BigDecimal::from_str(&swap_data.amount_out).unwrap_or_else(|_| BigDecimal::zero());
        let fee_rate = BigDecimal::from(swap_data.swap_fee_bps) / BigDecimal::from(10000);

        // Handle APT/USDC pool swaps
        if swap_data.pool == APT_USDC_POOL_ADDRESS {
            self.process_apt_usdc_swap(pool_entry, &swap_data, &raw_amount_in, &raw_amount_out, &fee_rate).await;
        }
        // Handle USDT/USDC pool swaps
        else if swap_data.pool == USDT_USDC_POOL_ADDRESS {
            self.process_usdt_usdc_swap(pool_entry, &swap_data, &raw_amount_in, &raw_amount_out, &fee_rate).await;
        }
        // Handle APT/USDT pool swaps
        else if swap_data.pool == APT_USDT_POOL_ADDRESS {
            self.process_apt_usdt_swap(pool_entry, &swap_data, &raw_amount_in, &raw_amount_out, &fee_rate).await;
        }
        // Handle APT/stAPT pool swaps
        else if swap_data.pool == APT_STAPT_POOL_ADDRESS {
            self.process_apt_stapt_swap(pool_entry, &swap_data, &raw_amount_in, &raw_amount_out, &fee_rate).await;
        }
        // Handle aBTC/APT pool swaps
        else if swap_data.pool == ABTC_APT_POOL_ADDRESS {
            self.process_abtc_apt_swap(pool_entry, &swap_data, &raw_amount_in, &raw_amount_out, &fee_rate).await;
        }
    }

    async fn process_apt_usdc_swap(
        &self,
        pool_entry: &mut PoolVolume,
        swap_data: &SwapData,
        raw_amount_in: &BigDecimal,
        raw_amount_out: &BigDecimal,
        fee_rate: &BigDecimal,
    ) {
        if swap_data.from_token == APT_COIN_TYPE && swap_data.to_token == USDC_COIN_TYPE {
            // APT -> USDC
            let apt_amount = raw_amount_in / &self.divisors.apt;
            let usdc_amount = raw_amount_out / &self.divisors.usdc;
            let apt_fee = &apt_amount * fee_rate;
            let apt_net_volume = &apt_amount - &apt_fee;
            
            pool_entry.apt_volume_24h += apt_net_volume.clone();
            pool_entry.usdc_volume_24h += usdc_amount.clone();
            pool_entry.apt_fee_24h += apt_fee.clone();
            
            info!("📈 Cellana APT->USDC: {} APT sold, {} USDC bought, {} APT fee ({}bps)", 
                apt_amount, usdc_amount, apt_fee, swap_data.swap_fee_bps);
                
        } else if swap_data.from_token == USDC_COIN_TYPE && swap_data.to_token == APT_COIN_TYPE {
            // USDC -> APT
            let usdc_amount = raw_amount_in / &self.divisors.usdc;
            let apt_amount = raw_amount_out / &self.divisors.apt;
            let usdc_fee = &usdc_amount * fee_rate;
            let usdc_net_volume = &usdc_amount - &usdc_fee;
            
            pool_entry.apt_volume_24h += apt_amount.clone();
            pool_entry.usdc_volume_24h += usdc_net_volume.clone();
            pool_entry.usdc_fee_24h += usdc_fee.clone();
            
            info!("📉 Cellana USDC->APT: {} USDC sold, {} APT bought, {} USDC fee ({}bps)", 
                usdc_amount, apt_amount, usdc_fee, swap_data.swap_fee_bps);
        }
    }

    async fn process_usdt_usdc_swap(
        &self,
        pool_entry: &mut PoolVolume,
        swap_data: &SwapData,
        raw_amount_in: &BigDecimal,
        raw_amount_out: &BigDecimal,
        fee_rate: &BigDecimal,
    ) {
        if swap_data.from_token == USDT_COIN_TYPE && swap_data.to_token == USDC_COIN_TYPE {
            // USDT -> USDC
            let usdt_amount = raw_amount_in / &self.divisors.usdt;
            let usdc_amount = raw_amount_out / &self.divisors.usdc;
            let usdt_fee = &usdt_amount * fee_rate;
            let usdt_net_volume = &usdt_amount - &usdt_fee;
            
            pool_entry.usdt_volume_24h += usdt_net_volume.clone();
            pool_entry.usdc_volume_24h += usdc_amount.clone();
            pool_entry.usdt_fee_24h += usdt_fee.clone();
            
            info!("💰 Cellana USDT->USDC: {} USDT sold, {} USDC bought, {} USDT fee ({}bps)", 
                usdt_amount, usdc_amount, usdt_fee, swap_data.swap_fee_bps);
                
        } else if swap_data.from_token == USDC_COIN_TYPE && swap_data.to_token == USDT_COIN_TYPE {
            // USDC -> USDT
            let usdc_amount = raw_amount_in / &self.divisors.usdc;
            let usdt_amount = raw_amount_out / &self.divisors.usdt;
            let usdc_fee = &usdc_amount * fee_rate;
            let usdc_net_volume = &usdc_amount - &usdc_fee;
            
            pool_entry.usdt_volume_24h += usdt_amount.clone();
            pool_entry.usdc_volume_24h += usdc_net_volume.clone();
            pool_entry.usdc_fee_24h += usdc_fee.clone();
            
            info!("💸 Cellana USDC->USDT: {} USDC sold, {} USDT bought, {} USDC fee ({}bps)", 
                usdc_amount, usdt_amount, usdc_fee, swap_data.swap_fee_bps);
        }
    }

    async fn process_apt_usdt_swap(
        &self,
        pool_entry: &mut PoolVolume,
        swap_data: &SwapData,
        raw_amount_in: &BigDecimal,
        raw_amount_out: &BigDecimal,
        fee_rate: &BigDecimal,
    ) {
        if swap_data.from_token == APT_COIN_TYPE && swap_data.to_token == USDT_COIN_TYPE {
            // APT -> USDT
            let apt_amount = raw_amount_in / &self.divisors.apt;
            let usdt_amount = raw_amount_out / &self.divisors.usdt;
            let apt_fee = &apt_amount * fee_rate;
            let apt_net_volume = &apt_amount - &apt_fee;
            
            pool_entry.apt_volume_24h += apt_net_volume.clone();
            pool_entry.usdt_volume_24h += usdt_amount.clone();
            pool_entry.apt_fee_24h += apt_fee.clone();
            
            info!("📈 Cellana APT->USDT: {} APT sold, {} USDT bought, {} APT fee ({}bps)", 
                apt_amount, usdt_amount, apt_fee, swap_data.swap_fee_bps);
                
        } else if swap_data.from_token == USDT_COIN_TYPE && swap_data.to_token == APT_COIN_TYPE {
            // USDT -> APT
            let usdt_amount = raw_amount_in / &self.divisors.usdt;
            let apt_amount = raw_amount_out / &self.divisors.apt;
            let usdt_fee = &usdt_amount * fee_rate;
            let usdt_net_volume = &usdt_amount - &usdt_fee;
            
            pool_entry.apt_volume_24h += apt_amount.clone();
            pool_entry.usdt_volume_24h += usdt_net_volume.clone();
            pool_entry.usdt_fee_24h += usdt_fee.clone();
            
            info!("📉 Cellana USDT->APT: {} USDT sold, {} APT bought, {} USDT fee ({}bps)", 
                usdt_amount, apt_amount, usdt_fee, swap_data.swap_fee_bps);
        }
    }

    async fn process_apt_stapt_swap(
        &self,
        pool_entry: &mut PoolVolume,
        swap_data: &SwapData,
        raw_amount_in: &BigDecimal,
        raw_amount_out: &BigDecimal,
        fee_rate: &BigDecimal,
    ) {
        if swap_data.from_token == APT_COIN_TYPE && swap_data.to_token == STAPT_COIN_TYPE {
            // APT -> stAPT
            let apt_amount = raw_amount_in / &self.divisors.apt;
            let stapt_amount = raw_amount_out / &self.divisors.stapt;
            let apt_fee = &apt_amount * fee_rate;
            let apt_net_volume = &apt_amount - &apt_fee;
            
            pool_entry.apt_volume_24h += apt_net_volume.clone();
            pool_entry.stapt_volume_24h += stapt_amount.clone();
            pool_entry.apt_fee_24h += apt_fee.clone();
            
            info!("📉 Cellana APT->stAPT: {} APT sold, {} stAPT bought, {} APT fee ({}bps)", 
                apt_amount, stapt_amount, apt_fee, swap_data.swap_fee_bps);
                
        } else if swap_data.from_token == STAPT_COIN_TYPE && swap_data.to_token == APT_COIN_TYPE {
            // stAPT -> APT
            let stapt_amount = raw_amount_in / &self.divisors.stapt;
            let apt_amount = raw_amount_out / &self.divisors.apt;
            let stapt_fee = &stapt_amount * fee_rate;
            let stapt_net_volume = &stapt_amount - &stapt_fee;
            
            pool_entry.apt_volume_24h += apt_amount.clone();
            pool_entry.stapt_volume_24h += stapt_net_volume.clone();
            pool_entry.stapt_fee_24h += stapt_fee.clone();
            
            info!("📉 Cellana stAPT->APT: {} stAPT sold, {} APT bought, {} stAPT fee ({}bps)", 
                stapt_amount, apt_amount, stapt_fee, swap_data.swap_fee_bps);
        }
    }

    async fn process_abtc_apt_swap(
        &self,
        pool_entry: &mut PoolVolume,
        swap_data: &SwapData,
        raw_amount_in: &BigDecimal,
        raw_amount_out: &BigDecimal,
        fee_rate: &BigDecimal,
    ) {
        if swap_data.from_token == ABTC_COIN_TYPE && swap_data.to_token == APT_COIN_TYPE {
            // aBTC -> APT
            let abtc_amount = raw_amount_in / &self.divisors.abtc;
            let apt_amount = raw_amount_out / &self.divisors.apt;
            let abtc_fee = &abtc_amount * fee_rate;
            let abtc_net_volume = &abtc_amount - &abtc_fee;
            
            pool_entry.abtc_volume_24h += abtc_net_volume.clone();
            pool_entry.apt_volume_24h += apt_amount.clone();
            pool_entry.abtc_fee_24h += abtc_fee.clone();
            
            info!("📉 Cellana aBTC->APT: {} aBTC sold, {} APT bought, {} aBTC fee ({}bps)", 
                abtc_amount, apt_amount, abtc_fee, swap_data.swap_fee_bps);
                
        } else if swap_data.from_token == APT_COIN_TYPE && swap_data.to_token == ABTC_COIN_TYPE {
            // APT -> aBTC
            let apt_amount = raw_amount_in / &self.divisors.apt;
            let abtc_amount = raw_amount_out / &self.divisors.abtc;
            let apt_fee = &apt_amount * fee_rate;
            let apt_net_volume = &apt_amount - &apt_fee;
            
            pool_entry.apt_volume_24h += apt_net_volume.clone();
            pool_entry.abtc_volume_24h += abtc_amount.clone();
            pool_entry.apt_fee_24h += apt_fee.clone();
            
            info!("📈 Cellana APT->aBTC: {} APT sold, {} aBTC bought, {} APT fee ({}bps)", 
                apt_amount, abtc_amount, apt_fee, swap_data.swap_fee_bps);
        }
    }

    pub fn is_target_pool(&self, pool_address: &str) -> bool {
        TARGET_POOLS.contains(&pool_address)
    }
} 