use super::constants::*;
use anyhow::Result;
use bigdecimal::{BigDecimal, Zero};
use serde_json;
use std::{collections::HashMap, str::FromStr};
use tracing::{info, debug};

// Cached decimal divisors for performance
struct DecimalDivisors {
    apt: BigDecimal,
    usdc: BigDecimal,
    usdt: BigDecimal,
}

impl DecimalDivisors {
    fn new() -> Self {
        Self {
            apt: BigDecimal::from(10_u64.pow(APT_DECIMALS as u32)),
            usdc: BigDecimal::from(10_u64.pow(USDC_DECIMALS as u32)),
            usdt: BigDecimal::from(10_u64.pow(USDT_DECIMALS as u32)),
        }
    }
}

// Pool addresses as a constant array for efficient lookup
const TARGET_POOLS: [&str; 3] = [
    THALA_APT_USDC_POOL_ADDRESS,
    THALA_USDT_USDC_POOL_ADDRESS,
    THALA_APT_USDT_POOL_ADDRESS,
];

#[derive(Debug)]
pub struct SwapData {
    pub amount_in: String,
    pub amount_out: String,
    pub from_token: String,
    pub to_token: String,
    pub pool: String,
    pub protocol_fee_amount: String,
}

#[derive(Debug)]
pub struct PoolVolume {
    pub pool: String,
    pub apt_volume_24h: BigDecimal,
    pub usdc_volume_24h: BigDecimal,
    pub usdt_volume_24h: BigDecimal,
    pub apt_fee_24h: BigDecimal,
    pub usdc_fee_24h: BigDecimal,
    pub usdt_fee_24h: BigDecimal,
    pub apt_buy_volume_24h: BigDecimal,
    pub apt_sell_volume_24h: BigDecimal,
    pub usdc_buy_volume_24h: BigDecimal,
    pub usdc_sell_volume_24h: BigDecimal,
    pub usdt_buy_volume_24h: BigDecimal,
    pub usdt_sell_volume_24h: BigDecimal,
}

impl Default for PoolVolume {
    fn default() -> Self {
        Self {
            pool: String::new(),
            apt_volume_24h: BigDecimal::zero(),
            usdc_volume_24h: BigDecimal::zero(),
            usdt_volume_24h: BigDecimal::zero(),
            apt_fee_24h: BigDecimal::zero(),
            usdc_fee_24h: BigDecimal::zero(),
            usdt_fee_24h: BigDecimal::zero(),
            apt_buy_volume_24h: BigDecimal::zero(),
            apt_sell_volume_24h: BigDecimal::zero(),
            usdc_buy_volume_24h: BigDecimal::zero(),
            usdc_sell_volume_24h: BigDecimal::zero(),
            usdt_buy_volume_24h: BigDecimal::zero(),
            usdt_sell_volume_24h: BigDecimal::zero(),
        }
    }
}

pub struct ThalaProcessor {
    divisors: DecimalDivisors,
}

impl ThalaProcessor {
    pub fn new() -> Self {
        Self {
            divisors: DecimalDivisors::new(),
        }
    }

    pub fn extract_swap_data(&self, event_data: &serde_json::Value) -> Result<SwapData> {
        debug!("🔍 Extracting Thala swap data from event");
        
        // Extract indices with early return on error
        let idx_in = event_data
            .get("idx_in")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<usize>().ok())
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid idx_in"))?;
            
        let idx_out = event_data
            .get("idx_out")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<usize>().ok())
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid idx_out"))?;

        // Extract amounts (required fields)
        let amount_in = event_data
            .get("amount_in")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing amount_in"))?;
            
        let amount_out = event_data
            .get("amount_out")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing amount_out"))?;

        // Extract optional protocol fee
        let protocol_fee_amount = event_data
            .get("protocol_fee_amount")
            .and_then(|v| v.as_str())
            .unwrap_or("0");

        // Extract pool address
        let pool_address = event_data
            .get("pool_obj")
            .and_then(|obj| obj.get("inner"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing pool_obj.inner"))?;

        // Extract coin types from metadata
        let metadata = event_data
            .get("metadata")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid metadata array"))?;

        if metadata.len() < 2 {
            return Err(anyhow::anyhow!("Metadata array must have at least 2 elements"));
        }

        let coin_type_0 = metadata[0]
            .get("inner")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing metadata[0].inner"))?;

        let coin_type_1 = metadata[1]
            .get("inner")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing metadata[1].inner"))?;

        // Determine token direction based on indices
        let (from_token, to_token) = match (idx_in, idx_out) {
            (0, 1) => (coin_type_0, coin_type_1),
            (1, 0) => (coin_type_1, coin_type_0),
            _ => return Err(anyhow::anyhow!("Invalid idx combination: idx_in={}, idx_out={}", idx_in, idx_out)),
        };

        debug!("✅ Extracted Thala swap: {} {} -> {} {} (pool: {}, fee: {})", 
            amount_in, from_token, amount_out, to_token, pool_address, protocol_fee_amount);

        Ok(SwapData {
            amount_in: amount_in.to_string(),
            amount_out: amount_out.to_string(),
            from_token: from_token.to_string(),
            to_token: to_token.to_string(),
            pool: pool_address.to_string(),
            protocol_fee_amount: protocol_fee_amount.to_string(),
        })
    }

    pub async fn process_swap(&self, pool_volumes: &mut HashMap<String, PoolVolume>, swap_data: SwapData) {
        // Get or create pool volume entry with optimized default
        let pool_entry = pool_volumes.entry(swap_data.pool.clone()).or_insert_with(|| {
            PoolVolume {
                pool: swap_data.pool.clone(),
                ..Default::default()
            }
        });

        // Parse amounts once with error handling
        let raw_amount_in = BigDecimal::from_str(&swap_data.amount_in).unwrap_or_else(|_| BigDecimal::zero());
        let raw_amount_out = BigDecimal::from_str(&swap_data.amount_out).unwrap_or_else(|_| BigDecimal::zero());
        let protocol_fee = BigDecimal::from_str(&swap_data.protocol_fee_amount).unwrap_or_else(|_| BigDecimal::zero());

        // Process the swap with unified logic
        self.process_thala_swap(pool_entry, &swap_data, &raw_amount_in, &raw_amount_out, &protocol_fee).await;
    }

    async fn process_thala_swap(
        &self,
        pool_entry: &mut PoolVolume,
        swap_data: &SwapData,
        raw_amount_in: &BigDecimal,
        raw_amount_out: &BigDecimal,
        protocol_fee: &BigDecimal,
    ) {
        // Match swap pairs and delegate to unified processing
        match (swap_data.from_token.as_str(), swap_data.to_token.as_str()) {
            (APT_COIN_TYPE, USDC_COIN_TYPE) => {
                self.process_swap_pair(
                    pool_entry, "APT", "USDC", raw_amount_in, raw_amount_out, protocol_fee,
                    &self.divisors.apt, &self.divisors.usdc, "📈", "APT->USDC"
                ).await;
            },
            (USDC_COIN_TYPE, APT_COIN_TYPE) => {
                self.process_swap_pair(
                    pool_entry, "USDC", "APT", raw_amount_in, raw_amount_out, protocol_fee,
                    &self.divisors.usdc, &self.divisors.apt, "📉", "USDC->APT"
                ).await;
            },
            (USDT_COIN_TYPE, USDC_COIN_TYPE) => {
                self.process_swap_pair(
                    pool_entry, "USDT", "USDC", raw_amount_in, raw_amount_out, protocol_fee,
                    &self.divisors.usdt, &self.divisors.usdc, "📈", "USDT->USDC"
                ).await;
            },
            (USDC_COIN_TYPE, USDT_COIN_TYPE) => {
                self.process_swap_pair(
                    pool_entry, "USDC", "USDT", raw_amount_in, raw_amount_out, protocol_fee,
                    &self.divisors.usdc, &self.divisors.usdt, "📉", "USDC->USDT"
                ).await;
            },
            (APT_COIN_TYPE, USDT_COIN_TYPE) => {
                self.process_swap_pair(
                    pool_entry, "APT", "USDT", raw_amount_in, raw_amount_out, protocol_fee,
                    &self.divisors.apt, &self.divisors.usdt, "📈", "APT->USDT"
                ).await;
            },
            (USDT_COIN_TYPE, APT_COIN_TYPE) => {
                self.process_swap_pair(
                    pool_entry, "USDT", "APT", raw_amount_in, raw_amount_out, protocol_fee,
                    &self.divisors.usdt, &self.divisors.apt, "📉", "USDT->APT"
                ).await;
            },
            _ => {
                debug!("🔄 Unsupported Thala swap pair: {} -> {}", swap_data.from_token, swap_data.to_token);
            }
        }
    }

    // Unified swap processing function to eliminate code duplication
    async fn process_swap_pair(
        &self,
        pool_entry: &mut PoolVolume,
        from_currency: &str,
        to_currency: &str,
        raw_amount_in: &BigDecimal,
        raw_amount_out: &BigDecimal,
        protocol_fee: &BigDecimal,
        from_divisor: &BigDecimal,
        to_divisor: &BigDecimal,
        emoji: &str,
        swap_type: &str,
    ) {
        // Calculate normalized amounts
        let from_amount = raw_amount_in / from_divisor;
        let to_amount = raw_amount_out / to_divisor;
        let fee_amount = protocol_fee / from_divisor;
        let net_volume = &from_amount - &fee_amount;

        // Update total volumes and fees based on currency types (for backward compatibility)
        match from_currency {
            "APT" => {
                pool_entry.apt_volume_24h += net_volume.clone();
                pool_entry.apt_fee_24h += fee_amount.clone();
            },
            "USDC" => {
                pool_entry.usdc_volume_24h += net_volume.clone();
                pool_entry.usdc_fee_24h += fee_amount.clone();
            },
            "USDT" => {
                pool_entry.usdt_volume_24h += net_volume.clone();
                pool_entry.usdt_fee_24h += fee_amount.clone();
            },
            _ => debug!("Unknown from_currency: {}", from_currency),
        }

        match to_currency {
            "APT" => pool_entry.apt_volume_24h += to_amount.clone(),
            "USDC" => pool_entry.usdc_volume_24h += to_amount.clone(),
            "USDT" => pool_entry.usdt_volume_24h += to_amount.clone(),
            _ => debug!("Unknown to_currency: {}", to_currency),
        }

        // Update buy/sell volumes based on actual transaction direction
        // from_currency is being sold, to_currency is being bought
        match from_currency {
            "APT" => pool_entry.apt_sell_volume_24h += net_volume.clone(),
            "USDC" => pool_entry.usdc_sell_volume_24h += net_volume.clone(),
            "USDT" => pool_entry.usdt_sell_volume_24h += net_volume.clone(),
            _ => debug!("Unknown from_currency for sell: {}", from_currency),
        }

        match to_currency {
            "APT" => pool_entry.apt_buy_volume_24h += to_amount.clone(),
            "USDC" => pool_entry.usdc_buy_volume_24h += to_amount.clone(),
            "USDT" => pool_entry.usdt_buy_volume_24h += to_amount.clone(),
            _ => debug!("Unknown to_currency for buy: {}", to_currency),
        }

        info!("{} Thala {}: {} {} sold (net: {}), {} {} bought, {} {} fee", 
            emoji, swap_type, from_amount, from_currency, net_volume, to_amount, to_currency, fee_amount, from_currency);
    }

    #[inline]
    pub fn is_target_pool(&self, pool_address: &str) -> bool {
        TARGET_POOLS.contains(&pool_address)
    }
} 