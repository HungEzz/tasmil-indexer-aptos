use super::constants::*;
use anyhow::Result;
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
    pub pool_id: String,
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

// Cached decimal divisors for performance
struct DecimalDivisors {
    apt: BigDecimal,
    usdc: BigDecimal,
    usdt: BigDecimal,
}

impl DecimalDivisors {
    fn new() -> Self {
        Self {
            apt: BigDecimal::from_u64(10_u64.pow(APT_DECIMALS as u32)).unwrap(),
            usdc: BigDecimal::from_u64(10_u64.pow(USDC_DECIMALS as u32)).unwrap(),
            usdt: BigDecimal::from_u64(10_u64.pow(USDT_DECIMALS as u32)).unwrap(),
        }
    }
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

pub struct HyperionProcessor {
    divisors: DecimalDivisors,
}

impl HyperionProcessor {
    pub fn new() -> Self {
        Self {
            divisors: DecimalDivisors::new(),
        }
    }

    pub fn extract_swap_data(&self, event_data: &serde_json::Value) -> Result<SwapData> {
        debug!("🔍 Extracting Hyperion swap data from event");
        
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
            .and_then(|obj| obj.get("inner"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing from_token.inner"))?;
            
        let to_token = event_data
            .get("to_token")
            .and_then(|obj| obj.get("inner"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing to_token.inner"))?;

        let pool_id = event_data
            .get("pool_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing pool_id"))?;

        let protocol_fee_amount = event_data
            .get("protocol_fee_amount")
            .and_then(|v| v.as_str())
            .unwrap_or("0");

        debug!("✅ Extracted Hyperion swap: {} {} -> {} {} (pool: {}, fee: {})", 
            amount_in, from_token, amount_out, to_token, pool_id, protocol_fee_amount);

        Ok(SwapData {
            amount_in: amount_in.to_string(),
            amount_out: amount_out.to_string(),
            from_token: from_token.to_string(),
            to_token: to_token.to_string(),
            pool_id: pool_id.to_string(),
            protocol_fee_amount: protocol_fee_amount.to_string(),
        })
    }

    pub async fn process_swap(&self, pool_volumes: &mut HashMap<String, PoolVolume>, swap_data: SwapData) {
        debug!("🔄 Processing Hyperion swap for pool: {}", swap_data.pool_id);

        // Process all Hyperion swaps (removed target pool filter)
        
        // Get or create pool entry
        let pool_entry = pool_volumes.entry(swap_data.pool_id.clone()).or_insert_with(|| {
            let mut volume = PoolVolume::default();
            volume.pool = swap_data.pool_id.clone();
            volume
        });

        // Parse amounts
        let raw_amount_in = BigDecimal::from_str(&swap_data.amount_in).unwrap_or_else(|_| BigDecimal::zero());
        let raw_amount_out = BigDecimal::from_str(&swap_data.amount_out).unwrap_or_else(|_| BigDecimal::zero());
        let protocol_fee = BigDecimal::from_str(&swap_data.protocol_fee_amount).unwrap_or_else(|_| BigDecimal::zero());

        // Process based on swap direction
        match (swap_data.from_token.as_str(), swap_data.to_token.as_str()) {
            (USDT_COIN_TYPE, USDC_COIN_TYPE) => {
                // USDT -> USDC swap
                let usdt_amount = &raw_amount_in / &self.divisors.usdt;
                let usdc_amount = &raw_amount_out / &self.divisors.usdc;
                let usdt_fee = &protocol_fee / &self.divisors.usdt;

                // Update volumes
                pool_entry.usdt_volume_24h += &usdt_amount;
                pool_entry.usdc_volume_24h += &usdc_amount;
                
                // Update fees (in USDT as this is the input token)
                pool_entry.usdt_fee_24h += &usdt_fee;
                
                // Update buy/sell volumes
                pool_entry.usdt_sell_volume_24h += &usdt_amount;  // USDT is being sold
                pool_entry.usdc_buy_volume_24h += &usdc_amount;   // USDC is being bought

                info!("📈 Hyperion USDT→USDC: {} USDT sold, {} USDC received, fee: {} USDT", 
                    usdt_amount, usdc_amount, usdt_fee);
            },
            (USDC_COIN_TYPE, USDT_COIN_TYPE) => {
                // USDC -> USDT swap
                let usdc_amount = &raw_amount_in / &self.divisors.usdc;
                let usdt_amount = &raw_amount_out / &self.divisors.usdt;
                let usdc_fee = &protocol_fee / &self.divisors.usdc;

                // Update volumes
                pool_entry.usdc_volume_24h += &usdc_amount;
                pool_entry.usdt_volume_24h += &usdt_amount;
                
                // Update fees (in USDC as this is the input token)
                pool_entry.usdc_fee_24h += &usdc_fee;
                
                // Update buy/sell volumes
                pool_entry.usdc_sell_volume_24h += &usdc_amount;  // USDC is being sold
                pool_entry.usdt_buy_volume_24h += &usdt_amount;   // USDT is being bought

                info!("📉 Hyperion USDC→USDT: {} USDC sold, {} USDT received, fee: {} USDC", 
                    usdc_amount, usdt_amount, usdc_fee);
            },
            (APT_COIN_TYPE, USDT_COIN_TYPE) => {
                // APT -> USDT swap
                let apt_amount = &raw_amount_in / &self.divisors.apt;
                let usdt_amount = &raw_amount_out / &self.divisors.usdt;
                let apt_fee = &protocol_fee / &self.divisors.apt;

                // Update volumes
                pool_entry.apt_volume_24h += &apt_amount;
                pool_entry.usdt_volume_24h += &usdt_amount;
                
                // Update fees (in APT as this is the input token)
                pool_entry.apt_fee_24h += &apt_fee;
                
                // Update buy/sell volumes
                pool_entry.apt_sell_volume_24h += &apt_amount;   // APT is being sold
                pool_entry.usdt_buy_volume_24h += &usdt_amount;  // USDT is being bought

                info!("📈 Hyperion APT→USDT: {} APT sold, {} USDT received, fee: {} APT", 
                    apt_amount, usdt_amount, apt_fee);
            },
            (USDT_COIN_TYPE, APT_COIN_TYPE) => {
                // USDT -> APT swap
                let usdt_amount = &raw_amount_in / &self.divisors.usdt;
                let apt_amount = &raw_amount_out / &self.divisors.apt;
                let usdt_fee = &protocol_fee / &self.divisors.usdt;

                // Update volumes
                pool_entry.usdt_volume_24h += &usdt_amount;
                pool_entry.apt_volume_24h += &apt_amount;
                
                // Update fees (in USDT as this is the input token)
                pool_entry.usdt_fee_24h += &usdt_fee;
                
                // Update buy/sell volumes
                pool_entry.usdt_sell_volume_24h += &usdt_amount;  // USDT is being sold
                pool_entry.apt_buy_volume_24h += &apt_amount;     // APT is being bought

                info!("📉 Hyperion USDT→APT: {} USDT sold, {} APT received, fee: {} USDT", 
                    usdt_amount, apt_amount, usdt_fee);
            },
            _ => {
                debug!("🚫 Unsupported token pair: {} -> {}", swap_data.from_token, swap_data.to_token);
                return;
            }
        }

        info!("📊 Hyperion {} volume updated: APT={}, USDC={}, USDT={}, APT_fee={}, USDC_fee={}, USDT_fee={}", 
            pool_entry.pool, pool_entry.apt_volume_24h, pool_entry.usdc_volume_24h, pool_entry.usdt_volume_24h, 
            pool_entry.apt_fee_24h, pool_entry.usdc_fee_24h, pool_entry.usdt_fee_24h);
    }
} 