use super::constants::{
    APT_COIN_TYPE,
    IZUSDC_COIN_TYPE,
    IZUSDT_COIN_TYPE,
    WHUSDC_COIN_TYPE,
    WHUSDT_COIN_TYPE,
    IZWETH_COIN_TYPE,
    WHWETH_COIN_TYPE,
    APT_DECIMALS,
    USDC_DECIMALS,
    USDT_DECIMALS,
    WETH_DECIMALS,
};
use anyhow::Result;
use bigdecimal::{BigDecimal, Zero, FromPrimitive};
use serde_json;
use std::{collections::HashMap, str::FromStr};
use tracing::{info, debug};

#[derive(Debug)]
pub struct LiquidSwapData {
    pub x_in: String,
    pub x_out: String,
    pub y_in: String,
    pub y_out: String,
    pub token_x: String,
    pub token_y: String,
}

#[derive(Debug)]
pub struct LiquidPoolVolume {
    pub pair: String,
    pub apt_volume_24h: BigDecimal,
    pub usdc_volume_24h: BigDecimal,
    pub usdt_volume_24h: BigDecimal,
    pub weth_volume_24h: BigDecimal,
}

// Cached decimal divisors for performance
struct LiquidDecimalDivisors {
    apt: BigDecimal,
    usdc: BigDecimal,
    usdt: BigDecimal,
    weth: BigDecimal,
}

impl LiquidDecimalDivisors {
    fn new() -> Self {
        Self {
            apt: BigDecimal::from_u64(10_u64.pow(APT_DECIMALS as u32)).unwrap(),
            usdc: BigDecimal::from_u64(10_u64.pow(USDC_DECIMALS as u32)).unwrap(),
            usdt: BigDecimal::from_u64(10_u64.pow(USDT_DECIMALS as u32)).unwrap(),
            weth: BigDecimal::from_u64(10_u64.pow(WETH_DECIMALS as u32)).unwrap(),
        }
    }
}

impl Default for LiquidPoolVolume {
    fn default() -> Self {
        Self {
            pair: String::new(),
            apt_volume_24h: BigDecimal::from(0),
            usdc_volume_24h: BigDecimal::from(0),
            usdt_volume_24h: BigDecimal::from(0),
            weth_volume_24h: BigDecimal::from(0),
        }
    }
}

pub struct LiquidSwapProcessor {
    divisors: LiquidDecimalDivisors,
}

impl LiquidSwapProcessor {
    pub fn new() -> Self {
        Self {
            divisors: LiquidDecimalDivisors::new(),
        }
    }

    /// Extract token types from LiquidSwap event type_str
    /// Example: "0x190d44266241744264b964a37b8f09863167a12d3e70cda39376cfb4e3561e12::liquidity_pool::SwapEvent<0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDC, 0x1::aptos_coin::AptosCoin, 0x190d44266241744264b964a37b8f09863167a12d3e70cda39376cfb4e3561e12::curves::Uncorrelated>"
    pub fn extract_token_types_from_type_str(&self, type_str: &str) -> Option<(String, String)> {
        debug!("🔍 Extracting token types from LiquidSwap type_str: {}", type_str);
        
        // Find the generic parameters between < and >
        if let Some(start) = type_str.find('<') {
            if let Some(end) = type_str.rfind('>') {
                let generic_part = &type_str[start + 1..end];
                
                // Split by comma and clean up - take only first 2 tokens (ignore the curve type)
                let tokens: Vec<&str> = generic_part.split(',').map(|s| s.trim()).collect();
                if tokens.len() >= 2 {
                    let token_x = tokens[0].to_string();
                    let token_y = tokens[1].to_string();
                    debug!("✅ Extracted tokens: X={}, Y={}", token_x, token_y);
                    return Some((token_x, token_y));
                }
            }
        }
        
        debug!("❌ Failed to extract token types from type_str");
        None
    }

    pub fn extract_liquidswap_data(&self, event_data: &serde_json::Value, type_str: &str) -> Result<LiquidSwapData> {
        debug!("🔍 Extracting LiquidSwap swap data from event");
        
        let x_in = event_data
            .get("x_in")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing x_in"))?;
            
        let x_out = event_data
            .get("x_out")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing x_out"))?;
            
        let y_in = event_data
            .get("y_in")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing y_in"))?;
            
        let y_out = event_data
            .get("y_out")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing y_out"))?;

        // Extract token types from type_str
        let (token_x, token_y) = self.extract_token_types_from_type_str(type_str)
            .ok_or_else(|| anyhow::anyhow!("Failed to extract token types from type_str"))?;

        debug!("✅ Extracted LiquidSwap data: x_in={}, x_out={}, y_in={}, y_out={}, token_x={}, token_y={}", 
            x_in, x_out, y_in, y_out, token_x, token_y);

        Ok(LiquidSwapData {
            x_in: x_in.to_string(),
            x_out: x_out.to_string(),
            y_in: y_in.to_string(),
            y_out: y_out.to_string(),
            token_x,
            token_y,
        })
    }

    pub fn is_supported_pair(&self, token_x: &str, token_y: &str) -> bool {
        // Check if this is APT/izUSDC pair (in either order)
        let is_apt_izusdc = (token_x == APT_COIN_TYPE && token_y == IZUSDC_COIN_TYPE) ||
                           (token_x == IZUSDC_COIN_TYPE && token_y == APT_COIN_TYPE);
        
        // Check if this is APT/izUSDT pair (in either order)
        let is_apt_izusdt = (token_x == APT_COIN_TYPE && token_y == IZUSDT_COIN_TYPE) ||
                           (token_x == IZUSDT_COIN_TYPE && token_y == APT_COIN_TYPE);
        
        // Check if this is APT/whUSDT pair (in either order)
        let is_apt_whusdt = (token_x == APT_COIN_TYPE && token_y == WHUSDT_COIN_TYPE) ||
                           (token_x == WHUSDT_COIN_TYPE && token_y == APT_COIN_TYPE);
        
        // Check if this is APT/izWETH pair (in either order)
        let is_apt_izweth = (token_x == APT_COIN_TYPE && token_y == IZWETH_COIN_TYPE) ||
                           (token_x == IZWETH_COIN_TYPE && token_y == APT_COIN_TYPE);
        
        // Check if this is APT/whWETH pair (in either order)
        let is_apt_whweth = (token_x == APT_COIN_TYPE && token_y == WHWETH_COIN_TYPE) ||
                           (token_x == WHWETH_COIN_TYPE && token_y == APT_COIN_TYPE);
        
        // Check if this is whUSDC/izUSDC pair (in either order)
        let is_whusdc_izusdc = (token_x == WHUSDC_COIN_TYPE && token_y == IZUSDC_COIN_TYPE) ||
                              (token_x == IZUSDC_COIN_TYPE && token_y == WHUSDC_COIN_TYPE);
        
        // Check if this is izUSDT/whUSDT pair (in either order)
        let is_izusdt_whusdt = (token_x == IZUSDT_COIN_TYPE && token_y == WHUSDT_COIN_TYPE) ||
                              (token_x == WHUSDT_COIN_TYPE && token_y == IZUSDT_COIN_TYPE);
        
        is_apt_izusdc || is_apt_izusdt || is_apt_whusdt || is_apt_izweth || is_apt_whweth || is_whusdc_izusdc || is_izusdt_whusdt
    }

    pub async fn process_liquidswap(&self, pool_volumes: &mut HashMap<String, LiquidPoolVolume>, swap_data: LiquidSwapData) {
        // Only process supported pairs
        if !self.is_supported_pair(&swap_data.token_x, &swap_data.token_y) {
            debug!("🚫 Unsupported pair: {} / {}", swap_data.token_x, swap_data.token_y);
            return;
        }

        // Create a unique identifier for the pair (always in consistent order)
        let pair_key = if (swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == IZUSDC_COIN_TYPE) ||
                          (swap_data.token_x == IZUSDC_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE) {
            "APT/USDC".to_string()
        } else if (swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == IZUSDT_COIN_TYPE) ||
                  (swap_data.token_x == IZUSDT_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE) {
            "APT/USDT".to_string()
        } else if (swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == WHUSDT_COIN_TYPE) ||
                  (swap_data.token_x == WHUSDT_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE) {
            "APT/USDT".to_string()  // whUSDT stored as USDT in database
        } else if (swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == IZWETH_COIN_TYPE) ||
                  (swap_data.token_x == IZWETH_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE) {
            "APT/WETH".to_string()  // izWETH stored as WETH in database
        } else if (swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == WHWETH_COIN_TYPE) ||
                  (swap_data.token_x == WHWETH_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE) {
            "APT/WETH".to_string()  // whWETH stored as WETH in database
        } else if (swap_data.token_x == WHUSDC_COIN_TYPE && swap_data.token_y == IZUSDC_COIN_TYPE) ||
                  (swap_data.token_x == IZUSDC_COIN_TYPE && swap_data.token_y == WHUSDC_COIN_TYPE) {
            "USDC/USDC".to_string()  // Both stored as USDC in database
        } else if (swap_data.token_x == IZUSDT_COIN_TYPE && swap_data.token_y == WHUSDT_COIN_TYPE) ||
                  (swap_data.token_x == WHUSDT_COIN_TYPE && swap_data.token_y == IZUSDT_COIN_TYPE) {
            "USDT/USDT".to_string()  // Both stored as USDT in database
        } else {
            return; // Should not happen due to is_supported_pair check
        };

        // Get or create pool entry
        let pool_entry = pool_volumes.entry(pair_key.clone()).or_insert_with(|| {
            let mut volume = LiquidPoolVolume::default();
            volume.pair = pair_key.clone();
            volume
        });

        // Parse amounts
        let x_in = BigDecimal::from_str(&swap_data.x_in).unwrap_or_else(|_| BigDecimal::zero());
        let x_out = BigDecimal::from_str(&swap_data.x_out).unwrap_or_else(|_| BigDecimal::zero());
        let y_in = BigDecimal::from_str(&swap_data.y_in).unwrap_or_else(|_| BigDecimal::zero());
        let y_out = BigDecimal::from_str(&swap_data.y_out).unwrap_or_else(|_| BigDecimal::zero());

        // Process based on token order and swap direction
        if swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == IZUSDC_COIN_TYPE {
            // APT is token_x, izUSDC is token_y
            self.process_apt_izusdc_liquidswap(pool_entry, &x_in, &x_out, &y_in, &y_out).await;
        } else if swap_data.token_x == IZUSDC_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE {
            // izUSDC is token_x, APT is token_y
            self.process_izusdc_apt_liquidswap(pool_entry, &x_in, &x_out, &y_in, &y_out).await;
        } else if swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == IZUSDT_COIN_TYPE {
            // APT is token_x, izUSDT is token_y
            self.process_apt_izusdt_liquidswap(pool_entry, &x_in, &x_out, &y_in, &y_out).await;
        } else if swap_data.token_x == IZUSDT_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE {
            // izUSDT is token_x, APT is token_y
            self.process_izusdt_apt_liquidswap(pool_entry, &x_in, &x_out, &y_in, &y_out).await;
        } else if swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == WHUSDT_COIN_TYPE {
            // APT is token_x, whUSDT is token_y
            self.process_apt_whusdt_liquidswap(pool_entry, &x_in, &x_out, &y_in, &y_out).await;
        } else if swap_data.token_x == WHUSDT_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE {
            // whUSDT is token_x, APT is token_y
            self.process_whusdt_apt_liquidswap(pool_entry, &x_in, &x_out, &y_in, &y_out).await;
        } else if swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == IZWETH_COIN_TYPE {
            // APT is token_x, izWETH is token_y
            self.process_apt_izweth_liquidswap(pool_entry, &x_in, &x_out, &y_in, &y_out).await;
        } else if swap_data.token_x == IZWETH_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE {
            // izWETH is token_x, APT is token_y
            self.process_izweth_apt_liquidswap(pool_entry, &x_in, &x_out, &y_in, &y_out).await;
        } else if swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == WHWETH_COIN_TYPE {
            // APT is token_x, whWETH is token_y
            self.process_apt_whweth_liquidswap(pool_entry, &x_in, &x_out, &y_in, &y_out).await;
        } else if swap_data.token_x == WHWETH_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE {
            // whWETH is token_x, APT is token_y
            self.process_whweth_apt_liquidswap(pool_entry, &x_in, &x_out, &y_in, &y_out).await;
        } else if swap_data.token_x == WHUSDC_COIN_TYPE && swap_data.token_y == IZUSDC_COIN_TYPE {
            // whUSDC is token_x, izUSDC is token_y
            self.process_whusdc_izusdc_liquidswap(pool_entry, &x_in, &x_out, &y_in, &y_out).await;
        } else if swap_data.token_x == IZUSDC_COIN_TYPE && swap_data.token_y == WHUSDC_COIN_TYPE {
            // izUSDC is token_x, whUSDC is token_y
            self.process_izusdc_whusdc_liquidswap(pool_entry, &x_in, &x_out, &y_in, &y_out).await;
        } else if swap_data.token_x == IZUSDT_COIN_TYPE && swap_data.token_y == WHUSDT_COIN_TYPE {
            // izUSDT is token_x, whUSDT is token_y
            self.process_izusdt_whusdt_liquidswap(pool_entry, &x_in, &x_out, &y_in, &y_out).await;
        } else if swap_data.token_x == WHUSDT_COIN_TYPE && swap_data.token_y == IZUSDT_COIN_TYPE {
            // whUSDT is token_x, izUSDT is token_y
            self.process_whusdt_izusdt_liquidswap(pool_entry, &x_in, &x_out, &y_in, &y_out).await;
        }

        info!("📊 LiquidSwap {} volume updated: APT={}, USDC={}, USDT={}, WETH={}", 
            pool_entry.pair, pool_entry.apt_volume_24h, pool_entry.usdc_volume_24h, pool_entry.usdt_volume_24h, pool_entry.weth_volume_24h);
    }

    async fn process_apt_izusdc_liquidswap(
        &self,
        pool_entry: &mut LiquidPoolVolume,
        x_in: &BigDecimal,
        x_out: &BigDecimal,
        y_in: &BigDecimal,
        y_out: &BigDecimal,
    ) {
        // APT is token_x, izUSDC is token_y
        // x_in, x_out represent APT amounts
        // y_in, y_out represent izUSDC amounts
        
        if x_in > &BigDecimal::zero() && y_out > &BigDecimal::zero() {
            // Selling APT for izUSDC: APT in, izUSDC out
            let apt_volume = x_in / &self.divisors.apt;
            let usdc_volume = y_out / &self.divisors.usdc;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.usdc_volume_24h += &usdc_volume;
            
            info!("💱 LiquidSwap APT→izUSDC: Sold {} APT, received {} izUSDC", apt_volume, usdc_volume);
        } else if y_in > &BigDecimal::zero() && x_out > &BigDecimal::zero() {
            // Selling izUSDC for APT: izUSDC in, APT out
            let usdc_volume = y_in / &self.divisors.usdc;
            let apt_volume = x_out / &self.divisors.apt;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.usdc_volume_24h += &usdc_volume;
            
            info!("💱 LiquidSwap izUSDC→APT: Sold {} izUSDC, received {} APT", usdc_volume, apt_volume);
        }
    }

    async fn process_izusdc_apt_liquidswap(
        &self,
        pool_entry: &mut LiquidPoolVolume,
        x_in: &BigDecimal,
        x_out: &BigDecimal,
        y_in: &BigDecimal,
        y_out: &BigDecimal,
    ) {
        // izUSDC is token_x, APT is token_y
        // x_in, x_out represent izUSDC amounts
        // y_in, y_out represent APT amounts
        
        if x_in > &BigDecimal::zero() && y_out > &BigDecimal::zero() {
            // Selling izUSDC for APT: izUSDC in, APT out
            let usdc_volume = x_in / &self.divisors.usdc;
            let apt_volume = y_out / &self.divisors.apt;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.usdc_volume_24h += &usdc_volume;
            
            info!("💱 LiquidSwap izUSDC→APT: Sold {} izUSDC, received {} APT", usdc_volume, apt_volume);
        } else if y_in > &BigDecimal::zero() && x_out > &BigDecimal::zero() {
            // Selling APT for izUSDC: APT in, izUSDC out
            let apt_volume = y_in / &self.divisors.apt;
            let usdc_volume = x_out / &self.divisors.usdc;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.usdc_volume_24h += &usdc_volume;
            
            info!("💱 LiquidSwap APT→izUSDC: Sold {} APT, received {} izUSDC", apt_volume, usdc_volume);
        }
    }

    async fn process_apt_izusdt_liquidswap(
        &self,
        pool_entry: &mut LiquidPoolVolume,
        x_in: &BigDecimal,
        x_out: &BigDecimal,
        y_in: &BigDecimal,
        y_out: &BigDecimal,
    ) {
        // APT is token_x, izUSDT is token_y
        // x_in, x_out represent APT amounts
        // y_in, y_out represent izUSDT amounts
        
        if x_in > &BigDecimal::zero() && y_out > &BigDecimal::zero() {
            // Selling APT for izUSDT: APT in, izUSDT out
            let apt_volume = x_in / &self.divisors.apt;
            let usdt_volume = y_out / &self.divisors.usdt;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.usdt_volume_24h += &usdt_volume;
            
            info!("💱 LiquidSwap APT→izUSDT: Sold {} APT, received {} izUSDT", apt_volume, usdt_volume);
        } else if y_in > &BigDecimal::zero() && x_out > &BigDecimal::zero() {
            // Selling izUSDT for APT: izUSDT in, APT out
            let usdt_volume = y_in / &self.divisors.usdt;
            let apt_volume = x_out / &self.divisors.apt;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.usdt_volume_24h += &usdt_volume;
            
            info!("💱 LiquidSwap izUSDT→APT: Sold {} izUSDT, received {} APT", usdt_volume, apt_volume);
        }
    }

    async fn process_izusdt_apt_liquidswap(
        &self,
        pool_entry: &mut LiquidPoolVolume,
        x_in: &BigDecimal,
        x_out: &BigDecimal,
        y_in: &BigDecimal,
        y_out: &BigDecimal,
    ) {
        // izUSDT is token_x, APT is token_y
        // x_in, x_out represent izUSDT amounts
        // y_in, y_out represent APT amounts
        
        if x_in > &BigDecimal::zero() && y_out > &BigDecimal::zero() {
            // Selling izUSDT for APT: izUSDT in, APT out
            let usdt_volume = x_in / &self.divisors.usdt;
            let apt_volume = y_out / &self.divisors.apt;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.usdt_volume_24h += &usdt_volume;
            
            info!("💱 LiquidSwap izUSDT→APT: Sold {} izUSDT, received {} APT", usdt_volume, apt_volume);
        } else if y_in > &BigDecimal::zero() && x_out > &BigDecimal::zero() {
            // Selling APT for izUSDT: APT in, izUSDT out
            let apt_volume = y_in / &self.divisors.apt;
            let usdt_volume = x_out / &self.divisors.usdt;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.usdt_volume_24h += &usdt_volume;
            
            info!("💱 LiquidSwap APT→izUSDT: Sold {} APT, received {} izUSDT", apt_volume, usdt_volume);
        }
    }

    async fn process_apt_whusdt_liquidswap(
        &self,
        pool_entry: &mut LiquidPoolVolume,
        x_in: &BigDecimal,
        x_out: &BigDecimal,
        y_in: &BigDecimal,
        y_out: &BigDecimal,
    ) {
        // APT is token_x, whUSDT is token_y
        // x_in, x_out represent APT amounts
        // y_in, y_out represent whUSDT amounts
        // whUSDT is stored as USDT in database
        
        if x_in > &BigDecimal::zero() && y_out > &BigDecimal::zero() {
            // Selling APT for whUSDT: APT in, whUSDT out
            let apt_volume = x_in / &self.divisors.apt;
            let usdt_volume = y_out / &self.divisors.usdt;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.usdt_volume_24h += &usdt_volume;
            
            info!("💱 LiquidSwap APT→whUSDT: Sold {} APT, received {} whUSDT", apt_volume, usdt_volume);
        } else if y_in > &BigDecimal::zero() && x_out > &BigDecimal::zero() {
            // Selling whUSDT for APT: whUSDT in, APT out
            let usdt_volume = y_in / &self.divisors.usdt;
            let apt_volume = x_out / &self.divisors.apt;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.usdt_volume_24h += &usdt_volume;
            
            info!("💱 LiquidSwap whUSDT→APT: Sold {} whUSDT, received {} APT", usdt_volume, apt_volume);
        }
    }

    async fn process_whusdt_apt_liquidswap(
        &self,
        pool_entry: &mut LiquidPoolVolume,
        x_in: &BigDecimal,
        x_out: &BigDecimal,
        y_in: &BigDecimal,
        y_out: &BigDecimal,
    ) {
        // whUSDT is token_x, APT is token_y
        // x_in, x_out represent whUSDT amounts
        // y_in, y_out represent APT amounts
        // whUSDT is stored as USDT in database
        
        if x_in > &BigDecimal::zero() && y_out > &BigDecimal::zero() {
            // Selling whUSDT for APT: whUSDT in, APT out
            let usdt_volume = x_in / &self.divisors.usdt;
            let apt_volume = y_out / &self.divisors.apt;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.usdt_volume_24h += &usdt_volume;
            
            info!("💱 LiquidSwap whUSDT→APT: Sold {} whUSDT, received {} APT", usdt_volume, apt_volume);
        } else if y_in > &BigDecimal::zero() && x_out > &BigDecimal::zero() {
            // Selling APT for whUSDT: APT in, whUSDT out
            let apt_volume = y_in / &self.divisors.apt;
            let usdt_volume = x_out / &self.divisors.usdt;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.usdt_volume_24h += &usdt_volume;
            
            info!("💱 LiquidSwap APT→whUSDT: Sold {} APT, received {} whUSDT", apt_volume, usdt_volume);
        }
    }

    async fn process_whusdc_izusdc_liquidswap(
        &self,
        pool_entry: &mut LiquidPoolVolume,
        x_in: &BigDecimal,
        x_out: &BigDecimal,
        y_in: &BigDecimal,
        y_out: &BigDecimal,
    ) {
        // whUSDC is token_x, izUSDC is token_y
        // x_in, x_out represent whUSDC amounts
        // y_in, y_out represent izUSDC amounts
        // Both are stored as USDC in database
        
        if x_in > &BigDecimal::zero() && y_out > &BigDecimal::zero() {
            // Selling whUSDC for izUSDC: whUSDC in, izUSDC out
            let whusdc_volume = x_in / &self.divisors.usdc;
            let izusdc_volume = y_out / &self.divisors.usdc;
            
            // Both volumes are added to usdc_volume_24h since both are USDC variants
            pool_entry.usdc_volume_24h += &whusdc_volume;
            pool_entry.usdc_volume_24h += &izusdc_volume;
            
            info!("💱 LiquidSwap whUSDC→izUSDC: Sold {} whUSDC, received {} izUSDC", whusdc_volume, izusdc_volume);
        } else if y_in > &BigDecimal::zero() && x_out > &BigDecimal::zero() {
            // Selling izUSDC for whUSDC: izUSDC in, whUSDC out
            let izusdc_volume = y_in / &self.divisors.usdc;
            let whusdc_volume = x_out / &self.divisors.usdc;
            
            // Both volumes are added to usdc_volume_24h since both are USDC variants
            pool_entry.usdc_volume_24h += &izusdc_volume;
            pool_entry.usdc_volume_24h += &whusdc_volume;
            
            info!("💱 LiquidSwap izUSDC→whUSDC: Sold {} izUSDC, received {} whUSDC", izusdc_volume, whusdc_volume);
        }
    }

    async fn process_izusdc_whusdc_liquidswap(
        &self,
        pool_entry: &mut LiquidPoolVolume,
        x_in: &BigDecimal,
        x_out: &BigDecimal,
        y_in: &BigDecimal,
        y_out: &BigDecimal,
    ) {
        // izUSDC is token_x, whUSDC is token_y
        // x_in, x_out represent izUSDC amounts
        // y_in, y_out represent whUSDC amounts
        // Both are stored as USDC in database
        
        if x_in > &BigDecimal::zero() && y_out > &BigDecimal::zero() {
            // Selling izUSDC for whUSDC: izUSDC in, whUSDC out
            let izusdc_volume = x_in / &self.divisors.usdc;
            let whusdc_volume = y_out / &self.divisors.usdc;
            
            // Both volumes are added to usdc_volume_24h since both are USDC variants
            pool_entry.usdc_volume_24h += &izusdc_volume;
            pool_entry.usdc_volume_24h += &whusdc_volume;
            
            info!("💱 LiquidSwap izUSDC→whUSDC: Sold {} izUSDC, received {} whUSDC", izusdc_volume, whusdc_volume);
        } else if y_in > &BigDecimal::zero() && x_out > &BigDecimal::zero() {
            // Selling whUSDC for izUSDC: whUSDC in, izUSDC out
            let whusdc_volume = y_in / &self.divisors.usdc;
            let izusdc_volume = x_out / &self.divisors.usdc;
            
            // Both volumes are added to usdc_volume_24h since both are USDC variants
            pool_entry.usdc_volume_24h += &whusdc_volume;
            pool_entry.usdc_volume_24h += &izusdc_volume;
            
            info!("💱 LiquidSwap whUSDC→izUSDC: Sold {} whUSDC, received {} izUSDC", whusdc_volume, izusdc_volume);
        }
    }

    async fn process_izusdt_whusdt_liquidswap(
        &self,
        pool_entry: &mut LiquidPoolVolume,
        x_in: &BigDecimal,
        x_out: &BigDecimal,
        y_in: &BigDecimal,
        y_out: &BigDecimal,
    ) {
        // izUSDT is token_x, whUSDT is token_y
        // x_in, x_out represent izUSDT amounts
        // y_in, y_out represent whUSDT amounts
        // Both are stored as USDT in database
        
        if x_in > &BigDecimal::zero() && y_out > &BigDecimal::zero() {
            // Selling izUSDT for whUSDT: izUSDT in, whUSDT out
            let izusdt_volume = x_in / &self.divisors.usdt;
            let whusdt_volume = y_out / &self.divisors.usdt;
            
            // Both volumes are added to usdt_volume_24h since both are USDT variants
            pool_entry.usdt_volume_24h += &izusdt_volume;
            pool_entry.usdt_volume_24h += &whusdt_volume;
            
            info!("💱 LiquidSwap izUSDT→whUSDT: Sold {} izUSDT, received {} whUSDT", izusdt_volume, whusdt_volume);
        } else if y_in > &BigDecimal::zero() && x_out > &BigDecimal::zero() {
            // Selling whUSDT for izUSDT: whUSDT in, izUSDT out
            let whusdt_volume = y_in / &self.divisors.usdt;
            let izusdt_volume = x_out / &self.divisors.usdt;
            
            // Both volumes are added to usdt_volume_24h since both are USDT variants
            pool_entry.usdt_volume_24h += &whusdt_volume;
            pool_entry.usdt_volume_24h += &izusdt_volume;
            
            info!("💱 LiquidSwap whUSDT→izUSDT: Sold {} whUSDT, received {} izUSDT", whusdt_volume, izusdt_volume);
        }
    }

    async fn process_whusdt_izusdt_liquidswap(
        &self,
        pool_entry: &mut LiquidPoolVolume,
        x_in: &BigDecimal,
        x_out: &BigDecimal,
        y_in: &BigDecimal,
        y_out: &BigDecimal,
    ) {
        // whUSDT is token_x, izUSDT is token_y
        // x_in, x_out represent whUSDT amounts
        // y_in, y_out represent izUSDT amounts
        // Both are stored as USDT in database
        
        if x_in > &BigDecimal::zero() && y_out > &BigDecimal::zero() {
            // Selling whUSDT for izUSDT: whUSDT in, izUSDT out
            let whusdt_volume = x_in / &self.divisors.usdt;
            let izusdt_volume = y_out / &self.divisors.usdt;
            
            // Both volumes are added to usdt_volume_24h since both are USDT variants
            pool_entry.usdt_volume_24h += &whusdt_volume;
            pool_entry.usdt_volume_24h += &izusdt_volume;
            
            info!("💱 LiquidSwap whUSDT→izUSDT: Sold {} whUSDT, received {} izUSDT", whusdt_volume, izusdt_volume);
        } else if y_in > &BigDecimal::zero() && x_out > &BigDecimal::zero() {
            // Selling izUSDT for whUSDT: izUSDT in, whUSDT out
            let izusdt_volume = y_in / &self.divisors.usdt;
            let whusdt_volume = x_out / &self.divisors.usdt;
            
            // Both volumes are added to usdt_volume_24h since both are USDT variants
            pool_entry.usdt_volume_24h += &izusdt_volume;
            pool_entry.usdt_volume_24h += &whusdt_volume;
            
            info!("💱 LiquidSwap izUSDT→whUSDT: Sold {} izUSDT, received {} whUSDT", izusdt_volume, whusdt_volume);
        }
    }

    async fn process_apt_izweth_liquidswap(
        &self,
        pool_entry: &mut LiquidPoolVolume,
        x_in: &BigDecimal,
        x_out: &BigDecimal,
        y_in: &BigDecimal,
        y_out: &BigDecimal,
    ) {
        // APT is token_x, izWETH is token_y
        // x_in, x_out represent APT amounts
        // y_in, y_out represent izWETH amounts
        // izWETH is stored as WETH in database
        
        if x_in > &BigDecimal::zero() && y_out > &BigDecimal::zero() {
            // Selling APT for izWETH: APT in, izWETH out
            let apt_volume = x_in / &self.divisors.apt;
            let weth_volume = y_out / &self.divisors.weth;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.weth_volume_24h += &weth_volume;
            
            info!("💱 LiquidSwap APT→izWETH: Sold {} APT, received {} izWETH", apt_volume, weth_volume);
        } else if y_in > &BigDecimal::zero() && x_out > &BigDecimal::zero() {
            // Selling izWETH for APT: izWETH in, APT out
            let weth_volume = y_in / &self.divisors.weth;
            let apt_volume = x_out / &self.divisors.apt;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.weth_volume_24h += &weth_volume;
            
            info!("💱 LiquidSwap izWETH→APT: Sold {} izWETH, received {} APT", weth_volume, apt_volume);
        }
    }

    async fn process_izweth_apt_liquidswap(
        &self,
        pool_entry: &mut LiquidPoolVolume,
        x_in: &BigDecimal,
        x_out: &BigDecimal,
        y_in: &BigDecimal,
        y_out: &BigDecimal,
    ) {
        // izWETH is token_x, APT is token_y
        // x_in, x_out represent izWETH amounts
        // y_in, y_out represent APT amounts
        // izWETH is stored as WETH in database
        
        if x_in > &BigDecimal::zero() && y_out > &BigDecimal::zero() {
            // Selling izWETH for APT: izWETH in, APT out
            let weth_volume = x_in / &self.divisors.weth;
            let apt_volume = y_out / &self.divisors.apt;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.weth_volume_24h += &weth_volume;
            
            info!("💱 LiquidSwap izWETH→APT: Sold {} izWETH, received {} APT", weth_volume, apt_volume);
        } else if y_in > &BigDecimal::zero() && x_out > &BigDecimal::zero() {
            // Selling APT for izWETH: APT in, izWETH out
            let apt_volume = y_in / &self.divisors.apt;
            let weth_volume = x_out / &self.divisors.weth;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.weth_volume_24h += &weth_volume;
            
            info!("💱 LiquidSwap APT→izWETH: Sold {} APT, received {} izWETH", apt_volume, weth_volume);
        }
    }

    async fn process_apt_whweth_liquidswap(
        &self,
        pool_entry: &mut LiquidPoolVolume,
        x_in: &BigDecimal,
        x_out: &BigDecimal,
        y_in: &BigDecimal,
        y_out: &BigDecimal,
    ) {
        // APT is token_x, whWETH is token_y
        // x_in, x_out represent APT amounts
        // y_in, y_out represent whWETH amounts
        // whWETH is stored as WETH in database
        
        if x_in > &BigDecimal::zero() && y_out > &BigDecimal::zero() {
            // Selling APT for whWETH: APT in, whWETH out
            let apt_volume = x_in / &self.divisors.apt;
            let weth_volume = y_out / &self.divisors.weth;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.weth_volume_24h += &weth_volume;
            
            info!("💱 LiquidSwap APT→whWETH: Sold {} APT, received {} whWETH", apt_volume, weth_volume);
        } else if y_in > &BigDecimal::zero() && x_out > &BigDecimal::zero() {
            // Selling whWETH for APT: whWETH in, APT out
            let weth_volume = y_in / &self.divisors.weth;
            let apt_volume = x_out / &self.divisors.apt;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.weth_volume_24h += &weth_volume;
            
            info!("💱 LiquidSwap whWETH→APT: Sold {} whWETH, received {} APT", weth_volume, apt_volume);
        }
    }

    async fn process_whweth_apt_liquidswap(
        &self,
        pool_entry: &mut LiquidPoolVolume,
        x_in: &BigDecimal,
        x_out: &BigDecimal,
        y_in: &BigDecimal,
        y_out: &BigDecimal,
    ) {
        // whWETH is token_x, APT is token_y
        // x_in, x_out represent whWETH amounts
        // y_in, y_out represent APT amounts
        // whWETH is stored as WETH in database
        
        if x_in > &BigDecimal::zero() && y_out > &BigDecimal::zero() {
            // Selling whWETH for APT: whWETH in, APT out
            let weth_volume = x_in / &self.divisors.weth;
            let apt_volume = y_out / &self.divisors.apt;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.weth_volume_24h += &weth_volume;
            
            info!("💱 LiquidSwap whWETH→APT: Sold {} whWETH, received {} APT", weth_volume, apt_volume);
        } else if y_in > &BigDecimal::zero() && x_out > &BigDecimal::zero() {
            // Selling APT for whWETH: APT in, whWETH out
            let apt_volume = y_in / &self.divisors.apt;
            let weth_volume = x_out / &self.divisors.weth;
            
            pool_entry.apt_volume_24h += &apt_volume;
            pool_entry.weth_volume_24h += &weth_volume;
            
            info!("💱 LiquidSwap APT→whWETH: Sold {} APT, received {} whWETH", apt_volume, weth_volume);
        }
    }

    pub fn is_liquidswap_event(&self, type_str: &str) -> bool {
        type_str.contains("190d44266241744264b964a37b8f09863167a12d3e70cda39376cfb4e3561e12::liquidity_pool::SwapEvent")
    }
} 