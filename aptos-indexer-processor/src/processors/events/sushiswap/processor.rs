use super::constants::{
    SUSHISWAP_SWAP_EVENT_TYPE,
    APT_COIN_TYPE,
    IZUSDT_COIN_TYPE,  // izUSDT but tracked as USDT in database
    IZUSDC_COIN_TYPE,  // izUSDC but tracked as USDC in database
    WHUSDC_COIN_TYPE,  // whUSDC but tracked as USDC in database
    IZWETH_COIN_TYPE,  // izWETH but tracked as WETH in database
    APT_DECIMALS,
    USDT_DECIMALS,
    USDC_DECIMALS,
    WETH_DECIMALS,
};
use anyhow::Result;
use bigdecimal::{BigDecimal, Zero, FromPrimitive};
use serde_json;
use std::{collections::HashMap, str::FromStr};
use tracing::{info, debug};

#[derive(Debug)]
pub struct SushiSwapData {
    pub amount_x_in: String,
    pub amount_x_out: String,
    pub amount_y_in: String,
    pub amount_y_out: String,
    pub token_x: String,
    pub token_y: String,
    pub user: String,
}

#[derive(Debug)]
pub struct SushiPoolVolume {
    pub pair: String,
    pub apt_volume_24h: BigDecimal,
    pub usdt_volume_24h: BigDecimal,
    pub usdc_volume_24h: BigDecimal,  // Add USDC support
    pub weth_volume_24h: BigDecimal,  // Add WETH support
    // Buy/Sell volume tracking
    pub apt_buy_volume_24h: BigDecimal,
    pub apt_sell_volume_24h: BigDecimal,
    pub usdt_buy_volume_24h: BigDecimal,
    pub usdt_sell_volume_24h: BigDecimal,
    pub usdc_buy_volume_24h: BigDecimal,
    pub usdc_sell_volume_24h: BigDecimal,
    pub weth_buy_volume_24h: BigDecimal,
    pub weth_sell_volume_24h: BigDecimal,
    // Note: SushiSwap doesn't have fees, so no fee fields
}

// Cached decimal divisors for performance
struct SushiDecimalDivisors {
    apt: BigDecimal,
    usdt: BigDecimal,
    usdc: BigDecimal,  // Add USDC divisor
    weth: BigDecimal,  // Add WETH divisor
}

impl SushiDecimalDivisors {
    fn new() -> Self {
        Self {
            apt: BigDecimal::from_u64(10_u64.pow(APT_DECIMALS as u32)).unwrap(),
            usdt: BigDecimal::from_u64(10_u64.pow(USDT_DECIMALS as u32)).unwrap(),
            usdc: BigDecimal::from_u64(10_u64.pow(USDC_DECIMALS as u32)).unwrap(),
            weth: BigDecimal::from_u64(10_u64.pow(WETH_DECIMALS as u32)).unwrap(),
        }
    }
}

impl Default for SushiPoolVolume {
    fn default() -> Self {
        Self {
            pair: String::new(),
            apt_volume_24h: BigDecimal::from(0),
            usdt_volume_24h: BigDecimal::from(0),
            usdc_volume_24h: BigDecimal::from(0),
            weth_volume_24h: BigDecimal::from(0),
            apt_buy_volume_24h: BigDecimal::from(0),
            apt_sell_volume_24h: BigDecimal::from(0),
            usdt_buy_volume_24h: BigDecimal::from(0),
            usdt_sell_volume_24h: BigDecimal::from(0),
            usdc_buy_volume_24h: BigDecimal::from(0),
            usdc_sell_volume_24h: BigDecimal::from(0),
            weth_buy_volume_24h: BigDecimal::from(0),
            weth_sell_volume_24h: BigDecimal::from(0),
        }
    }
}

pub struct SushiSwapProcessor {
    divisors: SushiDecimalDivisors,
}

impl SushiSwapProcessor {
    pub fn new() -> Self {
        Self {
            divisors: SushiDecimalDivisors::new(),
        }
    }

    /// Extract token types from SushiSwap event type_str
    /// Example: "0x31a6675cbe84365bf2b0cbce617ece6c47023ef70826533bde5203d32171dc3c::swap::SwapEvent<0x1::aptos_coin::AptosCoin, 0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDT>"
    pub fn extract_token_types_from_type_str(&self, type_str: &str) -> Option<(String, String)> {
        debug!("🔍 Extracting token types from SushiSwap type_str: {}", type_str);
        
        // Find the generic parameters between < and >
        if let Some(start) = type_str.find('<') {
            if let Some(end) = type_str.rfind('>') {
                let generic_part = &type_str[start + 1..end];
                
                // Split by comma and clean up
                let tokens: Vec<&str> = generic_part.split(',').map(|s| s.trim()).collect();
                if tokens.len() == 2 {
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

    pub fn extract_sushiswap_data(&self, event_data: &serde_json::Value, type_str: &str) -> Result<SushiSwapData> {
        debug!("🔍 Extracting SushiSwap swap data from event");
        
        let amount_x_in = event_data
            .get("amount_x_in")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing amount_x_in"))?;
            
        let amount_x_out = event_data
            .get("amount_x_out")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing amount_x_out"))?;
            
        let amount_y_in = event_data
            .get("amount_y_in")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing amount_y_in"))?;
            
        let amount_y_out = event_data
            .get("amount_y_out")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing amount_y_out"))?;

        let user = event_data
            .get("user")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing user"))?;

        // Extract token types from type_str
        let (token_x, token_y) = self.extract_token_types_from_type_str(type_str)
            .ok_or_else(|| anyhow::anyhow!("Failed to extract token types from type_str"))?;

        debug!("✅ Extracted SushiSwap data: x_in={}, x_out={}, y_in={}, y_out={}, token_x={}, token_y={}", 
            amount_x_in, amount_x_out, amount_y_in, amount_y_out, token_x, token_y);

        Ok(SushiSwapData {
            amount_x_in: amount_x_in.to_string(),
            amount_x_out: amount_x_out.to_string(),
            amount_y_in: amount_y_in.to_string(),
            amount_y_out: amount_y_out.to_string(),
            token_x,
            token_y,
            user: user.to_string(),
        })
    }

    pub fn is_supported_pair(&self, token_x: &str, token_y: &str) -> bool {
        // Check if this is APT/izUSDT pair (in either order)
        let is_apt_izusdt = (token_x == APT_COIN_TYPE && token_y == IZUSDT_COIN_TYPE) ||
                           (token_x == IZUSDT_COIN_TYPE && token_y == APT_COIN_TYPE);
        
        // Check if this is APT/izUSDC pair (in either order)
        let is_apt_izusdc = (token_x == APT_COIN_TYPE && token_y == IZUSDC_COIN_TYPE) ||
                           (token_x == IZUSDC_COIN_TYPE && token_y == APT_COIN_TYPE);
        
        // Check if this is APT/whUSDC pair (in either order)
        let is_apt_whusdc = (token_x == APT_COIN_TYPE && token_y == WHUSDC_COIN_TYPE) ||
                           (token_x == WHUSDC_COIN_TYPE && token_y == APT_COIN_TYPE);
        
        // Check if this is APT/izWETH pair (in either order)
        let is_apt_izweth = (token_x == APT_COIN_TYPE && token_y == IZWETH_COIN_TYPE) ||
                           (token_x == IZWETH_COIN_TYPE && token_y == APT_COIN_TYPE);
        
        // Check if this is izWETH/izUSDC pair (in either order)
        let is_izweth_izusdc = (token_x == IZWETH_COIN_TYPE && token_y == IZUSDC_COIN_TYPE) ||
                              (token_x == IZUSDC_COIN_TYPE && token_y == IZWETH_COIN_TYPE);
        
        // Check if this is whUSDC/izUSDC pair (in either order)
        let is_whusdc_izusdc = (token_x == WHUSDC_COIN_TYPE && token_y == IZUSDC_COIN_TYPE) ||
                              (token_x == IZUSDC_COIN_TYPE && token_y == WHUSDC_COIN_TYPE);
        
        is_apt_izusdt || is_apt_izusdc || is_apt_whusdc || is_apt_izweth || is_izweth_izusdc || is_whusdc_izusdc
    }

    pub async fn process_sushiswap(&self, pool_volumes: &mut HashMap<String, SushiPoolVolume>, swap_data: SushiSwapData) {
        // Only process supported pairs
        if !self.is_supported_pair(&swap_data.token_x, &swap_data.token_y) {
            debug!("🚫 Unsupported pair: {} / {}", swap_data.token_x, swap_data.token_y);
            return;
        }

        // Create a unique identifier for the pair (always in consistent order)
        let pair_key = if (swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == IZUSDT_COIN_TYPE) ||
                          (swap_data.token_x == IZUSDT_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE) {
            "APT/USDT".to_string()
        } else if (swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == IZUSDC_COIN_TYPE) ||
                  (swap_data.token_x == IZUSDC_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE) {
            "APT/USDC".to_string()
        } else if (swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == WHUSDC_COIN_TYPE) ||
                  (swap_data.token_x == WHUSDC_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE) {
            "APT/USDC".to_string()  // whUSDC also tracked as USDC
        } else if (swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == IZWETH_COIN_TYPE) ||
                  (swap_data.token_x == IZWETH_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE) {
            "APT/WETH".to_string()  // izWETH also tracked as WETH
        } else if (swap_data.token_x == IZWETH_COIN_TYPE && swap_data.token_y == IZUSDC_COIN_TYPE) ||
                  (swap_data.token_x == IZUSDC_COIN_TYPE && swap_data.token_y == IZWETH_COIN_TYPE) {
            "WETH/USDC".to_string()  // izWETH/izUSDC pair
        } else if (swap_data.token_x == WHUSDC_COIN_TYPE && swap_data.token_y == IZUSDC_COIN_TYPE) ||
                  (swap_data.token_x == IZUSDC_COIN_TYPE && swap_data.token_y == WHUSDC_COIN_TYPE) {
            "whUSDC/izUSDC".to_string()  // whUSDC/izUSDC pair - both stored as USDC
        } else {
            return; // Shouldn't happen due to is_supported_pair check
        };

        // Get or create pool volume entry
        let pool_entry = pool_volumes.entry(pair_key.clone()).or_insert_with(|| {
            SushiPoolVolume {
                pair: pair_key.clone(),
                ..Default::default()
            }
        });

        // Parse amounts
        let amount_x_in = BigDecimal::from_str(&swap_data.amount_x_in).unwrap_or_else(|_| BigDecimal::zero());
        let amount_x_out = BigDecimal::from_str(&swap_data.amount_x_out).unwrap_or_else(|_| BigDecimal::zero());
        let amount_y_in = BigDecimal::from_str(&swap_data.amount_y_in).unwrap_or_else(|_| BigDecimal::zero());
        let amount_y_out = BigDecimal::from_str(&swap_data.amount_y_out).unwrap_or_else(|_| BigDecimal::zero());

        // Determine swap direction and process volume
        if swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == IZUSDT_COIN_TYPE {
            // Token X = APT, Token Y = izUSDT (but track as USDT in database)
            self.process_apt_izusdt_sushiswap(pool_entry, &amount_x_in, &amount_x_out, &amount_y_in, &amount_y_out).await;
        } else if swap_data.token_x == IZUSDT_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE {
            // Token X = izUSDT, Token Y = APT (swapped order)
            self.process_izusdt_apt_sushiswap(pool_entry, &amount_x_in, &amount_x_out, &amount_y_in, &amount_y_out).await;
        } else if swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == IZUSDC_COIN_TYPE {
            // Token X = APT, Token Y = izUSDC (but track as USDC in database)
            self.process_apt_izusdc_sushiswap(pool_entry, &amount_x_in, &amount_x_out, &amount_y_in, &amount_y_out).await;
        } else if swap_data.token_x == IZUSDC_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE {
            // Token X = izUSDC, Token Y = APT (swapped order)
            self.process_izusdc_apt_sushiswap(pool_entry, &amount_x_in, &amount_x_out, &amount_y_in, &amount_y_out).await;
        } else if swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == WHUSDC_COIN_TYPE {
            // Token X = APT, Token Y = whUSDC (but track as USDC in database)
            self.process_apt_whusdc_sushiswap(pool_entry, &amount_x_in, &amount_x_out, &amount_y_in, &amount_y_out).await;
        } else if swap_data.token_x == WHUSDC_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE {
            // Token X = whUSDC, Token Y = APT (swapped order)
            self.process_whusdc_apt_sushiswap(pool_entry, &amount_x_in, &amount_x_out, &amount_y_in, &amount_y_out).await;
        } else if swap_data.token_x == APT_COIN_TYPE && swap_data.token_y == IZWETH_COIN_TYPE {
            // Token X = APT, Token Y = izWETH (but track as WETH in database)
            self.process_apt_izweth_sushiswap(pool_entry, &amount_x_in, &amount_x_out, &amount_y_in, &amount_y_out).await;
        } else if swap_data.token_x == IZWETH_COIN_TYPE && swap_data.token_y == APT_COIN_TYPE {
            // Token X = izWETH, Token Y = APT (swapped order)
            self.process_izweth_apt_sushiswap(pool_entry, &amount_x_in, &amount_x_out, &amount_y_in, &amount_y_out).await;
        } else if swap_data.token_x == IZWETH_COIN_TYPE && swap_data.token_y == IZUSDC_COIN_TYPE {
            // Token X = izWETH, Token Y = izUSDC
            self.process_izweth_izusdc_sushiswap(pool_entry, &amount_x_in, &amount_x_out, &amount_y_in, &amount_y_out).await;
        } else if swap_data.token_x == IZUSDC_COIN_TYPE && swap_data.token_y == IZWETH_COIN_TYPE {
            // Token X = izUSDC, Token Y = izWETH (swapped order)
            self.process_izusdc_izweth_sushiswap(pool_entry, &amount_x_in, &amount_x_out, &amount_y_in, &amount_y_out).await;
        } else if swap_data.token_x == WHUSDC_COIN_TYPE && swap_data.token_y == IZUSDC_COIN_TYPE {
            // Token X = whUSDC, Token Y = izUSDC (both stored as USDC)
            self.process_whusdc_izusdc_sushiswap(pool_entry, &amount_x_in, &amount_x_out, &amount_y_in, &amount_y_out).await;
        } else if swap_data.token_x == IZUSDC_COIN_TYPE && swap_data.token_y == WHUSDC_COIN_TYPE {
            // Token X = izUSDC, Token Y = whUSDC (swapped order, both stored as USDC)
            self.process_izusdc_whusdc_sushiswap(pool_entry, &amount_x_in, &amount_x_out, &amount_y_in, &amount_y_out).await;
        }
    }

    /// Process APT/izUSDT swap where Token X = APT, Token Y = izUSDT
    async fn process_apt_izusdt_sushiswap(
        &self,
        pool_entry: &mut SushiPoolVolume,
        amount_x_in: &BigDecimal,
        amount_x_out: &BigDecimal,
        amount_y_in: &BigDecimal,
        amount_y_out: &BigDecimal,
    ) {
        // Determine swap direction based on non-zero amounts
        if amount_x_in > &BigDecimal::zero() && amount_y_out > &BigDecimal::zero() {
            // APT → izUSDT: User sells APT (X) and receives izUSDT (Y)
            let apt_amount = amount_x_in / &self.divisors.apt;
            let izusdt_amount = amount_y_out / &self.divisors.usdt;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.apt_volume_24h += &apt_amount;
            pool_entry.usdt_volume_24h += &izusdt_amount;  // Save izUSDT as USDT volume
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.apt_sell_volume_24h += &apt_amount;  // APT is being sold
            pool_entry.usdt_buy_volume_24h += &izusdt_amount;  // USDT is being bought
            
            info!("📉 SushiSwap APT→izUSDT: {} APT sold, {} izUSDT received", 
                apt_amount, izusdt_amount);
                
        } else if amount_y_in > &BigDecimal::zero() && amount_x_out > &BigDecimal::zero() {
            // izUSDT → APT: User sells izUSDT (Y) and receives APT (X)
            let izusdt_amount = amount_y_in / &self.divisors.usdt;
            let apt_amount = amount_x_out / &self.divisors.apt;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.usdt_volume_24h += &izusdt_amount;  // Save izUSDT as USDT volume
            pool_entry.apt_volume_24h += &apt_amount;
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.usdt_sell_volume_24h += &izusdt_amount;  // USDT is being sold
            pool_entry.apt_buy_volume_24h += &apt_amount;  // APT is being bought
            
            info!("📈 SushiSwap izUSDT→APT: {} izUSDT sold, {} APT received", 
                izusdt_amount, apt_amount);
        }
    }

    /// Process izUSDT/APT swap where Token X = izUSDT, Token Y = APT
    async fn process_izusdt_apt_sushiswap(
        &self,
        pool_entry: &mut SushiPoolVolume,
        amount_x_in: &BigDecimal,
        amount_x_out: &BigDecimal,
        amount_y_in: &BigDecimal,
        amount_y_out: &BigDecimal,
    ) {
        // Determine swap direction based on non-zero amounts
        if amount_x_in > &BigDecimal::zero() && amount_y_out > &BigDecimal::zero() {
            // izUSDT → APT: User sells izUSDT (X) and receives APT (Y)
            let izusdt_amount = amount_x_in / &self.divisors.usdt;
            let apt_amount = amount_y_out / &self.divisors.apt;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.usdt_volume_24h += &izusdt_amount;  // Save izUSDT as USDT volume
            pool_entry.apt_volume_24h += &apt_amount;
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.usdt_sell_volume_24h += &izusdt_amount;  // USDT is being sold
            pool_entry.apt_buy_volume_24h += &apt_amount;  // APT is being bought
            
            info!("📈 SushiSwap izUSDT→APT: {} izUSDT sold, {} APT received", 
                izusdt_amount, apt_amount);
                
        } else if amount_y_in > &BigDecimal::zero() && amount_x_out > &BigDecimal::zero() {
            // APT → izUSDT: User sells APT (Y) and receives izUSDT (X)
            let apt_amount = amount_y_in / &self.divisors.apt;
            let izusdt_amount = amount_x_out / &self.divisors.usdt;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.apt_volume_24h += &apt_amount;
            pool_entry.usdt_volume_24h += &izusdt_amount;  // Save izUSDT as USDT volume
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.apt_sell_volume_24h += &apt_amount;  // APT is being sold
            pool_entry.usdt_buy_volume_24h += &izusdt_amount;  // USDT is being bought
            
            info!("📉 SushiSwap APT→izUSDT: {} APT sold, {} izUSDT received", 
                apt_amount, izusdt_amount);
        }
    }

    /// Process APT/izUSDC swap where Token X = APT, Token Y = izUSDC
    async fn process_apt_izusdc_sushiswap(
        &self,
        pool_entry: &mut SushiPoolVolume,
        amount_x_in: &BigDecimal,
        amount_x_out: &BigDecimal,
        amount_y_in: &BigDecimal,
        amount_y_out: &BigDecimal,
    ) {
        // Determine swap direction based on non-zero amounts
        if amount_x_in > &BigDecimal::zero() && amount_y_out > &BigDecimal::zero() {
            // APT → izUSDC: User sells APT (X) and receives izUSDC (Y)
            let apt_amount = amount_x_in / &self.divisors.apt;
            let izusdc_amount = amount_y_out / &self.divisors.usdc;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.apt_volume_24h += &apt_amount;
            pool_entry.usdc_volume_24h += &izusdc_amount;  // Save izUSDC as USDC volume
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.apt_sell_volume_24h += &apt_amount;  // APT is being sold
            pool_entry.usdc_buy_volume_24h += &izusdc_amount;  // USDC is being bought
            
            info!("📉 SushiSwap APT→izUSDC: {} APT sold, {} izUSDC received", 
                apt_amount, izusdc_amount);
                
        } else if amount_y_in > &BigDecimal::zero() && amount_x_out > &BigDecimal::zero() {
            // izUSDC → APT: User sells izUSDC (Y) and receives APT (X)
            let izusdc_amount = amount_y_in / &self.divisors.usdc;
            let apt_amount = amount_x_out / &self.divisors.apt;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.usdc_volume_24h += &izusdc_amount;  // Save izUSDC as USDC volume
            pool_entry.apt_volume_24h += &apt_amount;
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.usdc_sell_volume_24h += &izusdc_amount;  // USDC is being sold
            pool_entry.apt_buy_volume_24h += &apt_amount;  // APT is being bought
            
            info!("📈 SushiSwap izUSDC→APT: {} izUSDC sold, {} APT received", 
                izusdc_amount, apt_amount);
        }
    }

    /// Process izUSDC/APT swap where Token X = izUSDC, Token Y = APT
    async fn process_izusdc_apt_sushiswap(
        &self,
        pool_entry: &mut SushiPoolVolume,
        amount_x_in: &BigDecimal,
        amount_x_out: &BigDecimal,
        amount_y_in: &BigDecimal,
        amount_y_out: &BigDecimal,
    ) {
        // Determine swap direction based on non-zero amounts
        if amount_x_in > &BigDecimal::zero() && amount_y_out > &BigDecimal::zero() {
            // izUSDC → APT: User sells izUSDC (X) and receives APT (Y)
            let izusdc_amount = amount_x_in / &self.divisors.usdc;
            let apt_amount = amount_y_out / &self.divisors.apt;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.usdc_volume_24h += &izusdc_amount;  // Save izUSDC as USDC volume
            pool_entry.apt_volume_24h += &apt_amount;
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.usdc_sell_volume_24h += &izusdc_amount;  // USDC is being sold
            pool_entry.apt_buy_volume_24h += &apt_amount;  // APT is being bought
            
            info!("📈 SushiSwap izUSDC→APT: {} izUSDC sold, {} APT received", 
                izusdc_amount, apt_amount);
                
        } else if amount_y_in > &BigDecimal::zero() && amount_x_out > &BigDecimal::zero() {
            // APT → izUSDC: User sells APT (Y) and receives izUSDC (X)
            let apt_amount = amount_y_in / &self.divisors.apt;
            let izusdc_amount = amount_x_out / &self.divisors.usdc;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.apt_volume_24h += &apt_amount;
            pool_entry.usdc_volume_24h += &izusdc_amount;  // Save izUSDC as USDC volume
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.apt_sell_volume_24h += &apt_amount;  // APT is being sold
            pool_entry.usdc_buy_volume_24h += &izusdc_amount;  // USDC is being bought
            
            info!("📉 SushiSwap APT→izUSDC: {} APT sold, {} izUSDC received", 
                apt_amount, izusdc_amount);
        }
    }

    /// Process APT/whUSDC swap where Token X = APT, Token Y = whUSDC
    async fn process_apt_whusdc_sushiswap(
        &self,
        pool_entry: &mut SushiPoolVolume,
        amount_x_in: &BigDecimal,
        amount_x_out: &BigDecimal,
        amount_y_in: &BigDecimal,
        amount_y_out: &BigDecimal,
    ) {
        // Determine swap direction based on non-zero amounts
        if amount_x_in > &BigDecimal::zero() && amount_y_out > &BigDecimal::zero() {
            // APT → whUSDC: User sells APT (X) and receives whUSDC (Y)
            let apt_amount = amount_x_in / &self.divisors.apt;
            let whusdc_amount = amount_y_out / &self.divisors.usdc;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.apt_volume_24h += &apt_amount;
            pool_entry.usdc_volume_24h += &whusdc_amount;  // Save whUSDC as USDC volume
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.apt_sell_volume_24h += &apt_amount;  // APT is being sold
            pool_entry.usdc_buy_volume_24h += &whusdc_amount;  // USDC is being bought
            
            info!("📉 SushiSwap APT→whUSDC: {} APT sold, {} whUSDC received", 
                apt_amount, whusdc_amount);
                
        } else if amount_y_in > &BigDecimal::zero() && amount_x_out > &BigDecimal::zero() {
            // whUSDC → APT: User sells whUSDC (Y) and receives APT (X)
            let whusdc_amount = amount_y_in / &self.divisors.usdc;
            let apt_amount = amount_x_out / &self.divisors.apt;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.usdc_volume_24h += &whusdc_amount;  // Save whUSDC as USDC volume
            pool_entry.apt_volume_24h += &apt_amount;
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.usdc_sell_volume_24h += &whusdc_amount;  // USDC is being sold
            pool_entry.apt_buy_volume_24h += &apt_amount;  // APT is being bought
            
            info!("📈 SushiSwap whUSDC→APT: {} whUSDC sold, {} APT received", 
                whusdc_amount, apt_amount);
        }
    }

    /// Process whUSDC/APT swap where Token X = whUSDC, Token Y = APT
    async fn process_whusdc_apt_sushiswap(
        &self,
        pool_entry: &mut SushiPoolVolume,
        amount_x_in: &BigDecimal,
        amount_x_out: &BigDecimal,
        amount_y_in: &BigDecimal,
        amount_y_out: &BigDecimal,
    ) {
        // Determine swap direction based on non-zero amounts
        if amount_x_in > &BigDecimal::zero() && amount_y_out > &BigDecimal::zero() {
            // whUSDC → APT: User sells whUSDC (X) and receives APT (Y)
            let whusdc_amount = amount_x_in / &self.divisors.usdc;
            let apt_amount = amount_y_out / &self.divisors.apt;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.usdc_volume_24h += &whusdc_amount;  // Save whUSDC as USDC volume
            pool_entry.apt_volume_24h += &apt_amount;
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.usdc_sell_volume_24h += &whusdc_amount;  // USDC is being sold
            pool_entry.apt_buy_volume_24h += &apt_amount;  // APT is being bought
            
            info!("📈 SushiSwap whUSDC→APT: {} whUSDC sold, {} APT received", 
                whusdc_amount, apt_amount);
                
        } else if amount_y_in > &BigDecimal::zero() && amount_x_out > &BigDecimal::zero() {
            // APT → whUSDC: User sells APT (Y) and receives whUSDC (X)
            let apt_amount = amount_y_in / &self.divisors.apt;
            let whusdc_amount = amount_x_out / &self.divisors.usdc;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.apt_volume_24h += &apt_amount;
            pool_entry.usdc_volume_24h += &whusdc_amount;  // Save whUSDC as USDC volume
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.apt_sell_volume_24h += &apt_amount;  // APT is being sold
            pool_entry.usdc_buy_volume_24h += &whusdc_amount;  // USDC is being bought
            
            info!("📉 SushiSwap APT→whUSDC: {} APT sold, {} whUSDC received", 
                apt_amount, whusdc_amount);
        }
    }

    /// Process APT/izWETH swap where Token X = APT, Token Y = izWETH
    async fn process_apt_izweth_sushiswap(
        &self,
        pool_entry: &mut SushiPoolVolume,
        amount_x_in: &BigDecimal,
        amount_x_out: &BigDecimal,
        amount_y_in: &BigDecimal,
        amount_y_out: &BigDecimal,
    ) {
        // Determine swap direction based on non-zero amounts
        if amount_x_in > &BigDecimal::zero() && amount_y_out > &BigDecimal::zero() {
            // APT → izWETH: User sells APT (X) and receives izWETH (Y)
            let apt_amount = amount_x_in / &self.divisors.apt;
            let izweth_amount = amount_y_out / &self.divisors.weth;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.apt_volume_24h += &apt_amount;
            pool_entry.weth_volume_24h += &izweth_amount;  // Save izWETH as WETH volume
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.apt_sell_volume_24h += &apt_amount;  // APT is being sold
            pool_entry.weth_buy_volume_24h += &izweth_amount;  // WETH is being bought
            
            info!("📉 SushiSwap APT→izWETH: {} APT sold, {} izWETH received", 
                apt_amount, izweth_amount);
                
        } else if amount_y_in > &BigDecimal::zero() && amount_x_out > &BigDecimal::zero() {
            // izWETH → APT: User sells izWETH (Y) and receives APT (X)
            let izweth_amount = amount_y_in / &self.divisors.weth;
            let apt_amount = amount_x_out / &self.divisors.apt;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.weth_volume_24h += &izweth_amount;  // Save izWETH as WETH volume
            pool_entry.apt_volume_24h += &apt_amount;
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.weth_sell_volume_24h += &izweth_amount;  // WETH is being sold
            pool_entry.apt_buy_volume_24h += &apt_amount;  // APT is being bought
            
            info!("📈 SushiSwap izWETH→APT: {} izWETH sold, {} APT received", 
                izweth_amount, apt_amount);
        }
    }

    /// Process izWETH/APT swap where Token X = izWETH, Token Y = APT
    async fn process_izweth_apt_sushiswap(
        &self,
        pool_entry: &mut SushiPoolVolume,
        amount_x_in: &BigDecimal,
        amount_x_out: &BigDecimal,
        amount_y_in: &BigDecimal,
        amount_y_out: &BigDecimal,
    ) {
        // Determine swap direction based on non-zero amounts
        if amount_x_in > &BigDecimal::zero() && amount_y_out > &BigDecimal::zero() {
            // izWETH → APT: User sells izWETH (X) and receives APT (Y)
            let izweth_amount = amount_x_in / &self.divisors.weth;
            let apt_amount = amount_y_out / &self.divisors.apt;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.weth_volume_24h += &izweth_amount;  // Save izWETH as WETH volume
            pool_entry.apt_volume_24h += &apt_amount;
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.weth_sell_volume_24h += &izweth_amount;  // WETH is being sold
            pool_entry.apt_buy_volume_24h += &apt_amount;  // APT is being bought
            
            info!("📈 SushiSwap izWETH→APT: {} izWETH sold, {} APT received", 
                izweth_amount, apt_amount);
                
        } else if amount_y_in > &BigDecimal::zero() && amount_x_out > &BigDecimal::zero() {
            // APT → izWETH: User sells APT (Y) and receives izWETH (X)
            let apt_amount = amount_y_in / &self.divisors.apt;
            let izweth_amount = amount_x_out / &self.divisors.weth;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.apt_volume_24h += &apt_amount;
            pool_entry.weth_volume_24h += &izweth_amount;  // Save izWETH as WETH volume
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.apt_sell_volume_24h += &apt_amount;  // APT is being sold
            pool_entry.weth_buy_volume_24h += &izweth_amount;  // WETH is being bought
            
            info!("📉 SushiSwap APT→izWETH: {} APT sold, {} izWETH received", 
                apt_amount, izweth_amount);
        }
    }

    /// Process izWETH/izUSDC swap where Token X = izWETH, Token Y = izUSDC
    async fn process_izweth_izusdc_sushiswap(
        &self,
        pool_entry: &mut SushiPoolVolume,
        amount_x_in: &BigDecimal,
        amount_x_out: &BigDecimal,
        amount_y_in: &BigDecimal,
        amount_y_out: &BigDecimal,
    ) {
        // Determine swap direction based on non-zero amounts
        if amount_x_in > &BigDecimal::zero() && amount_y_out > &BigDecimal::zero() {
            // izWETH → izUSDC: User sells izWETH (X) and receives izUSDC (Y)
            let izweth_amount = amount_x_in / &self.divisors.weth;
            let izusdc_amount = amount_y_out / &self.divisors.usdc;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.weth_volume_24h += &izweth_amount;  // Save izWETH as WETH volume
            pool_entry.usdc_volume_24h += &izusdc_amount;  // Save izUSDC as USDC volume
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.weth_sell_volume_24h += &izweth_amount;  // WETH is being sold
            pool_entry.usdc_buy_volume_24h += &izusdc_amount;  // USDC is being bought
            
            info!("📉 SushiSwap izWETH→izUSDC: {} izWETH sold, {} izUSDC received", 
                izweth_amount, izusdc_amount);
                
        } else if amount_y_in > &BigDecimal::zero() && amount_x_out > &BigDecimal::zero() {
            // izUSDC → izWETH: User sells izUSDC (Y) and receives izWETH (X)
            let izusdc_amount = amount_y_in / &self.divisors.usdc;
            let izweth_amount = amount_x_out / &self.divisors.weth;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.usdc_volume_24h += &izusdc_amount;  // Save izUSDC as USDC volume
            pool_entry.weth_volume_24h += &izweth_amount;  // Save izWETH as WETH volume
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.usdc_sell_volume_24h += &izusdc_amount;  // USDC is being sold
            pool_entry.weth_buy_volume_24h += &izweth_amount;  // WETH is being bought
            
            info!("📈 SushiSwap izUSDC→izWETH: {} izUSDC sold, {} izWETH received", 
                izusdc_amount, izweth_amount);
        }
    }

    /// Process whUSDC/izUSDC swap where Token X = whUSDC, Token Y = izUSDC
    async fn process_whusdc_izusdc_sushiswap(
        &self,
        pool_entry: &mut SushiPoolVolume,
        amount_x_in: &BigDecimal,
        amount_x_out: &BigDecimal,
        amount_y_in: &BigDecimal,
        amount_y_out: &BigDecimal,
    ) {
        // Determine swap direction based on non-zero amounts
        if amount_x_in > &BigDecimal::zero() && amount_y_out > &BigDecimal::zero() {
            // whUSDC → izUSDC: User sells whUSDC (X) and receives izUSDC (Y)
            let whusdc_amount = amount_x_in / &self.divisors.usdc;
            let izusdc_amount = amount_y_out / &self.divisors.usdc;
            
            // Count BOTH tokens as USDC volume since both are USDC variants (for backward compatibility)
            pool_entry.usdc_volume_24h += &whusdc_amount;  // whUSDC as USDC
            pool_entry.usdc_volume_24h += &izusdc_amount;  // izUSDC as USDC
            
            // Update buy/sell volumes - both are USDC variants
            pool_entry.usdc_sell_volume_24h += &whusdc_amount;  // whUSDC is being sold
            pool_entry.usdc_buy_volume_24h += &izusdc_amount;  // izUSDC is being bought
            
            info!("📉 SushiSwap whUSDC→izUSDC: {} whUSDC sold, {} izUSDC received", 
                whusdc_amount, izusdc_amount);
                
        } else if amount_y_in > &BigDecimal::zero() && amount_x_out > &BigDecimal::zero() {
            // izUSDC → whUSDC: User sells izUSDC (Y) and receives whUSDC (X)
            let izusdc_amount = amount_y_in / &self.divisors.usdc;
            let whusdc_amount = amount_x_out / &self.divisors.usdc;
            
            // Count BOTH tokens as USDC volume since both are USDC variants (for backward compatibility)
            pool_entry.usdc_volume_24h += &izusdc_amount;  // izUSDC as USDC
            pool_entry.usdc_volume_24h += &whusdc_amount;  // whUSDC as USDC
            
            // Update buy/sell volumes - both are USDC variants
            pool_entry.usdc_sell_volume_24h += &izusdc_amount;  // izUSDC is being sold
            pool_entry.usdc_buy_volume_24h += &whusdc_amount;  // whUSDC is being bought
            
            info!("📈 SushiSwap izUSDC→whUSDC: {} izUSDC sold, {} whUSDC received", 
                izusdc_amount, whusdc_amount);
        }
    }

    /// Process izUSDC/whUSDC swap where Token X = izUSDC, Token Y = whUSDC
    async fn process_izusdc_whusdc_sushiswap(
        &self,
        pool_entry: &mut SushiPoolVolume,
        amount_x_in: &BigDecimal,
        amount_x_out: &BigDecimal,
        amount_y_in: &BigDecimal,
        amount_y_out: &BigDecimal,
    ) {
        // Determine swap direction based on non-zero amounts
        if amount_x_in > &BigDecimal::zero() && amount_y_out > &BigDecimal::zero() {
            // izUSDC → whUSDC: User sells izUSDC (X) and receives whUSDC (Y)
            let izusdc_amount = amount_x_in / &self.divisors.usdc;
            let whusdc_amount = amount_y_out / &self.divisors.usdc;
            
            // Count BOTH tokens as USDC volume since both are USDC variants (for backward compatibility)
            pool_entry.usdc_volume_24h += &izusdc_amount;  // izUSDC as USDC
            pool_entry.usdc_volume_24h += &whusdc_amount;  // whUSDC as USDC
            
            // Update buy/sell volumes - both are USDC variants
            pool_entry.usdc_sell_volume_24h += &izusdc_amount;  // izUSDC is being sold
            pool_entry.usdc_buy_volume_24h += &whusdc_amount;  // whUSDC is being bought
            
            info!("📉 SushiSwap izUSDC→whUSDC: {} izUSDC sold, {} whUSDC received", 
                izusdc_amount, whusdc_amount);
                
        } else if amount_y_in > &BigDecimal::zero() && amount_x_out > &BigDecimal::zero() {
            // whUSDC → izUSDC: User sells whUSDC (Y) and receives izUSDC (X)
            let whusdc_amount = amount_y_in / &self.divisors.usdc;
            let izusdc_amount = amount_x_out / &self.divisors.usdc;
            
            // Count BOTH tokens as USDC volume since both are USDC variants (for backward compatibility)
            pool_entry.usdc_volume_24h += &whusdc_amount;  // whUSDC as USDC
            pool_entry.usdc_volume_24h += &izusdc_amount;  // izUSDC as USDC
            
            // Update buy/sell volumes - both are USDC variants
            pool_entry.usdc_sell_volume_24h += &whusdc_amount;  // whUSDC is being sold
            pool_entry.usdc_buy_volume_24h += &izusdc_amount;  // izUSDC is being bought
            
            info!("📈 SushiSwap whUSDC→izUSDC: {} whUSDC sold, {} izUSDC received", 
                whusdc_amount, izusdc_amount);
        }
    }

    /// Process izUSDC/izWETH swap where Token X = izUSDC, Token Y = izWETH
    async fn process_izusdc_izweth_sushiswap(
        &self,
        pool_entry: &mut SushiPoolVolume,
        amount_x_in: &BigDecimal,
        amount_x_out: &BigDecimal,
        amount_y_in: &BigDecimal,
        amount_y_out: &BigDecimal,
    ) {
        // Determine swap direction based on non-zero amounts
        if amount_x_in > &BigDecimal::zero() && amount_y_out > &BigDecimal::zero() {
            // izUSDC → izWETH: User sells izUSDC (X) and receives izWETH (Y)
            let izusdc_amount = amount_x_in / &self.divisors.usdc;
            let izweth_amount = amount_y_out / &self.divisors.weth;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.usdc_volume_24h += &izusdc_amount;  // Save izUSDC as USDC volume
            pool_entry.weth_volume_24h += &izweth_amount;  // Save izWETH as WETH volume
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.usdc_sell_volume_24h += &izusdc_amount;  // USDC is being sold
            pool_entry.weth_buy_volume_24h += &izweth_amount;  // WETH is being bought
            
            info!("📉 SushiSwap izUSDC→izWETH: {} izUSDC sold, {} izWETH received", 
                izusdc_amount, izweth_amount);
                
        } else if amount_y_in > &BigDecimal::zero() && amount_x_out > &BigDecimal::zero() {
            // izWETH → izUSDC: User sells izWETH (Y) and receives izUSDC (X)
            let izweth_amount = amount_y_in / &self.divisors.weth;
            let izusdc_amount = amount_x_out / &self.divisors.usdc;
            
            // Save BOTH token volumes like Cellana/Thala (for backward compatibility)
            pool_entry.weth_volume_24h += &izweth_amount;  // Save izWETH as WETH volume
            pool_entry.usdc_volume_24h += &izusdc_amount;  // Save izUSDC as USDC volume
            
            // Update buy/sell volumes based on transaction direction
            pool_entry.weth_sell_volume_24h += &izweth_amount;  // WETH is being sold
            pool_entry.usdc_buy_volume_24h += &izusdc_amount;  // USDC is being bought
            
            info!("📈 SushiSwap izWETH→izUSDC: {} izWETH sold, {} izUSDC received", 
                izweth_amount, izusdc_amount);
        }
    }

    pub fn is_sushiswap_event(&self, type_str: &str) -> bool {
        type_str.contains(SUSHISWAP_SWAP_EVENT_TYPE)
    }
} 