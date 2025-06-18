// LiquidSwap constants for volume calculation

// LiquidSwap swap event type
pub const LIQUIDSWAP_SWAP_EVENT_TYPE: &str = "0x190d44266241744264b964a37b8f09863167a12d3e70cda39376cfb4e3561e12::liquidity_pool::SwapEvent";

// Coin types (reuse from existing constants)
pub const APT_COIN_TYPE: &str = "0x1::aptos_coin::AptosCoin";
pub const IZUSDC_COIN_TYPE: &str = "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDC";
pub const IZUSDT_COIN_TYPE: &str = "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDT";
pub const WHUSDC_COIN_TYPE: &str = "0x5e156f1207d0ebfa19a9eeff00d62a282278fb8719f4fab3a586a0a2c0fffbea::coin::T";
pub const WHUSDT_COIN_TYPE: &str = "0x1f9e145308ba2fbd4737c6a08204087f29f5d6bb7d76969cdd79d5fc95e0ae3::coin::T";
pub const IZWETH_COIN_TYPE: &str = "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::WETH";
pub const WHWETH_COIN_TYPE: &str = "0xcc8a89c8dce9693d354449f1f73e60e14e347417854f029db5bc8e7454008abb::coin::T";

// Decimal places for each token
pub const APT_DECIMALS: u8 = 8;
pub const USDC_DECIMALS: u8 = 6;
pub const USDT_DECIMALS: u8 = 6;
pub const WETH_DECIMALS: u8 = 6; 