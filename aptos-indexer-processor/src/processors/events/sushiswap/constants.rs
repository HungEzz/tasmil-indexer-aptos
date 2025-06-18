// SushiSwap constants
pub const SUSHISWAP_SWAP_EVENT_TYPE: &str = "0x31a6675cbe84365bf2b0cbce617ece6c47023ef70826533bde5203d32171dc3c::swap::SwapEvent";

// SushiSwap coin types (different from Cellana/Thala)
pub const APT_COIN_TYPE: &str = "0x1::aptos_coin::AptosCoin";
pub const IZUSDT_COIN_TYPE: &str = "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDT";
pub const IZUSDC_COIN_TYPE: &str = "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDC";
pub const WHUSDC_COIN_TYPE: &str = "0x5e156f1207d0ebfa19a9eeff00d62a282278fb8719f4fab3a586a0a2c0fffbea::coin::T";
pub const IZWETH_COIN_TYPE: &str = "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::WETH";

// Decimal places
pub const APT_DECIMALS: u8 = 8;
pub const USDT_DECIMALS: u8 = 6;
pub const USDC_DECIMALS: u8 = 6;
pub const WETH_DECIMALS: u8 = 6;

// SushiSwap divisors for efficient calculations (avoid floating point)
pub const APT_DIVISOR: u64 = 100_000_000; // 10^8
pub const USDT_DIVISOR: u64 = 1_000_000;  // 10^6
pub const WETH_DIVISOR: u64 = 1_000_000; // 10^6 