// Cellana constants
pub const CELLANA_SWAP_EVENT_TYPE: &str = "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::SwapEvent";
pub const CELLANA_LIQUIDITY_POOL_TYPE: &str = "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::LiquidityPool";

// Track APT/USDC, USDT/USDC, APT/USDT, APT/stAPT, and aBTC/APT pools (Cellana)
pub const APT_USDC_POOL_ADDRESS: &str = "0x71c6ae634bd3c36470eb7e7f4fb0912973bb31543dfdb7d7fb6863d886d81d67";
pub const USDT_USDC_POOL_ADDRESS: &str = "0xac21d74053633a030281bd0311361442eb2c4f2f95b19c4599b741c439cff77f";
pub const APT_USDT_POOL_ADDRESS: &str = "0xe8797d7adaf48abd7becddc9c4494f1309d9a83a6ea0c4be708482cbc54e36af";
pub const APT_STAPT_POOL_ADDRESS: &str = "0xf7d4a97f8a82b1454cd69f92b5a5bd5bcad609e44a6cf56377755adcfca5863a";
pub const ABTC_APT_POOL_ADDRESS: &str = "0x97058f574153bd3382414e2e3278d70a795ddf0232b8e0364ce376622495a03d";

// Coin types for Cellana
pub const APT_COIN_TYPE: &str = "0x1::aptos_coin::AptosCoin";
pub const USDC_COIN_TYPE: &str = "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b";
pub const USDT_COIN_TYPE: &str = "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b";
pub const STAPT_COIN_TYPE: &str = "0x111ae3e5bc816a5e63c2da97d0aa3886519e0cd5e4b046659fa35796bd11542a::stapt_token::StakedApt";
pub const ABTC_COIN_TYPE: &str = "0x4e1854f6d332c9525e258fb6e66f84b6af8aba687bbcb832a24768c4e175feec::abtc::ABTC";

// Decimal places
pub const APT_DECIMALS: u8 = 8;
pub const USDC_DECIMALS: u8 = 6;
pub const USDT_DECIMALS: u8 = 6;
pub const STAPT_DECIMALS: u8 = 8;
pub const ABTC_DECIMALS: u8 = 10; 