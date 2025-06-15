use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::db::postgres::schema::coin_volume_24h;

#[derive(Debug, Deserialize, Serialize, Clone, Queryable, Insertable)]
#[diesel(table_name = coin_volume_24h)]
pub struct CoinVolume24h {
    pub coin: String,
    pub buy_volume: Option<BigDecimal>,
    pub sell_volume: Option<BigDecimal>,
    pub inserted_at: NaiveDateTime,
}

#[derive(Debug, Deserialize, Serialize, Clone, Insertable)]
#[diesel(table_name = coin_volume_24h)]
pub struct NewCoinVolume24h {
    pub coin: String,
    pub buy_volume: Option<BigDecimal>,
    pub sell_volume: Option<BigDecimal>,
} 