// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::schema::apt_data;
use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Queryable, Insertable, Clone)]
#[diesel(table_name = apt_data)]
pub struct AptData {
    pub protocol_name: String,
    pub inserted_at: NaiveDateTime,
    pub apt_volume_24h: Option<BigDecimal>,
    pub usdc_volume_24h: Option<BigDecimal>,
    pub apt_fee_24h: Option<BigDecimal>,
    pub usdc_fee_24h: Option<BigDecimal>,
    pub usdt_volume_24h: Option<BigDecimal>,
    pub usdt_fee_24h: Option<BigDecimal>,
    pub stapt_volume_24h: Option<BigDecimal>,
    pub stapt_fee_24h: Option<BigDecimal>,
    pub abtc_volume_24h: Option<BigDecimal>,
    pub abtc_fee_24h: Option<BigDecimal>,
    pub weth_volume_24h: Option<BigDecimal>,
    pub weth_fee_24h: Option<BigDecimal>,
}

#[derive(Debug, Deserialize, Serialize, Insertable, Clone)]
#[diesel(table_name = apt_data)]
pub struct NewAptData {
    pub protocol_name: String,
    pub apt_volume_24h: Option<BigDecimal>,
    pub usdc_volume_24h: Option<BigDecimal>,
    pub apt_fee_24h: Option<BigDecimal>,
    pub usdc_fee_24h: Option<BigDecimal>,
    pub usdt_volume_24h: Option<BigDecimal>,
    pub usdt_fee_24h: Option<BigDecimal>,
    pub stapt_volume_24h: Option<BigDecimal>,
    pub stapt_fee_24h: Option<BigDecimal>,
    pub abtc_volume_24h: Option<BigDecimal>,
    pub abtc_fee_24h: Option<BigDecimal>,
    pub weth_volume_24h: Option<BigDecimal>,
    pub weth_fee_24h: Option<BigDecimal>,
}

// Prevent conflicts with other things named `AptData`
pub type AptDataModel = AptData; 