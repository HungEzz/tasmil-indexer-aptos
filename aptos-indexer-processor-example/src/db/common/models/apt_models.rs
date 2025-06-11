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
    pub pool: String,
    pub total_apt_usdc_volume_24h: Option<BigDecimal>,
    pub inserted_at: NaiveDateTime,
}

#[derive(Debug, Deserialize, Serialize, Insertable, Clone)]
#[diesel(table_name = apt_data)]
pub struct NewAptData {
    pub pool: String,
    pub total_apt_usdc_volume_24h: Option<BigDecimal>,
}

// Prevent conflicts with other things named `AptData`
pub type AptDataModel = AptData; 