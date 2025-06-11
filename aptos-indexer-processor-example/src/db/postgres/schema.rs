// @generated automatically by Diesel CLI.

diesel::table! {
    apt_data (pool) {
        pool -> Varchar,
        inserted_at -> Timestamp,
        apt_volume_24h -> Nullable<Numeric>,
        usdc_volume_24h -> Nullable<Numeric>,
        apt_fee_24h -> Nullable<Numeric>,
        usdc_fee_24h -> Nullable<Numeric>,
        usdt_volume_24h -> Nullable<Numeric>,
        usdt_fee_24h -> Nullable<Numeric>,
    }
}

diesel::table! {
    backfill_processor_status (backfill_alias) {
        #[max_length = 50]
        backfill_alias -> Varchar,
        #[max_length = 50]
        backfill_status -> Varchar,
        last_success_version -> Int8,
        last_updated -> Timestamp,
        last_transaction_timestamp -> Nullable<Timestamp>,
        backfill_start_version -> Int8,
        backfill_end_version -> Int8,
    }
}

diesel::table! {
    cetus_add_liquidity_events (id) {
        id -> Varchar,
        liquidity -> Varchar,
        after_liquidity -> Varchar,
    }
}

diesel::table! {
    cetus_remove_liquidity_events (id) {
        id -> Varchar,
        liquidity -> Varchar,
        after_liquidity -> Varchar,
    }
}

diesel::table! {
    cetus_swap_events (id) {
        id -> Varchar,
        amount_in -> Int8,
        amount_out -> Int8,
        pool -> Varchar,
        pool_id -> Varchar,
        atob -> Bool,
        timestamp -> Timestamptz,
    }
}

diesel::table! {
    daily_statistics (id) {
        id -> Int4,
        date -> Date,
        daily_volume_usd -> Numeric,
        daily_tvl_usd -> Numeric,
        daily_fees_usd -> Numeric,
        swap_count -> Int4,
        liquidity_events_count -> Int4,
        avg_price_sui_usd -> Nullable<Numeric>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    dapp_rankings (package_id) {
        rank_position -> Int4,
        package_id -> Varchar,
        dapp_name -> Varchar,
        dau_1h -> Int4,
        dapp_type -> Varchar,
        last_update -> Nullable<Timestamp>,
    }
}

diesel::table! {
    events (transaction_version, event_index) {
        sequence_number -> Int8,
        creation_number -> Int8,
        #[max_length = 66]
        account_address -> Varchar,
        transaction_version -> Int8,
        transaction_block_height -> Int8,
        #[sql_name = "type"]
        type_ -> Text,
        data -> Jsonb,
        inserted_at -> Timestamp,
        event_index -> Int8,
        #[max_length = 300]
        indexed_type -> Varchar,
    }
}

diesel::table! {
    hourly_statistics (id) {
        id -> Int4,
        hour_timestamp -> Timestamp,
        hourly_volume_usd -> Numeric,
        hourly_tvl_usd -> Numeric,
        hourly_fees_usd -> Numeric,
        swap_count -> Int4,
        avg_price_sui_usd -> Nullable<Numeric>,
        created_at -> Timestamp,
    }
}

diesel::table! {
    ledger_infos (chain_id) {
        chain_id -> Int8,
    }
}

diesel::table! {
    liquidity_events (id) {
        id -> Int4,
        #[max_length = 255]
        pool_id -> Varchar,
        amount_a -> Numeric,
        amount_b -> Numeric,
        timestamp -> Timestamp,
        #[max_length = 255]
        transaction_digest -> Varchar,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    apt_data,
    backfill_processor_status,
    cetus_add_liquidity_events,
    cetus_remove_liquidity_events,
    cetus_swap_events,
    daily_statistics,
    dapp_rankings,
    events,
    hourly_statistics,
    ledger_infos,
    liquidity_events,
);
