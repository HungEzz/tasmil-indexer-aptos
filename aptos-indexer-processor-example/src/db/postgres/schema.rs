// @generated automatically by Diesel CLI.

diesel::table! {
    apt_data (protocol_name) {
        protocol_name -> Varchar,
        inserted_at -> Timestamp,
        apt_volume_24h -> Nullable<Numeric>,
        usdc_volume_24h -> Nullable<Numeric>,
        apt_fee_24h -> Nullable<Numeric>,
        usdc_fee_24h -> Nullable<Numeric>,
        usdt_volume_24h -> Nullable<Numeric>,
        usdt_fee_24h -> Nullable<Numeric>,
        weth_volume_24h -> Nullable<Numeric>,
        weth_fee_24h -> Nullable<Numeric>,
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
    coin_volume_24h (coin) {
        coin -> Varchar,
        buy_volume -> Nullable<Numeric>,
        sell_volume -> Nullable<Numeric>,
        inserted_at -> Timestamp,
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
    ledger_infos (chain_id) {
        chain_id -> Int8,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    apt_data,
    backfill_processor_status,
    coin_volume_24h,
    events,
    ledger_infos,
);
