// @generated automatically by Diesel CLI.

diesel::table! {
    apt_data (pool) {
        pool -> Varchar,
        total_apt_usdc_volume_24h -> Nullable<Numeric>,
        inserted_at -> Timestamp,
    }
}

diesel::table! {
    processor_status (processor) {
        processor -> Varchar,
        last_success_version -> Int8,
        last_updated -> Timestamp,
        last_transaction_timestamp -> Nullable<Timestamp>,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    apt_data,
    processor_status,
);
