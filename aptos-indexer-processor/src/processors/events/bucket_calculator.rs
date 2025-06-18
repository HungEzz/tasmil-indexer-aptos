use chrono::{DateTime, Duration, FixedOffset, NaiveDateTime, Timelike};
use bigdecimal::{BigDecimal, Zero};
use std::collections::HashMap;
use tracing::{info, debug};

use crate::db::common::models::coin_volume_models::NewCoinVolumeBucket;

#[derive(Debug, Clone)]
pub struct SwapEventData {
    pub timestamp_seconds: i64,
    pub coin_volumes: Vec<CoinVolumeData>,
}

#[derive(Debug, Clone)]
pub struct CoinVolumeData {
    pub coin: String,
    pub volume: BigDecimal,
}

/// BucketCalculator handles grouping SwapEvents into 2-hour time buckets in GMT+7
pub struct BucketCalculator {
    gmt7_offset: FixedOffset,
}

impl BucketCalculator {
    pub fn new() -> Self {
        Self {
            gmt7_offset: FixedOffset::east_opt(7 * 3600).unwrap(), // GMT+7
        }
    }

    /// Calculate which 2-hour bucket a timestamp falls into
    fn calculate_bucket_range(&self, timestamp_seconds: i64) -> (NaiveDateTime, NaiveDateTime) {
        // Convert to UTC first, then to GMT+7
        let utc_dt = DateTime::from_timestamp(timestamp_seconds, 0)
            .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
        let gmt7_dt = utc_dt.with_timezone(&self.gmt7_offset);
        
        let hour = gmt7_dt.hour();
        
        // Round down to the nearest 2-hour boundary
        let bucket_start_hour = (hour / 2) * 2;
        let bucket_end_hour = bucket_start_hour + 2;
        
        // Create bucket start time (always on the same date as the transaction)
        let bucket_start = gmt7_dt
            .date_naive()
            .and_hms_opt(bucket_start_hour, 0, 0)
            .unwrap();
        
        // Create bucket end time
        let bucket_end = if bucket_end_hour >= 24 {
            // If bucket end goes to next day (22:00-00:00 case)
            let next_day = gmt7_dt.date_naive() + Duration::days(1);
            next_day.and_hms_opt(0, 0, 0).unwrap()
        } else {
            // Same day
            gmt7_dt
                .date_naive()
                .and_hms_opt(bucket_end_hour, 0, 0)
                .unwrap()
        };
        
        (bucket_start, bucket_end)
    }

    /// Check if timestamp is within the last 24 hours
    fn is_within_24h(&self, timestamp_seconds: i64, current_timestamp: i64) -> bool {
        let current_time = DateTime::from_timestamp(current_timestamp, 0)
            .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
        let cutoff_time = current_time - Duration::hours(24);
        let txn_time = DateTime::from_timestamp(timestamp_seconds, 0)
            .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
        
        txn_time >= cutoff_time
    }

    /// Group swap events into 2-hour buckets and aggregate volumes
    pub fn group_swaps_into_buckets(&self, swap_data: Vec<SwapEventData>, current_timestamp: i64) -> Vec<NewCoinVolumeBucket> {
        let mut bucket_volumes: HashMap<(String, NaiveDateTime, NaiveDateTime), BigDecimal> = HashMap::new();
        
        // Process each swap event
        for swap in &swap_data {
            // Only process swaps within the last 24 hours
            if !self.is_within_24h(swap.timestamp_seconds, current_timestamp) {
                continue;
            }
            
            let (bucket_start, bucket_end) = self.calculate_bucket_range(swap.timestamp_seconds);
            
            // Aggregate volumes for each coin in this swap
            for coin_volume in &swap.coin_volumes {
                let key = (coin_volume.coin.clone(), bucket_start, bucket_end);
                let current_volume = bucket_volumes.entry(key).or_insert_with(|| BigDecimal::zero());
                *current_volume += &coin_volume.volume;
                
                debug!("📊 Added volume {} for {} in bucket {} - {}", 
                    &coin_volume.volume, &coin_volume.coin, bucket_start, bucket_end);
            }
        }
        
        // Convert to database records
        let mut bucket_records = Vec::new();
        for ((coin, bucket_start, bucket_end), volume) in bucket_volumes {
            bucket_records.push(NewCoinVolumeBucket {
                coin,
                bucket_start,
                bucket_end,
                volume: Some(volume),
            });
        }
        
        // Sort by coin (ascending) then by bucket_start (ascending)
        bucket_records.sort_by(|a, b| {
            a.coin.cmp(&b.coin)
                .then_with(|| a.bucket_start.cmp(&b.bucket_start))
        });
        
        info!("🪣 Created {} bucket records from {} swap events (sorted by coin, bucket_start)", 
            bucket_records.len(), swap_data.len());
        
        bucket_records
    }

    /// Format bucket for display
    pub fn format_bucket_display(&self, bucket_start: &NaiveDateTime, bucket_end: &NaiveDateTime) -> String {
        format!("[{:02}:00–{:02}:00]", bucket_start.hour(), bucket_end.hour())
    }
}

impl Default for BucketCalculator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use bigdecimal::FromPrimitive;

    #[test]
    fn test_bucket_calculation() {
        let calculator = BucketCalculator::new();
        
        // Test timestamp: 1750080174 = 2025-06-16 20:22:54 GMT+7
        let timestamp = 1750080174;
        let (bucket_start, bucket_end) = calculator.calculate_bucket_range(timestamp);
        
        // Should be in 20:00-22:00 bucket
        assert_eq!(bucket_start.hour(), 20);
        assert_eq!(bucket_end.hour(), 22);
        assert_eq!(bucket_start.minute(), 0);
        assert_eq!(bucket_end.minute(), 0);
    }

    #[test]
    fn test_bucket_display_format() {
        let calculator = BucketCalculator::new();
        let bucket_start = NaiveDate::from_ymd_opt(2025, 6, 16).unwrap()
            .and_hms_opt(20, 0, 0).unwrap();
        let bucket_end = NaiveDate::from_ymd_opt(2025, 6, 16).unwrap()
            .and_hms_opt(22, 0, 0).unwrap();
        
        let display = calculator.format_bucket_display(&bucket_start, &bucket_end);
        assert_eq!(display, "[20:00–22:00]");
    }

    #[test]
    fn test_bucket_times_match_expected_format() {
        let calculator = BucketCalculator::new();
        
        // Create correct timestamps for June 17, 2025 in GMT+7
        use chrono::{NaiveDate, TimeZone};
        
        // June 17, 2025 00:30:00 GMT+7
        let june17_0030_gmt7 = calculator.gmt7_offset.from_local_datetime(
            &NaiveDate::from_ymd_opt(2025, 6, 17).unwrap().and_hms_opt(0, 30, 0).unwrap()
        ).unwrap();
        let timestamp_0030 = june17_0030_gmt7.timestamp();
        
        let (start, end) = calculator.calculate_bucket_range(timestamp_0030);
        println!("June 17 00:30 -> bucket: {} to {}", 
                start.format("%Y-%m-%d %H:%M:%S"), end.format("%Y-%m-%d %H:%M:%S"));
        assert_eq!(start.format("%Y-%m-%d %H:%M:%S").to_string(), "2025-06-17 00:00:00");
        assert_eq!(end.format("%Y-%m-%d %H:%M:%S").to_string(), "2025-06-17 02:00:00");
        
        // June 17, 2025 02:15:00 GMT+7
        let june17_0215_gmt7 = calculator.gmt7_offset.from_local_datetime(
            &NaiveDate::from_ymd_opt(2025, 6, 17).unwrap().and_hms_opt(2, 15, 0).unwrap()
        ).unwrap();
        let timestamp_0215 = june17_0215_gmt7.timestamp();
        
        let (start, end) = calculator.calculate_bucket_range(timestamp_0215);
        println!("June 17 02:15 -> bucket: {} to {}", 
                start.format("%Y-%m-%d %H:%M:%S"), end.format("%Y-%m-%d %H:%M:%S"));
        assert_eq!(start.format("%Y-%m-%d %H:%M:%S").to_string(), "2025-06-17 02:00:00");
        assert_eq!(end.format("%Y-%m-%d %H:%M:%S").to_string(), "2025-06-17 04:00:00");
        
        // June 17, 2025 04:45:00 GMT+7
        let june17_0445_gmt7 = calculator.gmt7_offset.from_local_datetime(
            &NaiveDate::from_ymd_opt(2025, 6, 17).unwrap().and_hms_opt(4, 45, 0).unwrap()
        ).unwrap();
        let timestamp_0445 = june17_0445_gmt7.timestamp();
        
        let (start, end) = calculator.calculate_bucket_range(timestamp_0445);
        println!("June 17 04:45 -> bucket: {} to {}", 
                start.format("%Y-%m-%d %H:%M:%S"), end.format("%Y-%m-%d %H:%M:%S"));
        assert_eq!(start.format("%Y-%m-%d %H:%M:%S").to_string(), "2025-06-17 04:00:00");
        assert_eq!(end.format("%Y-%m-%d %H:%M:%S").to_string(), "2025-06-17 06:00:00");
        
        println!("✅ All bucket times match expected format from image!");
    }

    #[test]
    fn test_volume_aggregation_in_bucket() {
        let calculator = BucketCalculator::new();
        
        // Create multiple swap events for the same timestamp (same bucket)
        let timestamp = 1734336000; // 2024-12-16 10:00:00 UTC
        
        let swap_events = vec![
            SwapEventData {
                timestamp_seconds: timestamp,
                coin_volumes: vec![
                    CoinVolumeData {
                        coin: "APT".to_string(),
                        volume: BigDecimal::from_f64(100.0).unwrap(),
                    },
                    CoinVolumeData {
                        coin: "USDC".to_string(),
                        volume: BigDecimal::from_f64(50.0).unwrap(),
                    },
                ],
            },
            SwapEventData {
                timestamp_seconds: timestamp, // Same timestamp = same bucket
                coin_volumes: vec![
                    CoinVolumeData {
                        coin: "APT".to_string(),
                        volume: BigDecimal::from_f64(75.0).unwrap(), // Should aggregate with first APT
                    },
                    CoinVolumeData {
                        coin: "USDC".to_string(),
                        volume: BigDecimal::from_f64(25.0).unwrap(), // Should aggregate with first USDC
                    },
                ],
            },
            SwapEventData {
                timestamp_seconds: timestamp, // Same timestamp = same bucket
                coin_volumes: vec![
                    CoinVolumeData {
                        coin: "APT".to_string(),
                        volume: BigDecimal::from_f64(25.0).unwrap(), // Should aggregate
                    },
                ],
            },
        ];
        
        let current_timestamp = timestamp + 3600; // 1 hour later
        let bucket_records = calculator.group_swaps_into_buckets(swap_events, current_timestamp);
        
        // Should have 2 bucket records (APT and USDC)
        assert_eq!(bucket_records.len(), 2);
        
        // Find APT record
        let apt_record = bucket_records.iter()
            .find(|r| r.coin == "APT")
            .expect("APT record should exist");
        
        // APT volume should be 100 + 75 + 25 = 200
        assert_eq!(apt_record.volume.as_ref().unwrap(), &BigDecimal::from_f64(200.0).unwrap());
        
        // Find USDC record
        let usdc_record = bucket_records.iter()
            .find(|r| r.coin == "USDC")
            .expect("USDC record should exist");
        
        // USDC volume should be 50 + 25 = 75
        assert_eq!(usdc_record.volume.as_ref().unwrap(), &BigDecimal::from_f64(75.0).unwrap());
        
        println!("✅ Volume aggregation test passed!");
        println!("   APT total volume: {}", apt_record.volume.as_ref().unwrap());
        println!("   USDC total volume: {}", usdc_record.volume.as_ref().unwrap());
    }

    #[test]
    fn test_bucket_sorting() {
        let calculator = BucketCalculator::new();
        
        // Create swap events with different timestamps and coins (in mixed order)
        let timestamp1 = 1734336000; // 2024-12-16 10:00:00 UTC (earlier)
        let timestamp2 = 1734343200; // 2024-12-16 12:00:00 UTC (later)
        
        let swap_events = vec![
            // USDC at later time (should be sorted after APT at earlier time)
            SwapEventData {
                timestamp_seconds: timestamp2,
                coin_volumes: vec![
                    CoinVolumeData {
                        coin: "USDC".to_string(),
                        volume: BigDecimal::from_f64(100.0).unwrap(),
                    },
                ],
            },
            // APT at later time (should be sorted after APT at earlier time)
            SwapEventData {
                timestamp_seconds: timestamp2,
                coin_volumes: vec![
                    CoinVolumeData {
                        coin: "APT".to_string(),
                        volume: BigDecimal::from_f64(200.0).unwrap(),
                    },
                ],
            },
            // APT at earlier time (should be first)
            SwapEventData {
                timestamp_seconds: timestamp1,
                coin_volumes: vec![
                    CoinVolumeData {
                        coin: "APT".to_string(),
                        volume: BigDecimal::from_f64(150.0).unwrap(),
                    },
                ],
            },
            // USDC at earlier time (should be after APT buckets)
            SwapEventData {
                timestamp_seconds: timestamp1,
                coin_volumes: vec![
                    CoinVolumeData {
                        coin: "USDC".to_string(),
                        volume: BigDecimal::from_f64(75.0).unwrap(),
                    },
                ],
            },
        ];
        
        let current_timestamp = timestamp2 + 3600; // 1 hour after latest
        let bucket_records = calculator.group_swaps_into_buckets(swap_events, current_timestamp);
        
        // Should have 4 bucket records
        assert_eq!(bucket_records.len(), 4);
        
        // Check sorting: should be APT (early), APT (late), USDC (early), USDC (late)
        assert_eq!(bucket_records[0].coin, "APT");
        assert!(bucket_records[0].bucket_start < bucket_records[1].bucket_start);
        
        assert_eq!(bucket_records[1].coin, "APT");
        assert!(bucket_records[1].bucket_start > bucket_records[0].bucket_start);
        
        assert_eq!(bucket_records[2].coin, "USDC");
        assert!(bucket_records[2].bucket_start < bucket_records[3].bucket_start);
        
        assert_eq!(bucket_records[3].coin, "USDC");
        assert!(bucket_records[3].bucket_start > bucket_records[2].bucket_start);
        
        println!("✅ Bucket sorting test passed!");
        println!("   Order: {} ({}), {} ({}), {} ({}), {} ({})", 
            bucket_records[0].coin, bucket_records[0].bucket_start.format("%H:%M"),
            bucket_records[1].coin, bucket_records[1].bucket_start.format("%H:%M"),
            bucket_records[2].coin, bucket_records[2].bucket_start.format("%H:%M"),
            bucket_records[3].coin, bucket_records[3].bucket_start.format("%H:%M"));
    }
} 