# Tasmil APT/USDC Indexer

🚀 **High-performance Aptos blockchain indexer for tracking APT/USDC trading data on CellanaFinance**

## 📊 Overview

This indexer extracts and processes **APT/USDC swap events** from CellanaFinance on the Aptos blockchain, providing:

- ✅ **Real-time APT price tracking** in USD
- ✅ **24-hour volume calculations** for APT/USDC pairs
- ✅ **Accurate decimal handling** (APT: 8 decimals, USDC: 6 decimals)
- ✅ **PostgreSQL storage** with optimized queries
- ✅ **Scalable processing pipeline** using Rust

## 🎯 Key Features

### 1. **APT Price Extraction**
- Monitors CellanaFinance `SwapEvent` transactions
- Calculates real-time APT price in USD based on APT/USDC swaps
- Handles both directions: APT→USDC and USDC→APT

### 2. **Volume Calculation**
- Computes 24-hour trading volume in USD
- Tracks total APT amounts traded
- Calculates average price over time periods

### 3. **Data Storage**
- **`apt_prices`**: Individual swap records with price data
- **`apt_volume_24h`**: Aggregated 24-hour volume statistics

## 🏗️ Architecture

```
Aptos Blockchain → Transaction Stream → CellanaSwapExtractor → AptPriceStorer → VolumeCalculator → VolumeStorer → PostgreSQL
```

### Core Components:

1. **`CellanaSwapExtractor`**: Extracts swap events and calculates APT prices
2. **`AptPriceStorer`**: Stores price data to database
3. **`VolumeCalculator`**: Computes 24h volume metrics
4. **`VolumeStorer`**: Stores volume data to database

## 📦 Database Schema

### APT Prices Table
```sql
CREATE TABLE apt_prices (
    id BIGSERIAL PRIMARY KEY,
    transaction_version BIGINT NOT NULL,
    apt_price_usd DECIMAL(20, 8) NOT NULL,
    amount_in DECIMAL(20, 8) NOT NULL,      -- Actual token amounts (converted from raw)
    amount_out DECIMAL(20, 8) NOT NULL,     -- Actual token amounts (converted from raw)
    from_token VARCHAR(300) NOT NULL,
    to_token VARCHAR(300) NOT NULL,
    pool_address VARCHAR(66) NOT NULL,
    timestamp_seconds BIGINT NOT NULL
);
```

### 24h Volume Table
```sql
CREATE TABLE apt_volume_24h (
    id BIGSERIAL PRIMARY KEY,
    total_volume_usd DECIMAL(20, 8) NOT NULL,
    total_apt_amount DECIMAL(20, 8) NOT NULL,
    avg_price DECIMAL(20, 8) NOT NULL,
    swap_count INTEGER NOT NULL,
    start_timestamp_seconds BIGINT NOT NULL,
    end_timestamp_seconds BIGINT NOT NULL
);
```

## 🚀 Quick Start

### 1. Prerequisites
```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install PostgreSQL
sudo apt-get install postgresql postgresql-contrib

# Install diesel CLI
cargo install diesel_cli --no-default-features --features postgres
```

### 2. Database Setup
```bash
# Create database
createdb tasmil_custom_indexer

# Run migrations
diesel migration run
```

### 3. Configuration
Update `config.yaml` with your settings:
```yaml
server_config:
  processor_config:
    type: "cellana_processor"
  transaction_stream_config:
    indexer_grpc_data_service_address: "https://grpc.mainnet.aptoslabs.com:443"
    starting_version: 2854805973
  db_config:
    postgres_connection_string: postgresql://username:password@localhost:5432/tasmil_custom_indexer
```

### 4. Run the Indexer
```bash
# Make script executable
chmod +x run-tasmil.sh

# Start indexing
./run-tasmil.sh
```

## 📈 Monitored Data

### **Pool Information**
- **Pool Address**: `0x71c6ae634bd3c36470eb7e7f4fb0912973bb31543dfdb7d7fb6863d886d81d67`
- **Trading Pair**: APT/USDC
- **APT Decimals**: 8
- **USDC Decimals**: 6

### **Tracked Events**
- **Event Type**: `0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::SwapEvent`
- **Swap Directions**: 
  - APT → USDC
  - USDC → APT

## 🔧 Development

### Build
```bash
cargo build --release
```

### Run Tests
```bash
cargo test
```

### Create New Migration
```bash
diesel migration generate migration_name
```

## 📊 Example Queries

### Get Latest APT Price
```sql
SELECT apt_price_usd, timestamp_seconds 
FROM apt_prices 
ORDER BY timestamp_seconds DESC 
LIMIT 1;
```

### Get 24h Volume
```sql
SELECT total_volume_usd, avg_price, swap_count
FROM apt_volume_24h
ORDER BY end_timestamp_seconds DESC
LIMIT 1;
```

### Price History (Last 100 swaps)
```sql
SELECT 
    apt_price_usd,
    amount_in,
    amount_out,
    from_token,
    to_token,
    timestamp_seconds
FROM apt_prices
ORDER BY timestamp_seconds DESC
LIMIT 100;
```

## 🎯 Technical Details

### **Decimal Conversion**
- Raw amounts from blockchain are converted to actual token amounts
- APT: `raw_amount / 10^8`
- USDC: `raw_amount / 10^6`

### **Price Calculation**
```rust
// APT → USDC
apt_price = usdc_amount / apt_amount

// USDC → APT  
apt_price = usdc_amount / apt_amount
```

### **Performance Features**
- Parallel transaction processing using Rayon
- Chunked database operations
- Connection pooling
- Optimized SQL queries with proper indexing

## 📝 License

This project is licensed under the Apache 2.0 License.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📞 Support

For questions and support, please open an issue in the repository.

---

**Built with ❤️ for the Aptos ecosystem** 🚀
