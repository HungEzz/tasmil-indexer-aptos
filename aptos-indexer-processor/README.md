# Tasmil Aptos Multi-Protocol DEX Indexer

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75+-orange.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-13+-blue.svg)](https://www.postgresql.org)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

A high-performance, real-time indexer for tracking swap events and calculating trading volumes across multiple DEX protocols on the Aptos blockchain. This production-ready indexer provides comprehensive analytics including 24-hour rolling volumes, time-bucketed data for charts, and detailed protocol-specific metrics.

## 🌟 Key Features

### 🔗 Multi-Protocol Support
- **Cellana**: Native AMM with concentrated liquidity features and advanced math
- **Thala**: Multi-asset stable and volatile pools with sophisticated algorithms  
- **SushiSwap**: Cross-chain DEX with extensive trading pair ecosystem
- **LiquidSwap**: Native Aptos DEX with multiple pool types and configurations
- **Hyperion**: V3 protocol with range orders and advanced trading features

### 📊 Advanced Analytics
- **Real-time Processing**: Sub-second latency transaction processing with live updates
- **Rolling Volume Calculation**: Continuous 24-hour volume tracking with automatic cleanup
- **Time-based Bucketing**: 2-hour interval data optimized for chart generation (GMT+7 timezone)
- **Fee Analytics**: Detailed protocol-specific fee calculation and aggregation
- **Cross-protocol Aggregation**: Unified volume metrics across all supported DEXs

### 🏗️ Enterprise Architecture
- **High Performance**: Optimized for handling thousands of transactions per second
- **Scalable Design**: Modular architecture enabling easy addition of new protocols
- **Production Ready**: Comprehensive error handling, monitoring, and recovery mechanisms
- **Database Optimization**: Advanced PostgreSQL features with connection pooling and indexing
- **Memory Efficiency**: Rust-powered performance with minimal resource footprint

## 🚀 Quick Start

### Prerequisites

Before you begin, ensure you have the following installed:

- **Rust**: Version 1.75.0 or later ([Install Rust](https://rustup.rs/))
- **PostgreSQL**: Version 13.0 or later ([Install PostgreSQL](https://www.postgresql.org/download/))
- **Git**: For cloning the repository
- **System Requirements**: 
  - 8GB RAM minimum (16GB recommended for production)
  - SSD storage for optimal database performance
  - Linux/macOS (Windows via WSL2)

### Installation

1. **Clone the Repository**

```bash
git clone https://github.com/your-org/tasmil-aptos-indexer.git
cd tasmil-aptos-indexer/aptos-indexer-processor
```

2. **Install Rust Dependencies**

```bash
# Install Diesel CLI for database management
cargo install diesel_cli --no-default-features --features postgres

# Build the project in release mode
cargo build --release
```

3. **Database Setup**

**Install PostgreSQL** (if not already installed):

```bash
# Ubuntu/Debian
sudo apt update && sudo apt install postgresql postgresql-contrib

# macOS
brew install postgresql
brew services start postgresql

# CentOS/RHEL
sudo yum install postgresql-server postgresql-contrib
sudo postgresql-setup initdb
sudo systemctl start postgresql
```

**Create Database and User**:

```bash
# Access PostgreSQL as superuser
sudo -u postgres psql

# Create database and user (run these commands in psql)
CREATE DATABASE tasmil_custom_indexer;
CREATE USER manager1 WITH ENCRYPTED PASSWORD 'manager1';
GRANT ALL PRIVILEGES ON DATABASE tasmil_custom_indexer TO manager1;
GRANT ALL ON SCHEMA public TO manager1;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO manager1;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO manager1;
ALTER USER manager1 CREATEDB;
\q
```

4. **Configure the Application**

```bash
# The config.yaml file should already be properly configured for local development
# Verify your database settings match:
cat config.yaml
```

5. **Run Database Migrations**

```bash
# Run migrations to set up the database schema
DATABASE_URL="postgresql://manager1:manager1@localhost:5432/tasmil_custom_indexer" \
diesel migration run
```

6. **Start the Indexer**

```bash
# Make the run script executable
chmod +x run-tasmil.sh

# Start the indexer
./run-tasmil.sh
```

You should see output like:
```
🚀 Starting Tasmil Multi-Protocol Swap Indexer...
📊 Protocols: Cellana, Thala, SushiSwap, LiquidSwap, Hyperion
📁 Config file: config.yaml
📝 Log level: info
⏰ Starting at: Wed Jan 15 10:30:45 UTC 2025
===============================================
🚀 Initializing VolumeCalculator with modular architecture
📊 Configured for Cellana, Thala, SushiSwap, LiquidSwap, and Hyperion volume tracking
✅ Database connection pool created with size: 150
📌 Starting from version: 2890500000
```

## 📊 Architecture Overview

```mermaid
graph TB
    A[Aptos Blockchain] --> B[gRPC Transaction Stream]
    B --> C[Transaction Processor]
    C --> D[Volume Calculator]
    D --> E[Protocol Processors]
    E --> F[Database Layer]
    
    E --> G[Cellana Processor]
    E --> H[Thala Processor]
    E --> I[SushiSwap Processor]
    E --> J[LiquidSwap Processor]
    E --> K[Hyperion Processor]
    
    F --> L[PostgreSQL Database]
    L --> M[apt_data Table]
    L --> N[coin_volume_24h Table]
    L --> O[coin_volume_buckets Table]
    
    F --> P[Analytics APIs]
    P --> Q[Volume Metrics]
    P --> R[Chart Data]
    P --> S[Fee Analytics]
```

### Core Components

| Component | Responsibility | Key Features |
|-----------|----------------|--------------|
| **Transaction Processor** | Filters and processes blockchain transactions | Batch processing, error handling, version tracking |
| **Volume Calculator** | Aggregates trading volumes across protocols | Real-time calculations, 24h rolling windows |
| **Protocol Processors** | Extract swap data from specific DEX protocols | Modular design, protocol-specific logic |
| **Bucket Calculator** | Creates time-based data buckets | 2-hour intervals, timezone-aware (GMT+7) |
| **Database Layer** | Manages PostgreSQL storage and queries | Connection pooling, bulk operations, indexing |

## 💾 Database Schema

### Primary Tables

#### `apt_data` - Protocol Volume Aggregations
Stores 24-hour rolling volumes and fees by protocol:

| Column | Type | Description |
|--------|------|-------------|
| `protocol_name` | VARCHAR(PK) | Protocol identifier (cellana, thala, sushiswap, etc.) |
| `apt_volume_24h` | DECIMAL | 24-hour APT trading volume |
| `usdc_volume_24h` | DECIMAL | 24-hour USDC trading volume |
| `usdt_volume_24h` | DECIMAL | 24-hour USDT trading volume |
| `weth_volume_24h` | DECIMAL | 24-hour WETH trading volume |
| `apt_fee_24h` | DECIMAL | 24-hour APT fees collected |
| `usdc_fee_24h` | DECIMAL | 24-hour USDC fees collected |
| `usdt_fee_24h` | DECIMAL | 24-hour USDT fees collected |
| `weth_fee_24h` | DECIMAL | 24-hour WETH fees collected |
| `inserted_at` | TIMESTAMP | Last update timestamp |

#### `coin_volume_24h` - Cross-Protocol Coin Volumes
Tracks aggregated 24-hour volumes by coin across all protocols:

| Column | Type | Description |
|--------|------|-------------|
| `coin` | VARCHAR(PK) | Coin identifier (aptos, usdc, usdt, weth) |
| `buy_volume` | DECIMAL | 24-hour buy volume |
| `sell_volume` | DECIMAL | 24-hour sell volume |
| `inserted_at` | TIMESTAMP | Last update timestamp |

#### `coin_volume_buckets` - Time-Series Analytics
Provides time-bucketed volume data for chart generation:

| Column | Type | Description |
|--------|------|-------------|
| `coin` | VARCHAR | Coin identifier |
| `bucket_start` | TIMESTAMP | Bucket start time (2-hour intervals) |
| `bucket_end` | TIMESTAMP | Bucket end time |
| `volume` | DECIMAL | Volume within the time bucket |
| `inserted_at` | TIMESTAMP | Record creation timestamp |

### Sample Queries

```sql
-- Get current 24h volumes by protocol
SELECT 
    protocol_name,
    apt_volume_24h,
    usdc_volume_24h,
    usdt_volume_24h,
    weth_volume_24h
FROM apt_data 
ORDER BY apt_volume_24h DESC NULLS LAST;

-- Get hourly volume chart data for APT (last 24 hours)
SELECT 
    bucket_start,
    volume,
    bucket_end
FROM coin_volume_buckets 
WHERE coin = 'aptos' 
  AND bucket_start >= NOW() - INTERVAL '24 hours'
ORDER BY bucket_start ASC;

-- Get top trading coins by 24h volume
SELECT 
    coin,
    (COALESCE(buy_volume, 0) + COALESCE(sell_volume, 0)) as total_volume
FROM coin_volume_24h 
WHERE buy_volume IS NOT NULL OR sell_volume IS NOT NULL
ORDER BY total_volume DESC 
LIMIT 10;

-- Get protocol fee comparison
SELECT 
    protocol_name,
    (COALESCE(apt_fee_24h, 0) + COALESCE(usdc_fee_24h, 0) + 
     COALESCE(usdt_fee_24h, 0) + COALESCE(weth_fee_24h, 0)) as total_fees_24h
FROM apt_data 
ORDER BY total_fees_24h DESC NULLS LAST;
```

## ⚙️ Configuration

### Configuration File Structure

The `config.yaml` file contains all indexer settings:

```yaml
# Health check endpoint for monitoring
health_check_port: 8085

server_config:
  processor_config:
    type: "swap_processor"  # Multi-protocol processor type
  
  # Aptos blockchain connection settings
  transaction_stream_config:
    indexer_grpc_data_service_address: "https://grpc.mainnet.aptoslabs.com:443"
    starting_version: 2890500000  # Starting block version
    auth_token: "your_auth_token"  # Aptos Labs API token
    request_name_header: "tasmil-apt-usdc-processor"
  
  # Database connection settings
  db_config:
    postgres_connection_string: "postgresql://manager1:manager1@localhost:5432/tasmil_custom_indexer"
    db_pool_size: 150  # Connection pool size
```

### Environment Variables

You can override configuration with environment variables:

```bash
# Set log level (debug, info, warn, error)
export RUST_LOG=info

# Override config file path
export CONFIG_PATH=/path/to/custom/config.yaml

# Database connection override
export DATABASE_URL="postgresql://user:pass@host:port/dbname"
```

### Production Settings

For production deployment, consider these optimizations:

```yaml
# config-production.yaml
health_check_port: 8085

server_config:
  processor_config:
    type: "swap_processor"
  
  transaction_stream_config:
    indexer_grpc_data_service_address: "https://grpc.mainnet.aptoslabs.com:443"
    starting_version: 0  # Full historical sync
    auth_token: "${APTOS_AUTH_TOKEN}"  # Use environment variable
    request_name_header: "tasmil-production-indexer"
  
  db_config:
    postgres_connection_string: "${DATABASE_URL}"
    db_pool_size: 200  # Increased for production load
```

## 🔧 Development

### Project Structure

```
aptos-indexer-processor/
├── src/
│   ├── processors/           # Core processing logic
│   │   ├── events/          # Event processors and calculations
│   │   │   ├── volume_calculator.rs    # Main volume aggregation
│   │   │   ├── bucket_calculator.rs    # Time-based bucketing
│   │   │   ├── cellana/               # Cellana protocol handler
│   │   │   ├── thala/                 # Thala protocol handler
│   │   │   ├── sushiswap/             # SushiSwap protocol handler
│   │   │   ├── liquidswap/            # LiquidSwap protocol handler
│   │   │   ├── hyperion/              # Hyperion protocol handler
│   │   │   └── swap_processor.rs      # Main processor orchestration
│   │   └── tasmil_processor.rs        # Transaction processing coordinator
│   ├── db/                  # Database layer
│   │   ├── common/models/   # Shared data models
│   │   └── postgres/        # PostgreSQL-specific code
│   ├── config/              # Configuration management
│   ├── utils/               # Shared utilities
│   ├── common/              # Common components
│   ├── main.rs              # Application entry point
│   └── lib.rs               # Library root
├── migrations/              # Database schema migrations
├── config.yaml              # Main configuration file
├── run-tasmil.sh           # Startup script
├── Cargo.toml              # Rust dependencies and metadata
└── README.md               # This documentation
```

### Adding New DEX Protocols

The indexer's modular architecture makes adding new protocols straightforward:

1. **Create Protocol Module**

```bash
mkdir src/processors/events/newprotocol
touch src/processors/events/newprotocol/mod.rs
touch src/processors/events/newprotocol/processor.rs
touch src/processors/events/newprotocol/constants.rs
```

2. **Implement Protocol Constants**

```rust
// src/processors/events/newprotocol/constants.rs
pub const NEW_PROTOCOL_SWAP_EVENT_TYPE: &str = "new_protocol_swap_event_signature";
pub const NEW_PROTOCOL_CONTRACT_ADDRESS: &str = "0x...";
```

3. **Implement Protocol Processor**

```rust
// src/processors/events/newprotocol/processor.rs
use serde_json::Value;
use anyhow::Result;
use bigdecimal::BigDecimal;

#[derive(Debug, Clone)]
pub struct SwapData {
    pub pool: String,
    pub coin_in: String,
    pub coin_out: String,
    pub amount_in: BigDecimal,
    pub amount_out: BigDecimal,
    pub fee_amount: BigDecimal,
}

pub struct NewProtocolProcessor;

impl NewProtocolProcessor {
    pub fn new() -> Self {
        Self
    }

    pub fn extract_swap_data(&self, event_data: &Value) -> Result<SwapData> {
        // Implement protocol-specific data extraction
        todo!("Implement swap data extraction logic")
    }

    pub async fn process_swap(
        &mut self, 
        volumes: &mut HashMap<String, PoolVolume>, 
        swap_data: SwapData
    ) {
        // Implement protocol-specific volume calculation
        todo!("Implement volume processing logic")
    }
}
```

4. **Update Volume Calculator**

```rust
// Add to src/processors/events/volume_calculator.rs
use super::newprotocol::{NewProtocolProcessor, constants::NEW_PROTOCOL_SWAP_EVENT_TYPE};

// Add to VolumeCalculator struct
pub struct VolumeCalculator {
    // ... existing processors
    new_protocol_processor: NewProtocolProcessor,
}

// Add to process method
NEW_PROTOCOL_SWAP_EVENT_TYPE => {
    debug!("🟣 Processing NewProtocol swap event");
    // Process the event
}
```

### Testing

```bash
# Run unit tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test
cargo test test_volume_calculation

# Run with debug logging
RUST_LOG=debug cargo test

# Performance testing
cargo bench
```

### Code Quality Tools

```bash
# Install development tools
cargo install cargo-watch cargo-audit cargo-tarpaulin clippy-sarif sarif-fmt

# Format code
cargo fmt

# Lint code  
cargo clippy -- -D warnings

# Security audit
cargo audit

# Generate test coverage
cargo tarpaulin --out html

# Watch for changes and run tests
cargo watch -x test

# Check for unused dependencies
cargo machete
```

## 🔍 Monitoring & Operations

### Health Checks

The indexer exposes a health check endpoint:

```bash
# Check if the indexer is running
curl http://localhost:8085/health

# Expected response
{"status": "healthy", "timestamp": "2025-01-15T10:30:45Z"}
```

### Key Metrics to Monitor

1. **Processing Metrics**
   - Transactions processed per second
   - Processing latency (median, p95, p99)
   - Error rate percentage
   - Queue depth and backlog

2. **Database Metrics**
   - Connection pool utilization
   - Query execution time
   - Database size growth
   - Index usage statistics

3. **System Metrics**
   - Memory usage and heap size
   - CPU utilization
   - Network I/O throughput
   - Disk space and I/O

4. **Business Metrics**
   - Volume calculation accuracy
   - Protocol coverage percentage
   - Data freshness (lag behind blockchain)
   - Fee calculation totals

### Logging

The indexer uses structured logging with multiple levels:

```bash
# Set detailed debug logging
export RUST_LOG=debug

# Set per-module logging
export RUST_LOG=aptos_indexer_processor_example=debug,diesel=warn

# Production logging (info level)
export RUST_LOG=info
```

### Troubleshooting

**Common Issues and Solutions:**

1. **Database Connection Issues**
```bash
# Test database connectivity
pg_isready -h localhost -p 5432

# Check database permissions
psql -U manager1 -d tasmil_custom_indexer -c "\dt"
```

2. **High Memory Usage**
```bash
# Monitor memory usage
ps aux | grep aptos-indexer-processor

# Check for memory leaks
valgrind --tool=memcheck cargo run --release
```

3. **Processing Lag**
```bash
# Check current processing version vs blockchain
# Compare with latest version from Aptos node
curl -X GET "https://fullnode.mainnet.aptoslabs.com/v1" | jq '.ledger_version'
```

## 🐳 Docker Deployment

### Dockerfile

```dockerfile
FROM rust:1.75-slim as builder

WORKDIR /app
COPY . .
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libssl3 \
    libpq5 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/aptos-indexer-processor /usr/local/bin/
COPY --from=builder /app/config.yaml /etc/tasmil/

EXPOSE 8085
CMD ["aptos-indexer-processor", "--config-path", "/etc/tasmil/config.yaml"]
```

### Docker Compose

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: tasmil_custom_indexer
      POSTGRES_USER: manager1
      POSTGRES_PASSWORD: manager1
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U manager1"]
      interval: 30s
      timeout: 10s
      retries: 5

  indexer:
    build: .
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      RUST_LOG: info
    ports:
      - "8085:8085"
    volumes:
      - ./config.yaml:/etc/tasmil/config.yaml:ro
    restart: unless-stopped

volumes:
  postgres_data:
```

## 🔐 Security

### Database Security

- Use strong passwords for database users
- Enable SSL connections for production
- Restrict database access to authorized hosts only
- Regular security updates for PostgreSQL

### API Security

- Secure storage of Aptos authentication tokens
- Network-level access controls
- Rate limiting for health check endpoints
- Monitoring for suspicious activities

### Secrets Management

```bash
# Use environment variables for sensitive data
export APTOS_AUTH_TOKEN="your_secure_token"
export DATABASE_PASSWORD="your_secure_password"

# Or use a secrets management system like:
# - HashiCorp Vault
# - AWS Secrets Manager
# - Kubernetes Secrets
```

## 📈 Performance Optimization

### Database Optimization

```sql
-- Recommended indexes for performance
CREATE INDEX CONCURRENTLY idx_apt_data_protocol_name ON apt_data(protocol_name);
CREATE INDEX CONCURRENTLY idx_coin_volume_24h_coin ON coin_volume_24h(coin);
CREATE INDEX CONCURRENTLY idx_coin_volume_buckets_coin_time ON coin_volume_buckets(coin, bucket_start);
CREATE INDEX CONCURRENTLY idx_events_type_version ON events(indexed_type, transaction_version);

-- Vacuum and analyze for optimal performance
VACUUM ANALYZE apt_data;
VACUUM ANALYZE coin_volume_24h;
VACUUM ANALYZE coin_volume_buckets;
```

### Application Tuning

```bash
# Increase worker threads for high-load scenarios
export TOKIO_WORKER_THREADS=32

# Optimize memory allocator
export MALLOC_CONF="background_thread:true,metadata_thp:auto"

# JVM-style GC for jemalloc
export MALLOC_CONF="dirty_decay_ms:1000,muzzy_decay_ms:5000"
```

## 🤝 Contributing

We welcome contributions! Please follow these guidelines:

### Development Workflow

1. **Fork and Clone**
```bash
git clone https://github.com/your-username/tasmil-aptos-indexer.git
cd tasmil-aptos-indexer
```

2. **Create Feature Branch**
```bash
git checkout -b feature/new-protocol-support
```

3. **Development Setup**
```bash
# Install development dependencies
cargo install cargo-watch cargo-audit cargo-tarpaulin

# Run tests in watch mode
cargo watch -x test

# Check code quality
cargo fmt && cargo clippy
```

4. **Testing**
```bash
# Run comprehensive tests
cargo test
cargo bench
cargo audit
```

5. **Submit Pull Request**
- Write clear commit messages
- Include comprehensive tests
- Update documentation
- Follow Rust conventions

### Code Standards

- **Documentation**: All public APIs must be documented
- **Testing**: Maintain >90% test coverage
- **Performance**: Benchmark performance-critical changes
- **Security**: Security review for any external dependencies

## 📜 License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for complete details.

```
Copyright 2025 Tasmil Project Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## 🗺️ Roadmap

### Q1 2025 - Core Enhancement
- [x] Multi-protocol support (Cellana, Thala, SushiSwap, LiquidSwap, Hyperion)
- [x] Real-time volume calculation
- [x] Time-bucketed analytics
- [ ] WebSocket API for real-time data streaming
- [ ] REST API for historical data queries
- [ ] Grafana dashboard templates

### Q2 2025 - Advanced Features
- [ ] Additional DEX protocol integrations
- [ ] Advanced DeFi metrics (TVL, APY calculations)
- [ ] Data export capabilities (CSV, JSON, Parquet)
- [ ] Performance optimizations and caching layer
- [ ] Mobile-optimized API endpoints

### Q3 2025 - Enterprise Features  
- [ ] Multi-chain support (Ethereum, BSC, Polygon)
- [ ] Machine learning price prediction models
- [ ] Advanced analytics dashboard
- [ ] Enterprise-grade monitoring and alerting

### Q4 2025 - Ecosystem Integration
- [ ] Third-party integrations (DeFiPulse, CoinGecko)
- [ ] Decentralized data verification
- [ ] Community governance mechanisms
- [ ] Open data marketplace

## 🙏 Acknowledgments

- **Aptos Labs**: For the robust blockchain infrastructure and indexer SDK
- **Rust Community**: For the excellent ecosystem and tools
- **PostgreSQL Team**: For the reliable and performant database system
- **DEX Protocol Teams**: Cellana, Thala, SushiSwap, LiquidSwap, and Hyperion for their innovation

---

**Built with ❤️ for the Aptos DeFi Ecosystem**

*Empowering developers and traders with comprehensive, real-time DEX analytics*
