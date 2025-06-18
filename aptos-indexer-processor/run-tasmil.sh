#!/bin/bash

# =============================================================================
# Tasmil Multi-Protocol Swap Indexer Startup Script
# =============================================================================
# This script starts the Tasmil indexer for processing swap events from
# multiple DEX protocols on the Aptos blockchain.
#
# Supported Protocols:
# - Cellana: Native AMM with concentrated liquidity
# - Thala: Multi-asset stable and volatile pools  
# - SushiSwap: Cross-chain DEX with extensive trading pairs
# - LiquidSwap: Native Aptos DEX with multiple pool types
# - Hyperion: V3 protocol with range orders
#
# Prerequisites:
# - Rust 1.75+ installed
# - PostgreSQL database configured and running
# - Valid config.yaml file in the current directory
#
# Usage:
#   ./run-tasmil.sh
#
# Environment Variables (optional):
#   RUST_LOG=info|debug|warn|error  (default: info)
#   CONFIG_PATH=/path/to/config.yaml (default: config.yaml)
# =============================================================================

# Set default values for environment variables
export RUST_LOG=${RUST_LOG:-info}
CONFIG_PATH=${CONFIG_PATH:-config.yaml}

# Check if config file exists
if [ ! -f "$CONFIG_PATH" ]; then
    echo "❌ Error: Configuration file '$CONFIG_PATH' not found!"
    echo "Please ensure the config.yaml file exists in the current directory."
    exit 1
fi

# Check if PostgreSQL is accessible (basic connectivity test)
if command -v pg_isready &> /dev/null; then
    echo "🔍 Testing PostgreSQL connectivity..."
    if ! pg_isready -h localhost -p 5432 &> /dev/null; then
        echo "⚠️  Warning: PostgreSQL server may not be running on localhost:5432"
        echo "Please ensure your PostgreSQL server is running and accessible."
    fi
fi

# Display startup information
echo "🚀 Starting Tasmil Multi-Protocol Swap Indexer..."
echo "📊 Protocols: Cellana, Thala, SushiSwap, LiquidSwap, Hyperion"
echo "📁 Config file: $CONFIG_PATH"
echo "📝 Log level: $RUST_LOG"
echo "⏰ Starting at: $(date)"
echo "==============================================="

# Start the indexer with optimized settings
cargo run --release -- --config-path "$CONFIG_PATH"

# Capture the exit code
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Indexer stopped gracefully"
else
    echo "❌ Indexer stopped with error code: $EXIT_CODE"
fi

exit $EXIT_CODE