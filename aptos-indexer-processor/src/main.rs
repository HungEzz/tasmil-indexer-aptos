// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! # Tasmil Aptos Multi-Protocol DEX Indexer
//! 
//! Processes swap events from multiple DEX protocols on Aptos:
//! - Cellana, Thala, SushiSwap, LiquidSwap, Hyperion
//! 
//! Calculates real-time 24h rolling volumes, fees, and time-bucketed data.

use anyhow::Result;
use aptos_indexer_processor::config::indexer_processor_config::IndexerProcessorConfig;
use aptos_indexer_processor_sdk_server_framework::ServerArgs;
use clap::Parser;

/// Configure jemalloc as the global allocator for better memory management
#[cfg(unix)]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// Main application entry point
/// 
/// Initializes the async runtime with optimized settings for blockchain data processing
/// and starts the indexer server with the provided configuration.
fn main() -> Result<()> {
    // Use at least 16 threads for concurrent database operations and network I/O
    let num_cpus = num_cpus::get();
    let worker_threads = num_cpus.max(16);

    // Build Tokio runtime optimized for high-throughput processing
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .disable_lifo_slot()  // Improves fairness in task scheduling
        .enable_all()         // Enable all I/O and timer drivers
        .worker_threads(worker_threads)
        .build()
        .expect("Failed to build async runtime")
        .block_on(async {
            // Parse command line arguments and run the indexer server
            let args = ServerArgs::parse();
            args.run::<IndexerProcessorConfig>(tokio::runtime::Handle::current())
                .await
        })
}
