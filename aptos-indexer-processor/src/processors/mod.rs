// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! # Transaction and Event Processors
//! 
//! This module contains the core processing logic for handling Aptos blockchain
//! transactions and extracting swap event data from multiple DEX protocols.
//! 
//! ## Main Components
//! 
//! ### `tasmil_processor`
//! The main orchestrator that coordinates transaction processing, volume calculation,
//! and database operations. It handles:
//! - Transaction batching and filtering
//! - Volume aggregation across protocols
//! - Database upserts and cleanup operations
//! - Real-time 24-hour rolling calculations
//! 
//! ### `events`
//! Contains protocol-specific processors and calculation engines:
//! - **Volume Calculator**: Aggregates trading volumes across all protocols
//! - **Bucket Calculator**: Creates time-based data buckets for charting
//! - **Protocol Processors**: Individual handlers for each DEX protocol
//!   - Cellana: Native AMM with concentrated liquidity
//!   - Thala: Multi-asset stable and volatile pools
//!   - SushiSwap: Cross-chain DEX with extensive trading pairs
//!   - LiquidSwap: Native Aptos DEX with multiple pool types
//!   - Hyperion: V3 protocol with range orders
//! 
//! ## Data Flow
//! 
//! ```text
//! Transaction Stream → TasmilProcessor → VolumeCalculator → Protocol Processors
//!                                                        ↓
//!                     Database ← BucketCalculator ← Event Extraction
//! ```
//! 
//! The processors use a modular architecture where each protocol has its own
//! specialized handler, making it easy to add new DEX protocols in the future.

/// Core transaction processor that orchestrates the entire indexing pipeline
pub mod tasmil_processor;

/// Event processing modules for swap data extraction and volume calculation
pub mod events;
