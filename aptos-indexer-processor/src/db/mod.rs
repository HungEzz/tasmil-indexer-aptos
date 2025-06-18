// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! # Database Layer
//! 
//! This module provides the database abstraction layer for the Tasmil indexer,
//! including models, schema definitions, and database connection management.
//! 
//! ## Architecture
//! 
//! The database layer is organized into:
//! - **PostgreSQL-specific code**: Connection management, migrations, schema
//! - **Common models**: Shared data structures and database models
//! - **ORM integration**: Diesel-based type-safe database operations
//! 
//! ## Database Schema
//! 
//! The indexer uses several core tables:
//! - `apt_data`: Protocol-specific volume and fee aggregations
//! - `coin_volume_24h`: 24-hour rolling volumes by coin type
//! - `coin_volume_buckets`: Time-bucketed volume data for analytics
//! - `events`: Raw event data from blockchain transactions
//! - `ledger_infos`: Chain metadata and validation information
//! 
//! ## Performance Optimizations
//! 
//! - Connection pooling with configurable pool sizes
//! - Bulk insert operations for high-throughput scenarios
//! - Optimized indexing strategies for time-series queries
//! - Automatic cleanup of historical data beyond 24-hour window

/// Common database models and shared data structures
pub mod common;

/// PostgreSQL-specific implementation including connection management and schema
pub mod postgres;
