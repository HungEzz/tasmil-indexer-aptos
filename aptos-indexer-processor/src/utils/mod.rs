// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! # Utility Functions and Shared Components
//! 
//! This module contains utility functions and shared components used throughout
//! the Tasmil indexer for database operations, validation, and data processing.
//! 
//! ## Key Components
//! 
//! ### Database Utilities (`database`)
//! - Connection pool management and configuration
//! - Bulk insert operations with error handling
//! - Query optimization and performance monitoring
//! - Database migration utilities
//! 
//! ### Chain Validation (`chain_id`)
//! - Validates the indexer is connected to the correct Aptos network
//! - Prevents accidental indexing of wrong chain data
//! 
//! ### Version Management (`starting_version`)
//! - Determines the starting block version for indexing
//! - Handles resume from last processed version
//! - Supports both fresh starts and continuation from checkpoints

/// Database connection management, pooling, and utility functions
pub mod database;

/// Blockchain chain ID validation and verification utilities
pub mod chain_id;

/// Transaction version management and starting point determination
pub mod starting_version;
