// Copyright © Aptos Foundation  
// SPDX-License-Identifier: Apache-2.0

//! # Common Utilities and Shared Components
//! 
//! This module contains shared utilities and components used across different
//! parts of the Tasmil indexer system.
//! 
//! ## Components
//! 
//! ### Processor Status Management
//! - Tracks processing progress and checkpoints
//! - Handles graceful shutdown and restart scenarios
//! - Provides status reporting for monitoring systems
//! - Ensures data consistency across restarts
//! 
//! The processor status saver maintains the current processing state
//! in the database, allowing the indexer to resume from the last
//! successfully processed transaction in case of restarts or failures.

/// Processor status tracking and checkpoint management for reliable processing
pub mod processor_status_saver;

pub use processor_status_saver::get_processor_status_saver;
