// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! # Configuration Management
//! 
//! This module handles all configuration aspects of the Tasmil indexer,
//! including processor settings, database connections, and runtime parameters.
//! 
//! ## Configuration Structure
//! 
//! The configuration system is hierarchical:
//! - **IndexerProcessorConfig**: Top-level configuration container
//! - **ProcessorConfig**: Processor-specific settings and type definitions
//! - **DatabaseConfig**: Database connection and pooling parameters
//! - **TransactionStreamConfig**: Blockchain data stream configuration
//! 
//! ## Configuration Sources
//! 
//! Configuration can be loaded from:
//! - YAML configuration files (primary method)
//! - Environment variables (for sensitive data like credentials)
//! - Command-line arguments (for runtime overrides)
//! 
//! ## Validation
//! 
//! All configuration values are validated at startup to ensure:
//! - Database connection strings are properly formatted
//! - Required authentication tokens are present
//! - Network endpoints are accessible
//! - Protocol-specific settings are valid

/// Main indexer processor configuration including all subsystem settings
pub mod indexer_processor_config;

/// Processor type definitions and protocol-specific configuration options
pub mod processor_config;
