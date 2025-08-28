// src/lib.rs - Trading OMS Library Root
//! # Trading OMS - High-Performance Order Management System
//!
//! A modern, high-performance Order Management System (OMS) built in Rust with focus on:
//! - Ultra-low latency order processing
//! - Lock-free concurrent architecture
//! - FIX protocol integration
//! - Real-time market data handling
//! - Comprehensive risk management
//! - Smart order routing
//! 
//! ## Features
//!
//! - **Order Management**: Complete order lifecycle management with state transitions
//! - **FIX Protocol**: Industry-standard FIX 4.4 protocol implementation
//! - **Risk Management**: Pre-trade and real-time risk checks
//! - **Smart Routing**: Intelligent order routing across multiple venues
//! - **Real-time Streaming**: WebSocket-based real-time updates
//! - **High Performance**: Lock-free data structures and zero-copy operations
//! - **Observability**: Comprehensive metrics and structured logging
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
//! │   REST API      │    │   WebSocket     │    │   FIX Gateway   │
//! │   (HTTP/JSON)   │    │   (Real-time)   │    │   (Market)      │
//! └─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
//!           │                      │                      │
//!           └──────────────────────┼──────────────────────┘
//!                                  │
//!                    ┌─────────────▼─────────────┐
//!                    │      Order Engine         │
//!                    │   (Matching & Routing)    │
//!                    └─────────────┬─────────────┘
//!                                  │
//!            ┌─────────────────────┼─────────────────────┐
//!            │                     │                     │
//!   ┌────────▼────────┐   ┌───────▼────────┐   ┌───────▼────────┐
//!   │   Risk Engine   │   │  Market Data   │   │    Storage     │
//!   │   (Validation)  │   │   (Pricing)    │   │ (Persistence)  │
//!   └─────────────────┘   └────────────────┘   └────────────────┘
//! ```
//!
//! ## Quick Start
//!
//! ```rust
//! use trading_oms::prelude::*;
//! 
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create order
//!     let order = OrderBuilder::new()
//!         .symbol("AAPL")
//!         .side(OrderSide::Buy)
//!         .quantity(100.0)
//!         .limit_price(150.0)
//!         .account("DEMO_ACCOUNT")
//!         .build()?;
//!     
//!     // Create OMS engine
//!     let oms = OmsEngine::new(OmsConfig::default()).await?;
//!     
//!     // Submit order
//!     let response = oms.submit_order(order).await?;
//!     println!("Order submitted: {}", response.order.id);
//!     
//!     Ok(())
//! }
//! ```

#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::must_use_candidate
)]
#![deny(missing_docs, unsafe_code)]

use std::sync::Arc;

// Re-export commonly used types
pub use anyhow::{Error as AnyhowError, Result as AnyhowResult};
pub use chrono::{DateTime, Utc};
pub use serde::{Deserialize, Serialize};
pub use tokio;
pub use uuid::Uuid;

// Core modules
pub mod core;
pub mod engine;
pub mod fix_handler;

// Feature-gated modules
#[cfg(feature = "risk")]
pub mod risk;

#[cfg(feature = "routing")]
pub mod routing;

#[cfg(feature = "storage")]
pub mod storage;

#[cfg(feature = "transport")]
pub mod transport;

#[cfg(feature = "market")]
pub mod market;

#[cfg(feature = "monitoring")]
pub mod monitoring;

// Re-export core types for convenience
pub use core::{
    events::{OrderEvent, OrderEventType},
    order::{
        Order, OrderBuilder, OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType,
        OrderUpdate, TimeInForce, Venue,
    },
    types::{AccountId, ClientOrderId, OrderId, Price, Quantity, Symbol, Timestamp},
};

pub use fix_handler::{FixHandler, FixMessage, ParsedFixMessage};

/// Prelude module for convenient imports
pub mod prelude {
    //! Prelude module that re-exports the most commonly used types and traits
    
    pub use crate::{
        core::{
            order::{Order, OrderBuilder, OrderSide, OrderStatus, OrderType, TimeInForce},
            types::{AccountId, OrderId, Price, Quantity, Symbol},
        },
        AnyhowResult as Result,
        DateTime, Serialize, Deserialize, Uuid, Utc,
    };
    
    pub use crate::engine::{OmsEngine, EngineConfig, OrderValidator, ValidationConfig, ValidationResult};
    
    #[cfg(feature = "risk")]
    pub use crate::risk::{RiskEngine, RiskCheck, RiskResult};
    
    #[cfg(feature = "routing")]
    pub use crate::routing::{Router, RoutingDecision, VenueConnector};
}

/// Library version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Library name
pub const NAME: &str = env!("CARGO_PKG_NAME");

/// Library description
pub const DESCRIPTION: &str = env!("CARGO_PKG_DESCRIPTION");

/// Default configuration for the OMS
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OmsConfig {
    /// Server configuration
    pub server: ServerConfig,
    /// Trading configuration  
    pub trading: TradingConfig,
    /// FIX protocol configuration
    pub fix: FixConfig,
    /// Logging configuration
    pub logging: LoggingConfig,
    /// Risk management configuration
    pub risk: RiskConfig,
    /// Storage configuration
    pub storage: StorageConfig,
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Host address to bind to
    pub host: String,
    /// Port to listen on
    pub port: u16,
    /// Maximum number of concurrent connections
    pub max_connections: usize,
    /// Request timeout in seconds
    pub request_timeout: u64,
    /// Enable TLS/SSL
    pub enable_tls: bool,
    /// TLS certificate path (if TLS enabled)
    pub tls_cert_path: Option<String>,
    /// TLS private key path (if TLS enabled)  
    pub tls_key_path: Option<String>,
}

/// Trading configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingConfig {
    /// Maximum order size allowed
    pub max_order_size: f64,
    /// Maximum orders per second per client
    pub max_orders_per_second: u32,
    /// Enable pre-trade risk checks
    pub enable_risk_checks: bool,
    /// Default venue for orders
    pub default_venue: String,
    /// Supported venues
    pub supported_venues: Vec<String>,
    /// Enable order routing
    pub enable_routing: bool,
    /// Enable market making features
    pub enable_market_making: bool,
}

/// FIX protocol configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixConfig {
    /// Enable FIX protocol
    pub enabled: bool,
    /// FIX listener port
    pub port: u16,
    /// Sender CompID
    pub sender_comp_id: String,
    /// Target CompID
    pub target_comp_id: String,
    /// Heartbeat interval in seconds
    pub heartbeat_interval: u64,
    /// FIX version to use
    pub version: String,
    /// Enable message validation
    pub validate_messages: bool,
    /// Message store directory
    pub store_directory: String,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error)
    pub level: String,
    /// Log format (json, pretty, compact)
    pub format: String,
    /// Optional log file path
    pub file_path: Option<String>,
    /// Enable structured logging
    pub structured: bool,
    /// Log to console
    pub console: bool,
    /// Include file and line numbers in logs
    pub include_location: bool,
}

/// Risk management configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    /// Enable real-time risk monitoring
    pub enabled: bool,
    /// Maximum position size per symbol
    pub max_position_size: f64,
    /// Maximum daily loss limit
    pub max_daily_loss: f64,
    /// Maximum order value
    pub max_order_value: f64,
    /// Enable position limits
    pub enable_position_limits: bool,
    /// Enable credit checks
    pub enable_credit_checks: bool,
    /// Risk check timeout in milliseconds
    pub check_timeout_ms: u64,
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Storage backend (memory, sqlite, postgres)
    pub backend: String,
    /// Database connection string (for persistent storage)
    pub connection_string: Option<String>,
    /// Enable write-ahead logging
    pub enable_wal: bool,
    /// Maximum cache size in MB
    pub cache_size_mb: usize,
    /// Enable compression
    pub enable_compression: bool,
    /// Data retention period in days
    pub retention_days: u32,
}

impl Default for OmsConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 8080,
                max_connections: 10000,
                request_timeout: 30,
                enable_tls: false,
                tls_cert_path: None,
                tls_key_path: None,
            },
            trading: TradingConfig {
                max_order_size: 1_000_000.0,
                max_orders_per_second: 1000,
                enable_risk_checks: true,
                default_venue: "EXCHANGE".to_string(),
                supported_venues: vec!["EXCHANGE".to_string(), "ECN1".to_string(), "ECN2".to_string()],
                enable_routing: true,
                enable_market_making: false,
            },
            fix: FixConfig {
                enabled: false,
                port: 9878,
                sender_comp_id: "TRADING_OMS".to_string(),
                target_comp_id: "EXCHANGE".to_string(),
                heartbeat_interval: 30,
                version: "FIX.4.4".to_string(),
                validate_messages: true,
                store_directory: "./fix_store".to_string(),
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                format: "json".to_string(),
                file_path: None,
                structured: true,
                console: true,
                include_location: false,
            },
            risk: RiskConfig {
                enabled: true,
                max_position_size: 100_000.0,
                max_daily_loss: 50_000.0,
                max_order_value: 1_000_000.0,
                enable_position_limits: true,
                enable_credit_checks: true,
                check_timeout_ms: 100,
            },
            storage: StorageConfig {
                backend: "memory".to_string(),
                connection_string: None,
                enable_wal: true,
                cache_size_mb: 256,
                enable_compression: false,
                retention_days: 90,
            },
        }
    }
}

/// Application state shared across the system
#[derive(Clone)]
pub struct AppState {
    /// Configuration
    pub config: Arc<OmsConfig>,
    /// Metrics collector
    pub metrics: Arc<dyn MetricsCollector + Send + Sync>,
    /// Event bus for order events
    pub event_bus: Arc<dyn EventBus + Send + Sync>,
}

/// Trait for collecting and reporting system metrics
pub trait MetricsCollector {
    /// Increment a counter metric
    fn increment_counter(&self, name: &str, value: u64);
    
    /// Set a gauge metric
    fn set_gauge(&self, name: &str, value: f64);
    
    /// Record a histogram value
    fn record_histogram(&self, name: &str, value: f64);
    
    /// Get all metric values
    fn get_metrics(&self) -> std::collections::HashMap<String, f64>;
}

/// Trait for publishing and subscribing to events
pub trait EventBus {
    /// Publish an event
    fn publish(&self, event: Box<dyn Event + Send + Sync>);
    
    /// Subscribe to events of a specific type
    fn subscribe<T>(&self, callback: Box<dyn Fn(&T) + Send + Sync>)
    where
        T: Event + 'static;
}

/// Base trait for all events in the system
pub trait Event: std::fmt::Debug {
    /// Get the event timestamp
    fn timestamp(&self) -> DateTime<Utc>;
    
    /// Get the event type identifier
    fn event_type(&self) -> &'static str;
    
    /// Convert to JSON for serialization
    fn to_json(&self) -> serde_json::Value;
}

/// Error types used throughout the library
#[derive(Debug, thiserror::Error)]
pub enum OmsError {
    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),
    
    /// Order validation error
    #[error("Order validation failed: {0}")]
    OrderValidation(String),
    
    /// Risk check failure
    #[error("Risk check failed: {0}")]
    RiskCheck(String),
    
    /// Routing error
    #[error("Order routing failed: {0}")]
    Routing(String),
    
    /// Storage error
    #[error("Storage error: {0}")]
    Storage(String),
    
    /// Network/transport error
    #[error("Network error: {0}")]
    Network(String),
    
    /// FIX protocol error
    #[error("FIX protocol error: {0}")]
    FixProtocol(String),
    
    /// Market data error
    #[error("Market data error: {0}")]
    MarketData(String),
    
    /// Internal system error
    #[error("Internal error: {0}")]
    Internal(String),
    
    /// External service error
    #[error("External service error: {0}")]
    External(String),
    
    /// Timeout error
    #[error("Operation timed out: {0}")]
    Timeout(String),
}

/// Result type used throughout the library
pub type OmsResult<T> = Result<T, OmsError>;

/// Utility functions
pub mod utils {
    //! Utility functions for common operations
    
    use super::*;
    
    /// Generate a unique client order ID
    pub fn generate_client_order_id(prefix: &str) -> String {
        format!("{}_{}", prefix, Uuid::new_v4().simple())
    }
    
    /// Calculate order value
    pub fn calculate_order_value(quantity: f64, price: Option<f64>) -> Option<f64> {
        price.map(|p| quantity * p)
    }
    
    /// Format price for display
    pub fn format_price(price: f64, decimals: usize) -> String {
        format!("{:.prec$}", price, prec = decimals)
    }
    
    /// Parse symbol from string with validation
    pub fn parse_symbol(symbol: &str) -> OmsResult<String> {
        if symbol.is_empty() || symbol.len() > 20 {
            return Err(OmsError::OrderValidation("Invalid symbol".to_string()));
        }
        
        // Basic symbol validation (alphanumeric + some special chars)
        if !symbol.chars().all(|c| c.is_alphanumeric() || c == '.' || c == '-' || c == '_') {
            return Err(OmsError::OrderValidation("Symbol contains invalid characters".to_string()));
        }
        
        Ok(symbol.to_uppercase())
    }
    
    /// Validate quantity
    pub fn validate_quantity(quantity: f64) -> OmsResult<()> {
        if quantity <= 0.0 {
            return Err(OmsError::OrderValidation("Quantity must be positive".to_string()));
        }
        
        if quantity.is_infinite() || quantity.is_nan() {
            return Err(OmsError::OrderValidation("Invalid quantity value".to_string()));
        }
        
        Ok(())
    }
    
    /// Validate price
    pub fn validate_price(price: f64) -> OmsResult<()> {
        if price <= 0.0 {
            return Err(OmsError::OrderValidation("Price must be positive".to_string()));
        }
        
        if price.is_infinite() || price.is_nan() {
            return Err(OmsError::OrderValidation("Invalid price value".to_string()));
        }
        
        Ok(())
    }
    
    /// Convert timestamp to milliseconds since epoch
    pub fn timestamp_to_millis(timestamp: DateTime<Utc>) -> u64 {
        timestamp.timestamp_millis() as u64
    }
    
    /// Convert milliseconds since epoch to timestamp
    pub fn millis_to_timestamp(millis: u64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis as i64).unwrap_or_else(|| Utc::now())
    }
    
    /// Calculate percentage
    pub fn calculate_percentage(value: f64, total: f64) -> f64 {
        if total == 0.0 {
            0.0
        } else {
            (value / total) * 100.0
        }
    }
}

/// Constants used throughout the library
pub mod constants {
    //! System-wide constants
    
    /// Maximum number of orders per second per client (default)
    pub const DEFAULT_MAX_ORDERS_PER_SECOND: u32 = 1000;
    
    /// Default order size limit
    pub const DEFAULT_MAX_ORDER_SIZE: f64 = 1_000_000.0;
    
    /// Default heartbeat interval for FIX (seconds)
    pub const DEFAULT_FIX_HEARTBEAT: u64 = 30;
    
    /// Maximum symbol length
    pub const MAX_SYMBOL_LENGTH: usize = 20;
    
    /// Maximum account ID length  
    pub const MAX_ACCOUNT_ID_LENGTH: usize = 50;
    
    /// Default request timeout (seconds)
    pub const DEFAULT_REQUEST_TIMEOUT: u64 = 30;
    
    /// Maximum message size (bytes)
    pub const MAX_MESSAGE_SIZE: usize = 1024 * 1024; // 1MB
    
    /// Minimum price tick
    pub const MIN_PRICE_TICK: f64 = 0.0001;
    
    /// Maximum price value
    pub const MAX_PRICE_VALUE: f64 = 999_999.999;
    
    /// Maximum quantity value
    pub const MAX_QUANTITY_VALUE: f64 = 1_000_000_000.0;
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_config() {
        let config = OmsConfig::default();
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 8080);
        assert!(config.trading.enable_risk_checks);
        assert!(!config.fix.enabled);
    }
    
    #[test]
    fn test_version_constants() {
        assert!(!VERSION.is_empty());
        assert!(!NAME.is_empty());
        assert!(!DESCRIPTION.is_empty());
    }
    
    #[test]
    fn test_utils_generate_client_order_id() {
        let id = utils::generate_client_order_id("TEST");
        assert!(id.starts_with("TEST_"));
        assert!(id.len() > 5);
    }
    
    #[test]
    fn test_utils_calculate_order_value() {
        assert_eq!(utils::calculate_order_value(100.0, Some(50.0)), Some(5000.0));
        assert_eq!(utils::calculate_order_value(100.0, None), None);
    }
    
    #[test]
    fn test_utils_validate_quantity() {
        assert!(utils::validate_quantity(100.0).is_ok());
        assert!(utils::validate_quantity(-100.0).is_err());
        assert!(utils::validate_quantity(0.0).is_err());
        assert!(utils::validate_quantity(f64::NAN).is_err());
    }
    
    #[test]
    fn test_utils_validate_price() {
        assert!(utils::validate_price(100.0).is_ok());
        assert!(utils::validate_price(-100.0).is_err());
        assert!(utils::validate_price(0.0).is_err());
        assert!(utils::validate_price(f64::INFINITY).is_err());
    }
    
    #[test]
    fn test_utils_parse_symbol() {
        assert_eq!(utils::parse_symbol("aapl").unwrap(), "AAPL");
        assert_eq!(utils::parse_symbol("SPY").unwrap(), "SPY");
        assert_eq!(utils::parse_symbol("BRK.A").unwrap(), "BRK.A");
        assert!(utils::parse_symbol("").is_err());
        assert!(utils::parse_symbol("A".repeat(25).as_str()).is_err());
        assert!(utils::parse_symbol("TEST@123").is_err());
    }
}
