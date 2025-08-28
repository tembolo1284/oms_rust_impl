// src/engine/validator.rs - Order Validation Engine
//! Comprehensive order validation system with business rules enforcement
//!
//! This module provides multi-layered validation for incoming orders:
//! - **Syntactic Validation**: Field format, data type, and range checks
//! - **Semantic Validation**: Business rule enforcement and logical consistency
//! - **Market Validation**: Symbol existence, trading hours, and market status
//! - **Account Validation**: Account status, permissions, and buying power
//! - **Risk Validation**: Position limits, order size, and concentration checks
//!
//! ## Validation Pipeline
//!
//! ```text
//! Order Input
//!     │
//!     ▼
//! ┌─────────────────┐    ❌ Invalid Format
//! │ Syntactic       │────────────────────►
//! │ Validation      │
//! └─────────┬───────┘
//!           │ ✅ Valid
//!           ▼
//! ┌─────────────────┐    ❌ Business Rule
//! │ Semantic        │────────────────────►
//! │ Validation      │
//! └─────────┬───────┘
//!           │ ✅ Valid
//!           ▼
//! ┌─────────────────┐    ❌ Market Closed
//! │ Market          │────────────────────►
//! │ Validation      │
//! └─────────┬───────┘
//!           │ ✅ Valid
//!           ▼
//! ┌─────────────────┐    ❌ Insufficient Funds
//! │ Account         │────────────────────►
//! │ Validation      │
//! └─────────┬───────┘
//!           │ ✅ Valid
//!           ▼
//! ┌─────────────────┐    ❌ Risk Limit
//! │ Risk            │────────────────────►
//! │ Validation      │
//! └─────────┬───────┘
//!           │ ✅ All Valid
//!           ▼
//!      Order Accepted
//! ```

use anyhow::{anyhow, Result};
use chrono::{DateTime, Datelike, NaiveTime, Timelike, Utc, Weekday};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tracing::{debug, error, info, instrument, warn};

use crate::core::{
    order::{Order, OrderSide, OrderStatus, OrderType},
    types::{ClientId, OrderId, Symbol},
};

// Constants for validation
const MAX_SYMBOL_LENGTH: usize = 12;
const MAX_CLIENT_ID_LENGTH: usize = 64;
const MAX_PRICE_VALUE: f64 = 999999.99;

// Type aliases for clarity
pub type Price = f64;
pub type Quantity = f64;

/// Account status for validation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountStatus {
    Active,
    Suspended,
    Closed,
    Restricted,
    PendingApproval,
}

/// Account type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountType {
    Cash,
    Margin,
    Portfolio,
}

/// Account information for validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: ClientId,
    pub status: AccountStatus,
    pub account_type: AccountType,
    pub buying_power: f64,
    pub cash_balance: f64,
}

/// Position information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: Symbol,
    pub quantity: Quantity,
    pub average_cost: Price,
    pub market_value: f64,
}

/// Risk limits for an account
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskLimits {
    pub max_order_size: Quantity,
    pub max_position_size: Quantity,
    pub max_order_value: f64,
    pub max_daily_volume: f64,
    pub enable_position_limits: bool,
    pub fat_finger_threshold: Option<f64>,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_order_size: 100_000.0,
            max_position_size: 1_000_000.0,
            max_order_value: 1_000_000.0,
            max_daily_volume: 10_000_000.0,
            enable_position_limits: true,
            fat_finger_threshold: Some(100_000.0),
        }
    }
}

/// Market data for validation
#[derive(Debug, Clone)]
pub struct MarketData {
    pub symbol: Symbol,
    pub last_price: Option<Price>,
    pub bid: Option<Price>,
    pub ask: Option<Price>,
    pub volume: Quantity,
    pub session_status: SessionStatus,
    pub volatility: Option<f64>,
    pub avg_daily_volume: Option<Quantity>,
}

/// Trading session status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionStatus {
    PreMarket,
    Open,
    PostMarket,
    Closed,
    Halted,
}

/// Validation rule configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConfig {
    /// Enable strict validation mode
    pub strict_mode: bool,
    /// Maximum order value allowed
    pub max_order_value: Price,
    /// Minimum order size
    pub min_order_size: Quantity,
    /// Maximum order size
    pub max_order_size: Quantity,
    /// Maximum price deviation from market (percentage)
    pub max_price_deviation_pct: f64,
    /// Enable market hours validation
    pub enforce_market_hours: bool,
    /// Enable account validation
    pub enable_account_validation: bool,
    /// Enable symbol validation
    pub enable_symbol_validation: bool,
    /// Enable price validation against market data
    pub enable_price_validation: bool,
    /// Enable duplicate order detection
    pub enable_duplicate_detection: bool,
    /// Duplicate detection window in seconds
    pub duplicate_window_seconds: u64,
    /// Maximum orders per second per account
    pub max_orders_per_second: u32,
    /// Rate limiting window in seconds
    pub rate_limit_window_seconds: u64,
    /// Enable fat finger checks
    pub enable_fat_finger_checks: bool,
    /// Fat finger threshold multiplier
    pub fat_finger_multiplier: f64,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            strict_mode: true,
            max_order_value: 10_000_000.0,
            min_order_size: 1.0,
            max_order_size: 1_000_000.0,
            max_price_deviation_pct: 10.0,
            enforce_market_hours: true,
            enable_account_validation: true,
            enable_symbol_validation: true,
            enable_price_validation: true,
            enable_duplicate_detection: true,
            duplicate_window_seconds: 5,
            max_orders_per_second: 100,
            rate_limit_window_seconds: 1,
            enable_fat_finger_checks: true,
            fat_finger_multiplier: 5.0,
        }
    }
}

/// Validation result with detailed error information
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationResult {
    /// Validation passed
    Valid,
    /// Validation failed with error details
    Invalid {
        error_code: String,
        message: String,
        field: Option<String>,
        suggestion: Option<String>,
    },
    /// Validation warning (order can proceed but with caution)
    Warning {
        warning_code: String,
        message: String,
        field: Option<String>,
    },
}

impl ValidationResult {
    pub fn is_valid(&self) -> bool {
        matches!(self, Self::Valid | Self::Warning { .. })
    }

    pub fn is_invalid(&self) -> bool {
        matches!(self, Self::Invalid { .. })
    }

    pub fn is_warning(&self) -> bool {
        matches!(self, Self::Warning { .. })
    }

    pub fn to_result(self) -> Result<()> {
        match self {
            Self::Valid | Self::Warning { .. } => Ok(()),
            Self::Invalid { message, .. } => Err(anyhow!("Validation failed: {}", message)),
        }
    }

    pub fn invalid(error_code: &str, message: &str) -> Self {
        Self::Invalid {
            error_code: error_code.to_string(),
            message: message.to_string(),
            field: None,
            suggestion: None,
        }
    }

    pub fn invalid_field(error_code: &str, message: &str, field: &str) -> Self {
        Self::Invalid {
            error_code: error_code.to_string(),
            message: message.to_string(),
            field: Some(field.to_string()),
            suggestion: None,
        }
    }

    pub fn warning(warning_code: &str, message: &str) -> Self {
        Self::Warning {
            warning_code: warning_code.to_string(),
            message: message.to_string(),
            field: None,
        }
    }
}

/// Order rate tracking for rate limiting
#[derive(Debug, Clone)]
struct RateTracker {
    timestamps: Vec<Instant>,
    last_cleanup: Instant,
}

impl RateTracker {
    fn new() -> Self {
        Self {
            timestamps: Vec::new(),
            last_cleanup: Instant::now(),
        }
    }

    fn check_rate(&mut self, max_orders: u32, window_seconds: u64) -> bool {
        let now = Instant::now();
        let window_duration = Duration::from_secs(window_seconds);

        // Cleanup old timestamps periodically
        if now.duration_since(self.last_cleanup) > window_duration {
            let cutoff = now - window_duration;
            self.timestamps.retain(|&t| t > cutoff);
            self.last_cleanup = now;
        }

        // Check current rate
        let cutoff = now - window_duration;
        let current_count = self.timestamps.iter().filter(|&&t| t > cutoff).count();

        if current_count >= max_orders as usize {
            false // Rate limit exceeded
        } else {
            self.timestamps.push(now);
            true
        }
    }
}

/// Duplicate order detection entry
#[derive(Debug, Clone)]
struct DuplicateEntry {
    hash: u64,
    timestamp: Instant,
}

/// Main order validation engine
pub struct OrderValidator {
    /// Validation configuration
    config: Arc<ValidationConfig>,
    /// Account information cache
    accounts: Arc<DashMap<ClientId, Account>>,
    /// Position cache
    positions: Arc<DashMap<(ClientId, Symbol), Position>>,
    /// Risk limits cache
    risk_limits: Arc<DashMap<ClientId, RiskLimits>>,
    /// Market data cache
    market_data: Arc<DashMap<Symbol, MarketData>>,
    /// Valid symbols set
    valid_symbols: Arc<RwLock<HashSet<Symbol>>>,
    /// Rate limiting tracker
    rate_trackers: Arc<DashMap<ClientId, RateTracker>>,
    /// Duplicate order detection
    duplicate_cache: Arc<RwLock<HashMap<ClientId, Vec<DuplicateEntry>>>>,
    /// Validation statistics
    stats: Arc<ValidationStats>,
}

/// Validation statistics
#[derive(Debug, Default)]
pub struct ValidationStats {
    pub total_validations: AtomicU64,
    pub valid_orders: AtomicU64,
    pub invalid_orders: AtomicU64,
    pub warnings_issued: AtomicU64,
    pub error_counts: DashMap<String, AtomicU64>,
    pub avg_validation_time_us: RwLock<f64>,
}

impl OrderValidator {
    /// Create new order validator
    pub fn new(config: ValidationConfig) -> Self {
        info!("Initializing Order Validator");

        Self {
            config: Arc::new(config),
            accounts: Arc::new(DashMap::new()),
            positions: Arc::new(DashMap::new()),
            risk_limits: Arc::new(DashMap::new()),
            market_data: Arc::new(DashMap::new()),
            valid_symbols: Arc::new(RwLock::new(HashSet::new())),
            rate_trackers: Arc::new(DashMap::new()),
            duplicate_cache: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(ValidationStats::default()),
        }
    }

    /// Validate an order through the complete validation pipeline
    #[instrument(skip(self, order))]
    pub async fn validate_order(&self, order: &Order) -> ValidationResult {
        let start_time = Instant::now();

        debug!("Validating order: {} for client: {}", order.id, order.client_id);

        self.stats.total_validations.fetch_add(1, Ordering::Relaxed);

        // Run validation pipeline
        let result = self.run_validation_pipeline(order).await;

        // Update statistics
        match &result {
            ValidationResult::Valid => {
                self.stats.valid_orders.fetch_add(1, Ordering::Relaxed);
            }
            ValidationResult::Invalid { error_code, .. } => {
                self.stats.invalid_orders.fetch_add(1, Ordering::Relaxed);
                self.stats.error_counts
                    .entry(error_code.clone())
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(1, Ordering::Relaxed);
            }
            ValidationResult::Warning { .. } => {
                self.stats.valid_orders.fetch_add(1, Ordering::Relaxed);
                self.stats.warnings_issued.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Update average validation time
        let validation_time_us = start_time.elapsed().as_micros() as f64;
        let mut avg_time = self.stats.avg_validation_time_us.write();
        *avg_time = (*avg_time * 0.95) + (validation_time_us * 0.05);

        debug!("Validation result for order {}: {:?}", order.id, result);
        result
    }

    /// Run the complete validation pipeline
    async fn run_validation_pipeline(&self, order: &Order) -> ValidationResult {
        // 1. Syntactic Validation
        if let result @ ValidationResult::Invalid { .. } = self.validate_syntax(order).await {
            return result;
        }

        // 2. Semantic Validation
        if let result @ ValidationResult::Invalid { .. } = self.validate_semantics(order).await {
            return result;
        }

        // 3. Market Validation
        if self.config.enable_symbol_validation || self.config.enforce_market_hours {
            if let result @ ValidationResult::Invalid { .. } = self.validate_market(order).await {
                return result;
            }
        }

        // 4. Account Validation
        if self.config.enable_account_validation {
            if let result @ ValidationResult::Invalid { .. } = self.validate_account(order).await {
                return result;
            }
        }

        // 5. Risk Validation
        if let result @ ValidationResult::Invalid { .. } = self.validate_risk(order).await {
            return result;
        }

        // 6. Rate Limiting
        if let result @ ValidationResult::Invalid { .. } = self.validate_rate_limits(order).await {
            return result;
        }

        // 7. Duplicate Detection
        if self.config.enable_duplicate_detection {
            if let result @ ValidationResult::Invalid { .. } = self.validate_duplicates(order).await {
                return result;
            }
        }

        // 8. Price Validation (may return warnings)
        if self.config.enable_price_validation {
            if let result = self.validate_price(order).await {
                if result.is_invalid() {
                    return result;
                }
                // Return warning if present
                if result.is_warning() {
                    return result;
                }
            }
        }

        ValidationResult::Valid
    }

    /// Validate order syntax and format
    async fn validate_syntax(&self, order: &Order) -> ValidationResult {
        // Validate symbol
        if order.symbol.0.is_empty() {
            return ValidationResult::invalid_field("EMPTY_SYMBOL", "Symbol cannot be empty", "symbol");
        }

        if order.symbol.0.len() > MAX_SYMBOL_LENGTH {
            return ValidationResult::invalid_field(
                "SYMBOL_TOO_LONG",
                &format!("Symbol length exceeds maximum of {}", MAX_SYMBOL_LENGTH),
                "symbol",
            );
        }

        // Validate client ID
        if order.client_id.0.is_empty() {
            return ValidationResult::invalid_field("EMPTY_CLIENT_ID", "Client ID cannot be empty", "client_id");
        }

        if order.client_id.0.len() > MAX_CLIENT_ID_LENGTH {
            return ValidationResult::invalid_field(
                "CLIENT_ID_TOO_LONG",
                &format!("Client ID length exceeds maximum of {}", MAX_CLIENT_ID_LENGTH),
                "client_id",
            );
        }

        // Validate quantity
        if order.quantity <= 0.0 {
            return ValidationResult::invalid_field("INVALID_QUANTITY", "Quantity must be positive", "quantity");
        }

        if order.quantity.is_infinite() || order.quantity.is_nan() {
            return ValidationResult::invalid_field("INVALID_QUANTITY", "Quantity must be finite", "quantity");
        }

        if order.quantity < self.config.min_order_size {
            return ValidationResult::invalid_field(
                "QUANTITY_TOO_SMALL",
                &format!("Quantity {} is below minimum of {}", order.quantity, self.config.min_order_size),
                "quantity",
            );
        }

        if order.quantity > self.config.max_order_size {
            return ValidationResult::invalid_field(
                "QUANTITY_TOO_LARGE",
                &format!("Quantity {} exceeds maximum of {}", order.quantity, self.config.max_order_size),
                "quantity",
            );
        }

        // Validate prices based on order type
        match order.order_type {
            OrderType::Limit { price } | OrderType::StopLimit { limit_price: price, .. } => {
                if price <= 0.0 || price.is_infinite() || price.is_nan() {
                    return ValidationResult::invalid_field("INVALID_PRICE", "Price must be positive and finite", "price");
                }
                if price > MAX_PRICE_VALUE {
                    return ValidationResult::invalid_field(
                        "PRICE_TOO_LARGE",
                        &format!("Price {} exceeds maximum of {}", price, MAX_PRICE_VALUE),
                        "price",
                    );
                }
            }
            OrderType::Stop { stop_price } | OrderType::StopLimit { stop_price, .. } => {
                if stop_price <= 0.0 || stop_price.is_infinite() || stop_price.is_nan() {
                    return ValidationResult::invalid_field("INVALID_STOP_PRICE", "Stop price must be positive and finite", "stop_price");
                }
            }
            _ => {}
        }

        ValidationResult::Valid
    }

    /// Validate semantic business rules
    async fn validate_semantics(&self, order: &Order) -> ValidationResult {
        // Validate stop-limit order logic
        if let OrderType::StopLimit { stop_price, limit_price } = order.order_type {
            match order.side {
                OrderSide::Buy => {
                    if stop_price < limit_price {
                        return ValidationResult::invalid(
                            "INVALID_STOP_LIMIT_PRICES",
                            "For buy stop-limit orders, stop price must be >= limit price",
                        );
                    }
                }
                OrderSide::Sell => {
                    if stop_price > limit_price {
                        return ValidationResult::invalid(
                            "INVALID_STOP_LIMIT_PRICES",
                            "For sell stop-limit orders, stop price must be <= limit price",
                        );
                    }
                }
            }
        }

        // Validate order value if applicable
        if let Some(order_value) = self.calculate_order_value(order) {
            if order_value > self.config.max_order_value {
                return ValidationResult::invalid(
                    "ORDER_VALUE_TOO_LARGE",
                    &format!(
                        "Order value ${:.2} exceeds maximum of ${:.2}",
                        order_value, self.config.max_order_value
                    ),
                );
            }
        }

        ValidationResult::Valid
    }

    /// Validate market conditions and trading hours
    async fn validate_market(&self, order: &Order) -> ValidationResult {
        // Validate symbol exists
        if self.config.enable_symbol_validation {
            let valid_symbols = self.valid_symbols.read();
            if !valid_symbols.contains(&order.symbol) {
                // If no symbols loaded, just warn
                if valid_symbols.is_empty() {
                    return ValidationResult::warning(
                        "NO_SYMBOL_DATA",
                        "Symbol validation data not loaded",
                    );
                }
                
                return ValidationResult::invalid_field(
                    "INVALID_SYMBOL",
                    &format!("Symbol '{}' is not valid or not supported", order.symbol),
                    "symbol",
                );
            }
        }

        // Validate market hours
        if self.config.enforce_market_hours {
            if let Some(market_data) = self.market_data.get(&order.symbol) {
                match market_data.session_status {
                    SessionStatus::Closed => {
                        return ValidationResult::invalid(
                            "MARKET_CLOSED",
                            "Market is closed for trading",
                        );
                    }
                    SessionStatus::Halted => {
                        return ValidationResult::invalid(
                            "MARKET_HALTED",
                            "Trading is halted for this symbol",
                        );
                    }
                    SessionStatus::PreMarket | SessionStatus::PostMarket => {
                        return ValidationResult::warning(
                            "EXTENDED_HOURS",
                            "Order submitted during extended trading hours",
                        );
                    }
                    SessionStatus::Open => {
                        // Regular trading hours - all good
                    }
                }
            } else {
                // No market data available - check time
                let status = get_session_status(Utc::now());
                match status {
                    SessionStatus::Closed => {
                        return ValidationResult::invalid(
                            "MARKET_CLOSED",
                            "Market is closed for trading",
                        );
                    }
                    _ => {}
                }
            }
        }

        ValidationResult::Valid
    }

    /// Validate account status and permissions
    async fn validate_account(&self, order: &Order) -> ValidationResult {
        let account = match self.accounts.get(&order.client_id) {
            Some(acc) => acc.clone(),
            None => {
                // No account data loaded - warn but allow
                return ValidationResult::warning(
                    "ACCOUNT_NOT_FOUND",
                    &format!("Account '{}' not found in cache", order.client_id),
                );
            }
        };

        // Check account status
        match account.status {
            AccountStatus::Active => {}
            AccountStatus::Suspended => {
                return ValidationResult::invalid(
                    "ACCOUNT_SUSPENDED",
                    "Account is suspended",
                );
            }
            AccountStatus::Closed => {
                return ValidationResult::invalid(
                    "ACCOUNT_CLOSED",
                    "Account is closed",
                );
            }
            AccountStatus::Restricted => {
                return ValidationResult::warning(
                    "ACCOUNT_RESTRICTED",
                    "Account has trading restrictions",
                );
            }
            AccountStatus::PendingApproval => {
                return ValidationResult::invalid(
                    "ACCOUNT_PENDING",
                    "Account is pending approval",
                );
            }
        }

        // Check buying power for buy orders
        if order.side == OrderSide::Buy {
            if let Some(order_value) = self.calculate_order_value(order) {
                if order_value > account.buying_power {
                    return ValidationResult::invalid(
                        "INSUFFICIENT_BUYING_POWER",
                        &format!(
                            "Order value ${:.2} exceeds available buying power ${:.2}",
                            order_value, account.buying_power
                        ),
                    );
                }
            }
        }

        // Check position for sell orders
        if order.side == OrderSide::Sell {
            if let Some(position) = self.positions.get(&(order.client_id.clone(), order.symbol.clone())) {
                if position.quantity < order.quantity {
                    if account.account_type == AccountType::Cash {
                        return ValidationResult::invalid(
                            "INSUFFICIENT_SHARES",
                            &format!(
                                "Cannot sell {} shares, only {} shares available",
                                order.quantity, position.quantity
                            ),
                        );
                    }
                    // Margin account - allow short selling with warning
                    return ValidationResult::warning(
                        "SHORT_SALE",
                        "Order will create or increase short position",
                    );
                }
            } else if account.account_type == AccountType::Cash {
                // No position data - warn but allow
                return ValidationResult::warning(
                    "NO_POSITION_DATA",
                    "Position data not available for validation",
                );
            }
        }

        ValidationResult::Valid
    }

    /// Validate risk limits and constraints
    async fn validate_risk(&self, order: &Order) -> ValidationResult {
        let risk_limits = match self.risk_limits.get(&order.client_id) {
            Some(limits) => limits.clone(),
            None => {
                // No specific limits defined - use defaults
                return ValidationResult::Valid;
            }
        };

        // Check maximum order size
        if order.quantity > risk_limits.max_order_size {
            return ValidationResult::invalid(
                "ORDER_SIZE_LIMIT_EXCEEDED",
                &format!(
                    "Order size {} exceeds limit of {}",
                    order.quantity, risk_limits.max_order_size
                ),
            );
        }

        // Check position limits
        if risk_limits.enable_position_limits {
            let current_position = self.positions
                .get(&(order.client_id.clone(), order.symbol.clone()))
                .map(|p| p.quantity)
                .unwrap_or(0.0);

            let projected_position = match order.side {
                OrderSide::Buy => current_position + order.quantity,
                OrderSide::Sell => current_position - order.quantity,
            };

            if projected_position.abs() > risk_limits.max_position_size {
                return ValidationResult::invalid(
                    "POSITION_LIMIT_EXCEEDED",
                    &format!(
                        "Projected position {} would exceed limit of {}",
                        projected_position, risk_limits.max_position_size
                    ),
                );
            }
        }

        // Check fat finger protection
        if self.config.enable_fat_finger_checks {
            if let Some(threshold) = risk_limits.fat_finger_threshold {
                if let Some(order_value) = self.calculate_order_value(order) {
                    if order_value > threshold {
                        return ValidationResult::warning(
                            "FAT_FINGER_CHECK",
                            &format!(
                                "Order value ${:.2} exceeds fat finger threshold ${:.2}",
                                order_value, threshold
                            ),
                        );
                    }
                }
            }
        }

        ValidationResult::Valid
    }

    /// Validate order rate limits
    async fn validate_rate_limits(&self, order: &Order) -> ValidationResult {
        let mut rate_tracker = self.rate_trackers
            .entry(order.client_id.clone())
            .or_insert_with(RateTracker::new);

        if !rate_tracker.check_rate(
            self.config.max_orders_per_second,
            self.config.rate_limit_window_seconds,
        ) {
            return ValidationResult::invalid(
                "RATE_LIMIT_EXCEEDED",
                &format!(
                    "Rate limit of {} orders per {} seconds exceeded",
                    self.config.max_orders_per_second,
                    self.config.rate_limit_window_seconds
                ),
            );
        }

        ValidationResult::Valid
    }

    /// Validate for duplicate orders
    async fn validate_duplicates(&self, order: &Order) -> ValidationResult {
        let order_hash = self.calculate_order_hash(order);
        let mut duplicate_cache = self.duplicate_cache.write();

        let account_entries = duplicate_cache
            .entry(order.client_id.clone())
            .or_insert_with(Vec::new);

        // Clean old entries
        let cutoff = Instant::now() - Duration::from_secs(self.config.duplicate_window_seconds);
        account_entries.retain(|entry| entry.timestamp > cutoff);

        // Check for duplicates
        if account_entries.iter().any(|entry| entry.hash == order_hash) {
            return ValidationResult::invalid(
                "DUPLICATE_ORDER",
                "Duplicate order detected within time window",
            );
        }

        // Add current order to cache
        account_entries.push(DuplicateEntry {
            hash: order_hash,
            timestamp: Instant::now(),
        });

        ValidationResult::Valid
    }

    /// Validate order price against market data
    async fn validate_price(&self, order: &Order) -> ValidationResult {
        let order_price = match order.order_type {
            OrderType::Limit { price } => price,
            OrderType::StopLimit { limit_price, .. } => limit_price,
            _ => return ValidationResult::Valid, // No price to validate
        };

        if let Some(market_data) = self.market_data.get(&order.symbol) {
            let reference_price = market_data.last_price
                .or(market_data.bid)
                .or(market_data.ask)
                .unwrap_or(order_price);

            let deviation = ((order_price - reference_price).abs() / reference_price) * 100.0;

            if deviation > self.config.max_price_deviation_pct {
                return ValidationResult::warning(
                    "PRICE_DEVIATION",
                    &format!(
                        "Order price ${:.2} deviates {:.2}% from market price ${:.2}",
                        order_price, deviation, reference_price
                    ),
                );
            }
        }

        ValidationResult::Valid
    }

    /// Calculate order value
    fn calculate_order_value(&self, order: &Order) -> Option<f64> {
        let price = match order.order_type {
            OrderType::Limit { price } => Some(price),
            OrderType::StopLimit { limit_price, .. } => Some(limit_price),
            OrderType::Market | OrderType::Stop { .. } => {
                // Use market price if available
                self.market_data
                    .get(&order.symbol)
                    .and_then(|data| data.last_price)
            }
        };

        price.map(|p| p * order.quantity)
    }

    /// Calculate hash for duplicate detection
    fn calculate_order_hash(&self, order: &Order) -> u64 {
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        order.symbol.hash(&mut hasher);
        order.side.hash(&mut hasher);
        order.quantity.to_bits().hash(&mut hasher);
        
        // Include price in hash if present
        match order.order_type {
            OrderType::Limit { price } => price.to_bits().hash(&mut hasher),
            OrderType::StopLimit { limit_price, stop_price } => {
                limit_price.to_bits().hash(&mut hasher);
                stop_price.to_bits().hash(&mut hasher);
            }
            OrderType::Stop { stop_price } => stop_price.to_bits().hash(&mut hasher),
            _ => {}
        }
        
        order.client_id.hash(&mut hasher);
        hasher.finish()
    }

    /// Update account information
    pub fn update_account(&self, account: Account) {
        self.accounts.insert(account.id.clone(), account);
    }

    /// Update position information
    pub fn update_position(&self, client_id: ClientId, symbol: Symbol, position: Position) {
        self.positions.insert((client_id, symbol), position);
    }

    /// Update risk limits
    pub fn update_risk_limits(&self, client_id: ClientId, limits: RiskLimits) {
        self.risk_limits.insert(client_id, limits);
    }

    /// Update market data
    pub fn update_market_data(&self, symbol: Symbol, data: MarketData) {
        self.market_data.insert(symbol, data);
    }

    /// Add valid symbol
    pub fn add_valid_symbol(&self, symbol: Symbol) {
        self.valid_symbols.write().insert(symbol);
    }

    /// Remove valid symbol
    pub fn remove_valid_symbol(&self, symbol: &Symbol) {
        self.valid_symbols.write().remove(symbol);
    }

    /// Get validation statistics
    pub fn get_stats(&self) -> &ValidationStats {
        &self.stats
    }
}

/// Utility function to determine market session status
pub fn get_session_status(timestamp: DateTime<Utc>) -> SessionStatus {
    // Check if it's a weekday
    match timestamp.weekday() {
        Weekday::Sat | Weekday::Sun => return SessionStatus::Closed,
        _ => {}
    }

    // Convert to Eastern time for US markets
    let hour = timestamp.hour();
    
    // Rough approximation (would need proper timezone handling in production)
    // Market hours: 9:30 AM - 4:00 PM ET (14:30 - 21:00 UTC approximately)
    if hour >= 14 && hour < 21 {
        SessionStatus::Open
    } else if hour >= 8 && hour < 14 {
        SessionStatus::PreMarket
    } else if hour >= 21 && hour < 24 {
        SessionStatus::PostMarket
    } else {
        SessionStatus::Closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::order::{OrderSide, OrderType};

    #[tokio::test]
    async fn test_validator_creation() {
        let config = ValidationConfig::default();
        let validator = OrderValidator::new(config);
        assert!(validator.config.strict_mode);
    }

    #[tokio::test]
    async fn test_syntax_validation() {
        let config = ValidationConfig::default();
        let validator = OrderValidator::new(config);

        // Valid order
        let valid_order = Order::new(
            "CLIENT_1".into(),
            "AAPL".into(),
            100.0,
            OrderSide::Buy,
            OrderType::Limit { price: 150.0 },
        );

        let result = validator.validate_syntax(&valid_order).await;
        assert!(result.is_valid());

        // Invalid order (empty symbol)
        let mut invalid_order = valid_order.clone();
        invalid_order.symbol = Symbol("".to_string());

        let result = validator.validate_syntax(&invalid_order).await;
        assert!(result.is_invalid());
    }

    #[tokio::test]
    async fn test_semantic_validation() {
        let config = ValidationConfig::default();
        let validator = OrderValidator::new(config);

        // Stop-limit order with invalid prices
        let order = Order::new(
            "CLIENT_1".into(),
            "AAPL".into(),
            100.0,
            OrderSide::Buy,
            OrderType::StopLimit {
                stop_price: 145.0,
                limit_price: 150.0,
            },
        );

        let result = validator.validate_semantics(&order).await;
        assert!(result.is_invalid()); // Buy stop-limit: stop should be >= limit
    }

    #[tokio::test]
    async fn test_rate_limiting() {
        let mut config = ValidationConfig::default();
        config.max_orders_per_second = 2;
        config.rate_limit_window_seconds = 1;

        let validator = OrderValidator::new(config);

        let order = Order::new(
            "RATE_TEST".into(),
            "AAPL".into(),
            100.0,
            OrderSide::Buy,
            OrderType::Market,
        );

        // First two orders should pass
        assert!(validator.validate_rate_limits(&order).await.is_valid());
        assert!(validator.validate_rate_limits(&order).await.is_valid());

        // Third order should fail
        assert!(validator.validate_rate_limits(&order).await.is_invalid());
    }

    #[tokio::test]
    async fn test_duplicate_detection() {
        let mut config = ValidationConfig::default();
        config.enable_duplicate_detection = true;
        config.duplicate_window_seconds = 5;

        let validator = OrderValidator::new(config);

        let order = Order::new(
            "DUP_TEST".into(),
            "AAPL".into(),
            100.0,
            OrderSide::Buy,
            OrderType::Limit { price: 150.0 },
        );

        // First submission should pass
        assert!(validator.validate_duplicates(&order).await.is_valid());

        // Immediate duplicate should fail
        assert!(validator.validate_duplicates(&order).await.is_invalid());
    }

    #[test]
    fn test_session_status() {
        use chrono::TimeZone;

        // Test regular market hours (3 PM UTC = ~10 AM ET)
        let market_hours = Utc.with_ymd_and_hms(2024, 1, 3, 15, 0, 0).unwrap();
        assert_eq!(get_session_status(market_hours), SessionStatus::Open);

        // Test weekend
        let weekend = Utc.with_ymd_and_hms(2024, 1, 6, 15, 0, 0).unwrap();
        assert_eq!(get_session_status(weekend), SessionStatus::Closed);
    }
}
