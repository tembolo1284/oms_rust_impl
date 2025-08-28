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

use crate::{
    core::{
        order::{Order, OrderSide, OrderStatus, OrderType, TimeInForce},
        types::{
            Account, AccountId, AccountStatus, AccountType, MarketTick, Money, Position, Price,
            Quantity, RiskLimits, Symbol, Timestamp,
        },
    },
    constants, utils, OmsError, OmsResult,
};
use chrono::{DateTime, Datelike, NaiveTime, Timelike, Utc, Weekday};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tracing::{debug, error, info, instrument, warn};

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
            max_order_value: 10_000_000.0, // $10M
            min_order_size: 1.0,
            max_order_size: 1_000_000.0, // 1M shares
            max_price_deviation_pct: 10.0, // 10%
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
        /// Error code for programmatic handling
        error_code: String,
        /// Human-readable error message
        message: String,
        /// Field that caused the error (if applicable)
        field: Option<String>,
        /// Suggested fix (if applicable)
        suggestion: Option<String>,
    },
    /// Validation warning (order can proceed but with caution)
    Warning {
        /// Warning code
        warning_code: String,
        /// Warning message
        message: String,
        /// Field that triggered the warning
        field: Option<String>,
    },
}

impl ValidationResult {
    /// Check if validation passed
    pub fn is_valid(&self) -> bool {
        matches!(self, Self::Valid | Self::Warning { .. })
    }
    
    /// Check if validation failed
    pub fn is_invalid(&self) -> bool {
        matches!(self, Self::Invalid { .. })
    }
    
    /// Check if result is a warning
    pub fn is_warning(&self) -> bool {
        matches!(self, Self::Warning { .. })
    }
    
    /// Convert to OmsResult
    pub fn to_result(self) -> OmsResult<()> {
        match self {
            Self::Valid | Self::Warning { .. } => Ok(()),
            Self::Invalid { message, .. } => Err(OmsError::OrderValidation(message)),
        }
    }
    
    /// Create invalid result
    pub fn invalid(error_code: &str, message: &str) -> Self {
        Self::Invalid {
            error_code: error_code.to_string(),
            message: message.to_string(),
            field: None,
            suggestion: None,
        }
    }
    
    /// Create invalid result with field
    pub fn invalid_field(error_code: &str, message: &str, field: &str) -> Self {
        Self::Invalid {
            error_code: error_code.to_string(),
            message: message.to_string(),
            field: Some(field.to_string()),
            suggestion: None,
        }
    }
    
    /// Create warning result
    pub fn warning(warning_code: &str, message: &str) -> Self {
        Self::Warning {
            warning_code: warning_code.to_string(),
            message: message.to_string(),
            field: None,
        }
    }
}

/// Market data for validation
#[derive(Debug, Clone)]
pub struct ValidationMarketData {
    /// Current market tick
    pub tick: MarketTick,
    /// Trading session status
    pub session_status: SessionStatus,
    /// Market volatility (for fat finger checks)
    pub volatility: Option<f64>,
    /// Average daily volume
    pub avg_daily_volume: Option<Quantity>,
}

/// Trading session status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionStatus {
    /// Pre-market trading
    PreMarket,
    /// Regular trading session
    Open,
    /// Post-market trading
    PostMarket,
    /// Market closed
    Closed,
    /// Market halted
    Halted,
}

/// Order rate tracking for rate limiting
#[derive(Debug, Clone)]
struct RateTracker {
    /// Order timestamps within the current window
    timestamps: Vec<Instant>,
    /// Last cleanup time
    last_cleanup: Instant,
}

impl RateTracker {
    fn new() -> Self {
        Self {
            timestamps: Vec::new(),
            last_cleanup: Instant::now(),
        }
    }
    
    /// Add new order timestamp and check rate limit
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
    /// Order details hash
    hash: u64,
    /// Submission time
    timestamp: Instant,
}

/// Main order validation engine
pub struct OrderValidator {
    /// Validation configuration
    config: Arc<ValidationConfig>,
    /// Account information cache
    accounts: Arc<DashMap<AccountId, Account>>,
    /// Position cache
    positions: Arc<DashMap<(AccountId, Symbol), Position>>,
    /// Risk limits cache
    risk_limits: Arc<DashMap<AccountId, RiskLimits>>,
    /// Market data cache
    market_data: Arc<DashMap<Symbol, ValidationMarketData>>,
    /// Valid symbols set
    valid_symbols: Arc<RwLock<HashSet<Symbol>>>,
    /// Rate limiting tracker
    rate_trackers: Arc<DashMap<AccountId, RateTracker>>,
    /// Duplicate order detection
    duplicate_cache: Arc<RwLock<HashMap<AccountId, Vec<DuplicateEntry>>>>,
    /// Validation statistics
    stats: Arc<ValidationStats>,
}

/// Validation statistics
#[derive(Debug, Default)]
pub struct ValidationStats {
    /// Total validations performed
    pub total_validations: AtomicU64,
    /// Valid orders
    pub valid_orders: AtomicU64,
    /// Invalid orders
    pub invalid_orders: AtomicU64,
    /// Warnings issued
    pub warnings_issued: AtomicU64,
    /// Validation errors by type
    pub error_counts: DashMap<String, AtomicU64>,
    /// Average validation time (microseconds)
    pub avg_validation_time_us: RwLock<f64>,
}

impl OrderValidator {
    /// Create new order validator
    pub fn new(config: ValidationConfig) -> Self {
        info!("Initializing Order Validator with config: {:?}", config);
        
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
        
        debug!("Validating order: {} for account: {}", order.id, order.account);
        
        // Update statistics
        self.stats.total_validations.fetch_add(1, Ordering::Relaxed);
        
        // Run validation pipeline
        let result = self.run_validation_pipeline(order).await;
        
        // Update statistics based on result
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
            if let result @ ValidationResult::Invalid { .. } = self.validate_price(order).await {
                return result;
            }
        }
        
        ValidationResult::Valid
    }
    
    /// Validate order syntax and format
    async fn validate_syntax(&self, order: &Order) -> ValidationResult {
        // Validate symbol
        if order.symbol.is_empty() {
            return ValidationResult::invalid_field("EMPTY_SYMBOL", "Symbol cannot be empty", "symbol");
        }
        
        if order.symbol.len() > constants::MAX_SYMBOL_LENGTH {
            return ValidationResult::invalid_field(
                "SYMBOL_TOO_LONG",
                &format!("Symbol length exceeds maximum of {}", constants::MAX_SYMBOL_LENGTH),
                "symbol",
            );
        }
        
        // Validate account
        if order.account.is_empty() {
            return ValidationResult::invalid_field("EMPTY_ACCOUNT", "Account cannot be empty", "account");
        }
        
        if order.account.len() > constants::MAX_ACCOUNT_ID_LENGTH {
            return ValidationResult::invalid_field(
                "ACCOUNT_TOO_LONG",
                &format!("Account ID length exceeds maximum of {}", constants::MAX_ACCOUNT_ID_LENGTH),
                "account",
            );
        }
        
        // Validate client order ID
        if order.client_order_id.is_empty() {
            return ValidationResult::invalid_field("EMPTY_CLIENT_ORDER_ID", "Client order ID cannot be empty", "client_order_id");
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
        
        // Validate prices if present
        if let Some(price) = order.price {
            if price <= 0.0 || price.is_infinite() || price.is_nan() {
                return ValidationResult::invalid_field("INVALID_PRICE", "Price must be positive and finite", "price");
            }
            
            if price > constants::MAX_PRICE_VALUE {
                return ValidationResult::invalid_field(
                    "PRICE_TOO_LARGE",
                    &format!("Price {} exceeds maximum of {}", price, constants::MAX_PRICE_VALUE),
                    "price",
                );
            }
        }
        
        if let Some(stop_price) = order.stop_price {
            if stop_price <= 0.0 || stop_price.is_infinite() || stop_price.is_nan() {
                return ValidationResult::invalid_field("INVALID_STOP_PRICE", "Stop price must be positive and finite", "stop_price");
            }
        }
        
        ValidationResult::Valid
    }
    
    /// Validate semantic business rules
    async fn validate_semantics(&self, order: &Order) -> ValidationResult {
        // Validate order type requirements
        if order.order_type.requires_limit_price() && order.price.is_none() {
            return ValidationResult::invalid(
                "MISSING_LIMIT_PRICE",
                &format!("Order type {:?} requires a limit price", order.order_type),
            );
        }
        
        if order.order_type.requires_stop_price() && order.stop_price.is_none() {
            return ValidationResult::invalid(
                "MISSING_STOP_PRICE",
                &format!("Order type {:?} requires a stop price", order.order_type),
            );
        }
        
        // Validate stop-limit order logic
        if order.order_type == OrderType::StopLimit {
            if let (Some(stop_price), Some(limit_price)) = (order.stop_price, order.price) {
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
        }
        
        // Validate time in force compatibility
        if matches!(order.time_in_force, TimeInForce::GoodTilDate) && order.expire_time.is_none() {
            return ValidationResult::invalid(
                "MISSING_EXPIRE_TIME",
                "GTD orders require an expiry time",
            );
        }
        
        // Validate order value if applicable
        if let Some(order_value) = utils::calculate_order_value(order.quantity, order.price) {
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
                        // Allow with warning for extended hours
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
                // No market data available - proceed with warning
                return ValidationResult::warning(
                    "NO_MARKET_DATA",
                    "No market data available for validation",
                );
            }
        }
        
        ValidationResult::Valid
    }
    
    /// Validate account status and permissions
    async fn validate_account(&self, order: &Order) -> ValidationResult {
        let account = match self.accounts.get(&order.account) {
            Some(acc) => acc.clone(),
            None => {
                return ValidationResult::invalid(
                    "ACCOUNT_NOT_FOUND",
                    &format!("Account '{}' not found", order.account),
                );
            }
        };
        
        // Check account status
        match account.status {
            AccountStatus::Active => {
                // Account is active - continue validation
            }
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
            if let Some(order_value) = utils::calculate_order_value(order.quantity, order.price) {
                let available_buying_power = account.buying_power.amount;
                
                if order_value > available_buying_power {
                    return ValidationResult::invalid(
                        "INSUFFICIENT_BUYING_POWER",
                        &format!(
                            "Order value ${:.2} exceeds available buying power ${:.2}",
                            order_value, available_buying_power
                        ),
                    );
                }
            }
        }
        
        // Check position for sell orders
        if order.side == OrderSide::Sell {
            if let Some(position) = self.positions.get(&(order.account.clone(), order.symbol.clone())) {
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
                return ValidationResult::invalid(
                    "NO_POSITION",
                    "Cannot sell shares, no position exists",
                );
            }
        }
        
        ValidationResult::Valid
    }
    
    /// Validate risk limits and constraints
    async fn validate_risk(&self, order: &Order) -> ValidationResult {
        let risk_limits = match self.risk_limits.get(&order.account) {
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
                .get(&(order.account.clone(), order.symbol.clone()))
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
        if self.config.enable_fat_finger_checks && risk_limits.fat_finger_threshold.is_some() {
            if let Some(order_value) = utils::calculate_order_value(order.quantity, order.price) {
                let threshold = risk_limits.fat_finger_threshold.unwrap();
                
                if order_value > threshold {
                    return ValidationResult::invalid(
                        "FAT_FINGER_CHECK",
                        &format!(
                            "Order value ${:.2} exceeds fat finger threshold ${:.2}",
                            order_value, threshold
                        ),
                    );
                }
            }
        }
        
        ValidationResult::Valid
    }
    
    /// Validate order rate limits
    async fn validate_rate_limits(&self, order: &Order) -> ValidationResult {
        let mut rate_tracker = self.rate_trackers
            .entry(order.account.clone())
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
            .entry(order.account.clone())
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
        if let Some(price) = order.price {
            if let Some(market_data) = self.market_data.get(&order.symbol) {
                let reference_price = market_data.tick.mid_price()
                    .or(market_data.tick.last)
                    .unwrap_or(price); // Fallback to order price
                
                let deviation = ((price - reference_price).abs() / reference_price) * 100.0;
                
                if deviation > self.config.max_price_deviation_pct {
                    return ValidationResult::warning(
                        "PRICE_DEVIATION",
                        &format!(
                            "Order price deviates {:.2}% from market price",
                            deviation
                        ),
                    );
                }
            }
        }
        
        ValidationResult::Valid
    }
    
    /// Calculate hash for duplicate detection
    fn calculate_order_hash(&self, order: &Order) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        order.client_order_id.hash(&mut hasher);
        order.symbol.hash(&mut hasher);
        order.side.hash(&mut hasher);
        order.order_type.hash(&mut hasher);
        order.quantity.to_bits().hash(&mut hasher);
        if let Some(price) = order.price {
            price.to_bits().hash(&mut hasher);
        }
        order.account.hash(&mut hasher);
        
        hasher.finish()
    }
    
    /// Update account information
    pub fn update_account(&self, account: Account) {
        self.accounts.insert(account.id.clone(), account);
    }
    
    /// Update position information
    pub fn update_position(&self, account_id: AccountId, symbol: Symbol, position: Position) {
        self.positions.insert((account_id, symbol), position);
    }
    
    /// Update risk limits
    pub fn update_risk_limits(&self, account_id: AccountId, limits: RiskLimits) {
        self.risk_limits.insert(account_id, limits);
    }
    
    /// Update market data
    pub fn update_market_data(&self, symbol: Symbol, data: ValidationMarketData) {
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
    pub fn get_stats(&self) -> ValidationStats {
        ValidationStats {
            total_validations: AtomicU64::new(self.stats.total_validations.load(Ordering::Relaxed)),
            valid_orders: AtomicU64::new(self.stats.valid_orders.load(Ordering::Relaxed)),
            invalid_orders: AtomicU64::new(self.stats.invalid_orders.load(Ordering::Relaxed)),
            warnings_issued: AtomicU64::new(self.stats.warnings_issued.load(Ordering::Relaxed)),
            error_counts: self.stats.error_counts.clone(),
            avg_validation_time_us: RwLock::new(*self.stats.avg_validation_time_us.read()),
        }
    }
}

/// Utility function to determine market session status
pub fn get_session_status(symbol: &str, timestamp: DateTime<Utc>) -> SessionStatus {
    // Convert to US Eastern Time for market hours
    let eastern_time = timestamp.with_timezone(&chrono_tz::US::Eastern);
    
    // Check if it's a weekday
    match eastern_time.weekday() {
        Weekday::Sat | Weekday::Sun => return SessionStatus::Closed,
        _ => {}
    }
    
    let time = eastern_time.time();
    
    // Standard market hours (9:30 AM - 4:00 PM ET)
    let market_open = NaiveTime::from_hms_opt(9, 30, 0).unwrap();
    let market_close = NaiveTime::from_hms_opt(16, 0, 0).unwrap();
    
    // Extended hours
    let pre_market_start = NaiveTime::from_hms_opt(4, 0, 0).unwrap();
    let post_market_end = NaiveTime::from_hms_opt(20, 0, 0).unwrap();
    
    if time >= market_open && time < market_close {
        SessionStatus::Open
    } else if time >= pre_market_start && time < market_open {
        SessionStatus::PreMarket
    } else if time >= market_close && time < post_market_end {
        SessionStatus::PostMarket
    } else {
        SessionStatus::Closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::order::{OrderBuilder, OrderSide, OrderType};
    
    #[tokio::test]
    async fn test_validator_creation() {
        let config = ValidationConfig::default();
        let validator = OrderValidator::new(config);
        
        // Validator should be created successfully
        assert!(validator.config.strict_mode);
    }
    
    #[tokio::test]
    async fn test_syntax_validation() {
        let config = ValidationConfig::default();
        let validator = OrderValidator::new(config);
        
        // Valid order should pass
        let valid_order = OrderBuilder::new()
            .client_order_id("TEST_001")
            .symbol("AAPL")
            .side(OrderSide::Buy)
            .order_type(OrderType::Limit)
            .quantity(100.0)
            .limit_price(150.0)
            .account("TEST_ACCOUNT")
            .build()
            .unwrap();
        
        let result = validator.validate_syntax(&valid_order).await;
        assert!(result.is_valid());
        
        // Invalid order (empty symbol) should fail
        let mut invalid_order = valid_order.clone();
        invalid_order.symbol = String::new();
        
        let result = validator.validate_syntax(&invalid_order).await;
        assert!(result.is_invalid());
    }
    
    #[tokio::test]
    async fn test_semantic_validation() {
        let config = ValidationConfig::default();
        let validator = OrderValidator::new(config);
        
        // Limit order without price should fail
        let order = OrderBuilder::new()
            .client_order_id("TEST_002")
            .symbol("AAPL")
            .side(OrderSide::Buy)
            .order_type(OrderType::Limit)
            .quantity(100.0)
            .account("TEST_ACCOUNT")
            .build()
            .unwrap();
        
        let mut invalid_order = order;
        invalid_order.price = None; // Remove required price
        
        let result = validator.validate_semantics(&invalid_order).await;
        assert!(result.is_invalid());
    }
    
    #[tokio::test]
    async fn test_rate_limiting() {
        let mut config = ValidationConfig::default();
        config.max_orders_per_second = 2;
        config.rate_limit_window_seconds = 1;
        
        let validator = OrderValidator::new(config);
        
        let order = OrderBuilder::new()
            .client_order_id("TEST_003")
            .symbol("AAPL")
            .side(OrderSide::Buy)
            .order_type(OrderType::Market)
            .quantity(100.0)
            .account("RATE_TEST")
            .build()
            .unwrap();
        
        // First two orders should pass
        let result1 = validator.validate_rate_limits(&order).await;
        assert!(result1.is_valid());
        
        let result2 = validator.validate_rate_limits(&order).await;
        assert!(result2.is_valid());
        
        // Third order should fail due to rate limit
        let result3 = validator.validate_rate_limits(&order).await;
        assert!(result3.is_invalid());
    }
    
    #[test]
    fn test_session_status() {
        use chrono::TimeZone;
        
        // Test regular market hours (10 AM ET on a Wednesday)
        let market_hours = chrono_tz::US::Eastern
            .ymd_opt(2024, 1, 3) // Wednesday
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        
        let status = get_session_status("AAPL", market_hours);
        assert_eq!(status, SessionStatus::Open);
        
        // Test weekend (Saturday)
        let weekend = chrono_tz::US::Eastern
            .ymd_opt(2024, 1, 6) // Saturday
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        
        let status = get_session_status("AAPL", weekend);
        assert_eq!(status, SessionStatus::Closed);
    }
    
    #[tokio::test]
    async fn test_duplicate_detection() {
        let mut config = ValidationConfig::default();
        config.enable_duplicate_detection = true;
        config.duplicate_window_seconds = 5;
        
        let validator = OrderValidator::new(config);
        
        let order = OrderBuilder::new()
            .client_order_id("DUPLICATE_TEST")
            .symbol("AAPL")
            .side(OrderSide::Buy)
            .order_type(OrderType::Limit)
            .quantity(100.0)
            .limit_price(150.0)
            .account("DUP_TEST")
            .build()
            .unwrap();
        
        // First submission should pass
        let result1 = validator.validate_duplicates(&order).await;
        assert!(result1.is_valid());
        
        // Immediate duplicate should fail
        let result2 = validator.validate_duplicates(&order).await;
        assert!(result2.is_invalid());
    }
}
