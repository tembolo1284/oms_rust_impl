// src/core/types.rs - Core Type Definitions
//! Core type definitions used throughout the trading system
//!
//! This module defines the fundamental types used across all modules to ensure
//! type safety and consistency throughout the system.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use uuid::Uuid;

/// Type alias for order IDs - using UUID for global uniqueness
pub type OrderId = Uuid;

/// Type alias for client-provided order IDs
pub type ClientOrderId = String;

/// Type alias for account identifiers
pub type AccountId = String;

/// Type alias for trading symbols
pub type Symbol = String;

/// Type alias for venue identifiers
pub type Venue = String;

/// Type alias for prices - using f64 for precision
/// Note: In production systems, consider using a decimal type for exact precision
pub type Price = f64;

/// Type alias for quantities
pub type Quantity = f64;

/// Type alias for timestamps
pub type Timestamp = DateTime<Utc>;

/// Type alias for sequence numbers
pub type SequenceNumber = u64;

/// Type alias for trade IDs
pub type TradeId = String;

/// Type alias for execution IDs
pub type ExecutionId = Uuid;

/// Currency code (ISO 4217)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Currency(String);

impl Currency {
    /// Create a new currency
    pub fn new(code: &str) -> Self {
        Self(code.to_uppercase())
    }
    
    /// Get the currency code
    pub fn code(&self) -> &str {
        &self.0
    }
    
    /// Common currencies
    pub const USD: Currency = Currency(String::new());
    pub const EUR: Currency = Currency(String::new()); 
    pub const GBP: Currency = Currency(String::new());
    pub const JPY: Currency = Currency(String::new());
}

impl Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for Currency {
    fn from(code: &str) -> Self {
        Self::new(code)
    }
}

/// Money amount with currency
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Money {
    /// The amount
    pub amount: Price,
    /// The currency
    pub currency: Currency,
}

impl Money {
    /// Create a new money amount
    pub fn new(amount: Price, currency: Currency) -> Self {
        Self { amount, currency }
    }
    
    /// Create USD amount
    pub fn usd(amount: Price) -> Self {
        Self::new(amount, Currency::new("USD"))
    }
    
    /// Get the amount
    pub fn amount(&self) -> Price {
        self.amount
    }
    
    /// Get the currency
    pub fn currency(&self) -> &Currency {
        &self.currency
    }
}

impl Display for Money {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:.2} {}", self.amount, self.currency)
    }
}

/// Execution report details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Execution {
    /// Execution ID
    pub id: ExecutionId,
    /// Order ID this execution belongs to
    pub order_id: OrderId,
    /// Trade ID
    pub trade_id: TradeId,
    /// Execution price
    pub price: Price,
    /// Execution quantity
    pub quantity: Quantity,
    /// Execution side
    pub side: crate::core::order::OrderSide,
    /// Execution timestamp
    pub timestamp: Timestamp,
    /// Venue where trade occurred
    pub venue: Venue,
    /// Commission charged
    pub commission: Option<Money>,
    /// Contra party
    pub contra_party: Option<String>,
    /// Settlement date
    pub settlement_date: Option<DateTime<Utc>>,
}

/// Position information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    /// Account ID
    pub account: AccountId,
    /// Symbol
    pub symbol: Symbol,
    /// Current quantity (positive = long, negative = short)
    pub quantity: Quantity,
    /// Average cost basis
    pub avg_cost: Price,
    /// Market value
    pub market_value: Money,
    /// Unrealized P&L
    pub unrealized_pnl: Money,
    /// Realized P&L for the day
    pub realized_pnl: Money,
    /// Last updated timestamp
    pub updated_at: Timestamp,
}

impl Position {
    /// Check if position is long
    pub fn is_long(&self) -> bool {
        self.quantity > 0.0
    }
    
    /// Check if position is short
    pub fn is_short(&self) -> bool {
        self.quantity < 0.0
    }
    
    /// Check if position is flat (no position)
    pub fn is_flat(&self) -> bool {
        self.quantity == 0.0
    }
    
    /// Get absolute position size
    pub fn abs_quantity(&self) -> Quantity {
        self.quantity.abs()
    }
}

/// Account information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    /// Account ID
    pub id: AccountId,
    /// Account name
    pub name: String,
    /// Account type
    pub account_type: AccountType,
    /// Account status
    pub status: AccountStatus,
    /// Base currency
    pub base_currency: Currency,
    /// Available buying power
    pub buying_power: Money,
    /// Total equity
    pub total_equity: Money,
    /// Cash balance
    pub cash_balance: Money,
    /// Margin requirement
    pub margin_requirement: Money,
    /// Day trading buying power
    pub day_trading_buying_power: Option<Money>,
    /// Created timestamp
    pub created_at: Timestamp,
    /// Last updated timestamp
    pub updated_at: Timestamp,
}

/// Account type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountType {
    /// Cash account
    Cash,
    /// Margin account
    Margin,
    /// Pattern day trader account
    PatternDayTrader,
    /// Portfolio margin account
    PortfolioMargin,
    /// Institutional account
    Institutional,
}

/// Account status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountStatus {
    /// Active account
    Active,
    /// Suspended account
    Suspended,
    /// Closed account
    Closed,
    /// Restricted account
    Restricted,
    /// Pending approval
    PendingApproval,
}

/// Market data tick
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketTick {
    /// Symbol
    pub symbol: Symbol,
    /// Bid price
    pub bid: Option<Price>,
    /// Ask price  
    pub ask: Option<Price>,
    /// Last trade price
    pub last: Option<Price>,
    /// Bid size
    pub bid_size: Option<Quantity>,
    /// Ask size
    pub ask_size: Option<Quantity>,
    /// Last trade size
    pub last_size: Option<Quantity>,
    /// Volume for the day
    pub volume: Quantity,
    /// High price for the day
    pub high: Option<Price>,
    /// Low price for the day
    pub low: Option<Price>,
    /// Open price for the day
    pub open: Option<Price>,
    /// Previous close price
    pub prev_close: Option<Price>,
    /// Timestamp of the tick
    pub timestamp: Timestamp,
    /// Venue/exchange
    pub venue: Venue,
}

impl MarketTick {
    /// Get the mid price (average of bid and ask)
    pub fn mid_price(&self) -> Option<Price> {
        match (self.bid, self.ask) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            _ => None,
        }
    }
    
    /// Get the spread (ask - bid)
    pub fn spread(&self) -> Option<Price> {
        match (self.bid, self.ask) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        }
    }
    
    /// Get the spread in basis points
    pub fn spread_bps(&self) -> Option<Price> {
        self.spread().and_then(|spread| {
            self.mid_price().map(|mid| (spread / mid) * 10_000.0)
        })
    }
}

/// Risk limits for an account or symbol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskLimits {
    /// Maximum position size
    pub max_position_size: Quantity,
    /// Maximum order size
    pub max_order_size: Quantity,
    /// Maximum daily loss
    pub max_daily_loss: Money,
    /// Maximum leverage
    pub max_leverage: Option<f64>,
    /// Concentration limit (% of portfolio)
    pub concentration_limit: Option<f64>,
    /// Maximum orders per second
    pub max_orders_per_second: u32,
    /// Fat finger check threshold
    pub fat_finger_threshold: Option<Price>,
}

/// Trade information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    /// Trade ID
    pub id: TradeId,
    /// Symbol traded
    pub symbol: Symbol,
    /// Trade price
    pub price: Price,
    /// Trade quantity
    pub quantity: Quantity,
    /// Trade timestamp
    pub timestamp: Timestamp,
    /// Venue where trade occurred
    pub venue: Venue,
    /// Trade conditions/flags
    pub conditions: Vec<String>,
    /// Settlement date
    pub settlement_date: DateTime<Utc>,
}

/// Session information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    /// Session ID
    pub id: String,
    /// User/client ID
    pub user_id: String,
    /// Session start time
    pub started_at: Timestamp,
    /// Last activity time
    pub last_activity: Timestamp,
    /// Session status
    pub status: SessionStatus,
    /// Client IP address
    pub client_ip: Option<String>,
    /// User agent
    pub user_agent: Option<String>,
}

/// Session status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    /// Active session
    Active,
    /// Expired session
    Expired,
    /// Terminated session
    Terminated,
}

/// Connection information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connection {
    /// Connection ID
    pub id: String,
    /// Connection type
    pub connection_type: ConnectionType,
    /// Remote address
    pub remote_addr: String,
    /// Connected at timestamp
    pub connected_at: Timestamp,
    /// Last heartbeat
    pub last_heartbeat: Timestamp,
    /// Connection status
    pub status: ConnectionStatus,
}

/// Connection type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionType {
    /// FIX protocol connection
    Fix,
    /// WebSocket connection
    WebSocket,
    /// HTTP connection
    Http,
    /// TCP connection
    Tcp,
}

/// Connection status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionStatus {
    /// Connected
    Connected,
    /// Disconnected
    Disconnected,
    /// Connecting
    Connecting,
    /// Error state
    Error,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_currency_creation() {
        let usd = Currency::new("usd");
        assert_eq!(usd.code(), "USD");
        
        let eur = Currency::from("eur");
        assert_eq!(eur.code(), "EUR");
    }
    
    #[test]
    fn test_money_operations() {
        let amount = Money::usd(100.50);
        assert_eq!(amount.amount(), 100.50);
        assert_eq!(amount.currency().code(), "USD");
        
        let formatted = format!("{}", amount);
        assert_eq!(formatted, "100.50 USD");
    }
    
    #[test]
    fn test_position_checks() {
        let mut pos = Position {
            account: "TEST".to_string(),
            symbol: "AAPL".to_string(),
            quantity: 100.0,
            avg_cost: 150.0,
            market_value: Money::usd(15000.0),
            unrealized_pnl: Money::usd(0.0),
            realized_pnl: Money::usd(0.0),
            updated_at: Utc::now(),
        };
        
        assert!(pos.is_long());
        assert!(!pos.is_short());
        assert!(!pos.is_flat());
        assert_eq!(pos.abs_quantity(), 100.0);
        
        pos.quantity = -50.0;
        assert!(!pos.is_long());
        assert!(pos.is_short());
        assert_eq!(pos.abs_quantity(), 50.0);
        
        pos.quantity = 0.0;
        assert!(pos.is_flat());
    }
    
    #[test]
    fn test_market_tick_calculations() {
        let tick = MarketTick {
            symbol: "AAPL".to_string(),
            bid: Some(150.0),
            ask: Some(150.10),
            last: Some(150.05),
            bid_size: Some(100.0),
            ask_size: Some(200.0),
            last_size: Some(50.0),
            volume: 1000.0,
            high: Some(151.0),
            low: Some(149.0),
            open: Some(149.5),
            prev_close: Some(149.0),
            timestamp: Utc::now(),
            venue: "NASDAQ".to_string(),
        };
        
        assert_eq!(tick.mid_price(), Some(150.05));
        assert_eq!(tick.spread(), Some(0.10));
        
        let spread_bps = tick.spread_bps().unwrap();
        assert!((spread_bps - 6.66).abs() < 0.1); // Approximately 6.66 bps
    }
}
