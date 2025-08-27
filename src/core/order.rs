// src/core/order.rs - Order Domain Models and Business Logic
//! Core order domain models, validation, and business logic
//!
//! This module contains the fundamental order types and business rules for the trading system.
//! It provides type-safe order construction, validation, and state transitions.
//!
//! # Order Lifecycle
//!
//! ```text
//! New → PendingNew → [PartiallyFilled] → Filled
//!   ↘     ↓              ↓                ↓
//!    → PendingCancel → Canceled      → Canceled
//!   ↘     ↓              ↓                ↓  
//!    → PendingReplace → Replaced     → Replaced
//!   ↘                    ↓                ↓
//!    → Rejected    → → → Rejected   → → Rejected
//! ```

use crate::{utils, OmsError, OmsResult};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{self, Display},
    str::FromStr,
};
use uuid::Uuid;

// Re-export common types
pub use crate::core::types::{AccountId, ClientOrderId, OrderId, Price, Quantity, Symbol, Timestamp, Venue};

/// Order side - Buy or Sell
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderSide {
    /// Buy order - client wants to acquire shares
    Buy,
    /// Sell order - client wants to dispose of shares
    Sell,
}

impl Display for OrderSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Buy => write!(f, "Buy"),
            Self::Sell => write!(f, "Sell"),
        }
    }
}

impl FromStr for OrderSide {
    type Err = OmsError;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "buy" | "b" | "1" => Ok(Self::Buy),
            "sell" | "s" | "2" => Ok(Self::Sell),
            _ => Err(OmsError::OrderValidation(format!("Invalid order side: {}", s))),
        }
    }
}

impl OrderSide {
    /// Convert to FIX protocol side value
    pub fn to_fix_side(self) -> &'static str {
        match self {
            Self::Buy => "1",
            Self::Sell => "2",
        }
    }
    
    /// Create from FIX protocol side value
    pub fn from_fix_side(fix_side: &str) -> OmsResult<Self> {
        match fix_side {
            "1" => Ok(Self::Buy),
            "2" => Ok(Self::Sell),
            _ => Err(OmsError::FixProtocol(format!("Invalid FIX side: {}", fix_side))),
        }
    }
    
    /// Check if this side is opposite to another
    pub fn is_opposite(self, other: Self) -> bool {
        self != other
    }
}

/// Order type - Market, Limit, Stop, etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderType {
    /// Market order - execute immediately at best available price
    Market,
    /// Limit order - execute only at specified price or better
    Limit,
    /// Stop order - becomes market order when stop price is reached
    Stop,
    /// Stop-limit order - becomes limit order when stop price is reached
    StopLimit,
    /// Market-on-close order
    MarketOnClose,
    /// Limit-on-close order
    LimitOnClose,
}

impl Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Market => write!(f, "Market"),
            Self::Limit => write!(f, "Limit"),
            Self::Stop => write!(f, "Stop"),
            Self::StopLimit => write!(f, "Stop Limit"),
            Self::MarketOnClose => write!(f, "Market on Close"),
            Self::LimitOnClose => write!(f, "Limit on Close"),
        }
    }
}

impl FromStr for OrderType {
    type Err = OmsError;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "market" | "m" | "1" => Ok(Self::Market),
            "limit" | "l" | "2" => Ok(Self::Limit),
            "stop" | "3" => Ok(Self::Stop),
            "stoplimit" | "stop_limit" | "4" => Ok(Self::StopLimit),
            "marketonclose" | "moc" | "5" => Ok(Self::MarketOnClose),
            "limitonclose" | "loc" | "6" => Ok(Self::LimitOnClose),
            _ => Err(OmsError::OrderValidation(format!("Invalid order type: {}", s))),
        }
    }
}

impl OrderType {
    /// Convert to FIX protocol order type
    pub fn to_fix_type(self) -> &'static str {
        match self {
            Self::Market => "1",
            Self::Limit => "2",
            Self::Stop => "3",
            Self::StopLimit => "4",
            Self::MarketOnClose => "5",
            Self::LimitOnClose => "6",
        }
    }
    
    /// Create from FIX protocol order type
    pub fn from_fix_type(fix_type: &str) -> OmsResult<Self> {
        match fix_type {
            "1" => Ok(Self::Market),
            "2" => Ok(Self::Limit),
            "3" => Ok(Self::Stop),
            "4" => Ok(Self::StopLimit),
            "5" => Ok(Self::MarketOnClose),
            "6" => Ok(Self::LimitOnClose),
            _ => Err(OmsError::FixProtocol(format!("Invalid FIX order type: {}", fix_type))),
        }
    }
    
    /// Check if this order type requires a limit price
    pub fn requires_limit_price(self) -> bool {
        matches!(self, Self::Limit | Self::StopLimit | Self::LimitOnClose)
    }
    
    /// Check if this order type requires a stop price
    pub fn requires_stop_price(self) -> bool {
        matches!(self, Self::Stop | Self::StopLimit)
    }
    
    /// Check if this is a market order type
    pub fn is_market_order(self) -> bool {
        matches!(self, Self::Market | Self::MarketOnClose)
    }
}

/// Order status representing the current state of an order
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// Order created but not yet sent to market
    New,
    /// Order sent to market, awaiting acknowledgment
    PendingNew,
    /// Order is active in the market
    Active,
    /// Order has been partially filled
    PartiallyFilled,
    /// Order has been completely filled
    Filled,
    /// Cancel request sent, awaiting confirmation
    PendingCancel,
    /// Order has been canceled
    Canceled,
    /// Replace request sent, awaiting confirmation
    PendingReplace,
    /// Order has been replaced
    Replaced,
    /// Order was rejected by the market or risk system
    Rejected,
    /// Order has expired
    Expired,
    /// Order is suspended
    Suspended,
}

impl Display for OrderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::New => write!(f, "New"),
            Self::PendingNew => write!(f, "Pending New"),
            Self::Active => write!(f, "Active"),
            Self::PartiallyFilled => write!(f, "Partially Filled"),
            Self::Filled => write!(f, "Filled"),
            Self::PendingCancel => write!(f, "Pending Cancel"),
            Self::Canceled => write!(f, "Canceled"),
            Self::PendingReplace => write!(f, "Pending Replace"),
            Self::Replaced => write!(f, "Replaced"),
            Self::Rejected => write!(f, "Rejected"),
            Self::Expired => write!(f, "Expired"),
            Self::Suspended => write!(f, "Suspended"),
        }
    }
}

impl OrderStatus {
    /// Check if the order is in a final state (cannot be modified)
    pub fn is_final(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Canceled | Self::Rejected | Self::Expired | Self::Replaced
        )
    }
    
    /// Check if the order can be canceled
    pub fn can_be_canceled(self) -> bool {
        matches!(self, Self::New | Self::Active | Self::PartiallyFilled)
    }
    
    /// Check if the order can be replaced/modified
    pub fn can_be_replaced(self) -> bool {
        matches!(self, Self::New | Self::Active | Self::PartiallyFilled)
    }
    
    /// Check if the order is working in the market
    pub fn is_working(self) -> bool {
        matches!(self, Self::Active | Self::PartiallyFilled)
    }
    
    /// Check if the order is pending an action
    pub fn is_pending(self) -> bool {
        matches!(self, Self::PendingNew | Self::PendingCancel | Self::PendingReplace)
    }
    
    /// Get the next valid states from the current state
    pub fn valid_transitions(self) -> Vec<OrderStatus> {
        match self {
            Self::New => vec![Self::PendingNew, Self::Rejected, Self::Canceled],
            Self::PendingNew => vec![Self::Active, Self::Rejected, Self::PendingCancel],
            Self::Active => vec![
                Self::PartiallyFilled,
                Self::Filled,
                Self::PendingCancel,
                Self::PendingReplace,
                Self::Expired,
                Self::Suspended,
            ],
            Self::PartiallyFilled => vec![
                Self::Filled,
                Self::PendingCancel,
                Self::PendingReplace,
                Self::Expired,
            ],
            Self::PendingCancel => vec![Self::Canceled, Self::Active, Self::PartiallyFilled],
            Self::PendingReplace => vec![Self::Replaced, Self::Active, Self::PartiallyFilled],
            Self::Suspended => vec![Self::Active, Self::Canceled],
            _ => vec![], // Final states have no valid transitions
        }
    }
    
    /// Check if transition from this state to another is valid
    pub fn can_transition_to(self, new_status: Self) -> bool {
        self.valid_transitions().contains(&new_status)
    }
}

/// Time in force specification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    /// Good for the trading day
    Day,
    /// Good until canceled
    GoodTilCanceled,
    /// Immediate or cancel - fill immediately or cancel
    ImmediateOrCancel,
    /// Fill or kill - fill entire quantity immediately or cancel
    FillOrKill,
    /// Good till date/time
    GoodTilDate,
    /// At the opening
    AtTheOpening,
    /// At the close
    AtTheClose,
}

impl Display for TimeInForce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Day => write!(f, "Day"),
            Self::GoodTilCanceled => write!(f, "GTC"),
            Self::ImmediateOrCancel => write!(f, "IOC"),
            Self::FillOrKill => write!(f, "FOK"),
            Self::GoodTilDate => write!(f, "GTD"),
            Self::AtTheOpening => write!(f, "OPG"),
            Self::AtTheClose => write!(f, "CLS"),
        }
    }
}

impl TimeInForce {
    /// Convert to FIX protocol time in force
    pub fn to_fix_tif(self) -> &'static str {
        match self {
            Self::Day => "0",
            Self::GoodTilCanceled => "1",
            Self::ImmediateOrCancel => "3",
            Self::FillOrKill => "4",
            Self::GoodTilDate => "6",
            Self::AtTheOpening => "2",
            Self::AtTheClose => "7",
        }
    }
    
    /// Create from FIX protocol time in force
    pub fn from_fix_tif(fix_tif: &str) -> OmsResult<Self> {
        match fix_tif {
            "0" => Ok(Self::Day),
            "1" => Ok(Self::GoodTilCanceled),
            "3" => Ok(Self::ImmediateOrCancel),
            "4" => Ok(Self::FillOrKill),
            "6" => Ok(Self::GoodTilDate),
            "2" => Ok(Self::AtTheOpening),
            "7" => Ok(Self::AtTheClose),
            _ => Err(OmsError::FixProtocol(format!("Invalid FIX time in force: {}", fix_tif))),
        }
    }
}

/// Main order structure containing all order details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    /// Unique system order ID
    pub id: OrderId,
    /// Client-provided order ID
    pub client_order_id: ClientOrderId,
    /// Trading symbol
    pub symbol: Symbol,
    /// Order side (Buy/Sell)
    pub side: OrderSide,
    /// Order type
    pub order_type: OrderType,
    /// Order quantity
    pub quantity: Quantity,
    /// Limit price (for limit orders)
    pub price: Option<Price>,
    /// Stop price (for stop orders)
    pub stop_price: Option<Price>,
    /// Time in force
    pub time_in_force: TimeInForce,
    /// Current order status
    pub status: OrderStatus,
    /// Filled quantity
    pub filled_quantity: Quantity,
    /// Leaves quantity (remaining to be filled)
    pub leaves_quantity: Quantity,
    /// Average fill price
    pub avg_price: Option<Price>,
    /// Last fill price
    pub last_price: Option<Price>,
    /// Last fill quantity
    pub last_quantity: Option<Quantity>,
    /// Creation timestamp
    pub created_at: Timestamp,
    /// Last update timestamp
    pub updated_at: Timestamp,
    /// Expiry time (for GTD orders)
    pub expire_time: Option<Timestamp>,
    /// Target venue
    pub venue: Option<Venue>,
    /// Account ID
    pub account: AccountId,
    /// Order attributes/tags
    pub attributes: HashMap<String, String>,
    /// Cumulative commission
    pub cumulative_commission: Option<Price>,
    /// Order text/comments
    pub text: Option<String>,
}

impl Order {
    /// Create a new order with required fields
    pub fn new(
        client_order_id: ClientOrderId,
        symbol: Symbol,
        side: OrderSide,
        order_type: OrderType,
        quantity: Quantity,
        account: AccountId,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: OrderId::new_v4(),
            client_order_id,
            symbol,
            side,
            order_type,
            quantity,
            price: None,
            stop_price: None,
            time_in_force: TimeInForce::Day,
            status: OrderStatus::New,
            filled_quantity: 0.0,
            leaves_quantity: quantity,
            avg_price: None,
            last_price: None,
            last_quantity: None,
            created_at: now,
            updated_at: now,
            expire_time: None,
            venue: None,
            account,
            attributes: HashMap::new(),
            cumulative_commission: None,
            text: None,
        }
    }
    
    /// Validate the order against business rules
    pub fn validate(&self) -> OmsResult<()> {
        // Validate symbol
        utils::parse_symbol(&self.symbol)?;
        
        // Validate quantity
        utils::validate_quantity(self.quantity)?;
        
        // Validate prices
        if let Some(price) = self.price {
            utils::validate_price(price)?;
        }
        
        if let Some(stop_price) = self.stop_price {
            utils::validate_price(stop_price)?;
        }
        
        // Order type specific validations
        if self.order_type.requires_limit_price() && self.price.is_none() {
            return Err(OmsError::OrderValidation(
                "Limit price required for this order type".to_string(),
            ));
        }
        
        if self.order_type.requires_stop_price() && self.stop_price.is_none() {
            return Err(OmsError::OrderValidation(
                "Stop price required for this order type".to_string(),
            ));
        }
        
        // Validate account
        if self.account.is_empty() {
            return Err(OmsError::OrderValidation("Account ID is required".to_string()));
        }
        
        // Validate client order ID
        if self.client_order_id.is_empty() {
            return Err(OmsError::OrderValidation("Client order ID is required".to_string()));
        }
        
        // Quantity validations
        if self.filled_quantity > self.quantity {
            return Err(OmsError::OrderValidation(
                "Filled quantity cannot exceed order quantity".to_string(),
            ));
        }
        
        if self.leaves_quantity != self.quantity - self.filled_quantity {
            return Err(OmsError::OrderValidation(
                "Leaves quantity calculation is incorrect".to_string(),
            ));
        }
        
        Ok(())
    }
    
    /// Apply a fill to the order
    pub fn apply_fill(&mut self, fill_quantity: Quantity, fill_price: Price) -> OmsResult<()> {
        if fill_quantity <= 0.0 {
            return Err(OmsError::OrderValidation("Fill quantity must be positive".to_string()));
        }
        
        if self.filled_quantity + fill_quantity > self.quantity {
            return Err(OmsError::OrderValidation(
                "Fill would exceed order quantity".to_string(),
            ));
        }
        
        // Update fill information
        self.last_quantity = Some(fill_quantity);
        self.last_price = Some(fill_price);
        
        // Calculate new average price
        let total_value = self.avg_price.unwrap_or(0.0) * self.filled_quantity + fill_price * fill_quantity;
        let new_filled_quantity = self.filled_quantity + fill_quantity;
        self.avg_price = Some(total_value / new_filled_quantity);
        
        // Update quantities
        self.filled_quantity = new_filled_quantity;
        self.leaves_quantity = self.quantity - self.filled_quantity;
        
        // Update status
        self.status = if self.leaves_quantity == 0.0 {
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };
        
        self.updated_at = Utc::now();
        
        Ok(())
    }
    
    /// Update the order status with validation
    pub fn update_status(&mut self, new_status: OrderStatus) -> OmsResult<()> {
        if !self.status.can_transition_to(new_status) {
            return Err(OmsError::OrderValidation(format!(
                "Invalid status transition from {:?} to {:?}",
                self.status, new_status
            )));
        }
        
        self.status = new_status;
        self.updated_at = Utc::now();
        
        Ok(())
    }
    
    /// Check if the order can be canceled
    pub fn can_cancel(&self) -> bool {
        self.status.can_be_canceled()
    }
    
    /// Check if the order can be replaced
    pub fn can_replace(&self) -> bool {
        self.status.can_be_replaced()
    }
    
    /// Get the order value (quantity * price)
    pub fn order_value(&self) -> Option<Price> {
        utils::calculate_order_value(self.quantity, self.price)
    }
    
    /// Get the filled value (filled_quantity * avg_price)
    pub fn filled_value(&self) -> Option<Price> {
        utils::calculate_order_value(self.filled_quantity, self.avg_price)
    }
    
    /// Check if the order is completely filled
    pub fn is_filled(&self) -> bool {
        self.status == OrderStatus::Filled
    }
    
    /// Check if the order has any fills
    pub fn is_partially_filled(&self) -> bool {
        self.filled_quantity > 0.0
    }
    
    /// Get fill percentage
    pub fn fill_percentage(&self) -> f64 {
        utils::calculate_percentage(self.filled_quantity, self.quantity)
    }
}

/// Order request for creating new orders
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    /// Client-provided order ID
    pub client_order_id: ClientOrderId,
    /// Trading symbol
    pub symbol: Symbol,
    /// Order side
    pub side: OrderSide,
    /// Order type
    pub order_type: OrderType,
    /// Order quantity
    pub quantity: Quantity,
    /// Limit price (optional)
    pub price: Option<Price>,
    /// Stop price (optional)
    pub stop_price: Option<Price>,
    /// Time in force
    pub time_in_force: TimeInForce,
    /// Account ID
    pub account: AccountId,
    /// Expiry time for GTD orders
    pub expire_time: Option<Timestamp>,
    /// Target venue (optional)
    pub venue: Option<Venue>,
    /// Order attributes
    pub attributes: Option<HashMap<String, String>>,
    /// Order text/comments
    pub text: Option<String>,
}

impl OrderRequest {
    /// Convert the request to an Order
    pub fn to_order(self) -> Order {
        let mut order = Order::new(
            self.client_order_id,
            self.symbol,
            self.side,
            self.order_type,
            self.quantity,
            self.account,
        );
        
        order.price = self.price;
        order.stop_price = self.stop_price;
        order.time_in_force = self.time_in_force;
        order.expire_time = self.expire_time;
        order.venue = self.venue;
        order.attributes = self.attributes.unwrap_or_default();
        order.text = self.text;
        
        order
    }
    
    /// Validate the order request
    pub fn validate(&self) -> OmsResult<()> {
        // Create temporary order for validation
        let order = self.clone().to_order();
        order.validate()
    }
}

/// Order response containing order and status message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResponse {
    /// The order object
    pub order: Order,
    /// Status message
    pub message: String,
    /// Success indicator
    pub success: bool,
    /// Error details (if any)
    pub error_code: Option<String>,
}

impl OrderResponse {
    /// Create a successful response
    pub fn success(order: Order, message: String) -> Self {
        Self {
            order,
            message,
            success: true,
            error_code: None,
        }
    }
    
    /// Create an error response
    pub fn error(order: Order, message: String, error_code: String) -> Self {
        Self {
            order,
            message,
            success: false,
            error_code: Some(error_code),
        }
    }
}

/// Order update/event for streaming
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderUpdate {
    /// Order ID
    pub order_id: OrderId,
    /// Updated status
    pub status: OrderStatus,
    /// Filled quantity
    pub filled_quantity: Quantity,
    /// Average price
    pub avg_price: Option<Price>,
    /// Last fill price
    pub last_price: Option<Price>,
    /// Last fill quantity
    pub last_quantity: Option<Quantity>,
    /// Update message
    pub message: Option<String>,
    /// Update timestamp
    pub updated_at: Timestamp,
    /// Update type
    pub update_type: OrderUpdateType,
}

/// Types of order updates
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderUpdateType {
    /// Order acknowledged
    Acknowledged,
    /// Order fill
    Fill,
    /// Partial fill
    PartialFill,
    /// Order canceled
    Canceled,
    /// Order rejected
    Rejected,
    /// Order replaced
    Replaced,
    /// Order expired
    Expired,
    /// Status change
    StatusChange,
}

/// Builder pattern for creating orders
#[derive(Debug, Clone)]
pub struct OrderBuilder {
    client_order_id: Option<ClientOrderId>,
    symbol: Option<Symbol>,
    side: Option<OrderSide>,
    order_type: Option<OrderType>,
    quantity: Option<Quantity>,
    price: Option<Price>,
    stop_price: Option<Price>,
    time_in_force: TimeInForce,
    account: Option<AccountId>,
    expire_time: Option<Timestamp>,
    venue: Option<Venue>,
    attributes: HashMap<String, String>,
    text: Option<String>,
}

impl Default for OrderBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderBuilder {
    /// Create a new order builder
    pub fn new() -> Self {
        Self {
            client_order_id: None,
            symbol: None,
            side: None,
            order_type: None,
            quantity: None,
            price: None,
            stop_price: None,
            time_in_force: TimeInForce::Day,
            account: None,
            expire_time: None,
            venue: None,
            attributes: HashMap::new(),
            text: None,
        }
    }
    
    /// Set client order ID
    pub fn client_order_id<S: Into<ClientOrderId>>(mut self, id: S) -> Self {
        self.client_order_id = Some(id.into());
        self
    }
    
    /// Set symbol
    pub fn symbol<S: Into<Symbol>>(mut self, symbol: S) -> Self {
        self.symbol = Some(symbol.into());
        self
    }
    
    /// Set order side
    pub fn side(mut self, side: OrderSide) -> Self {
        self.side = Some(side);
        self
    }
    
    /// Set as buy order
    pub fn buy(mut self) -> Self {
        self.side = Some(OrderSide::Buy);
        self
    }
    
    /// Set as sell order
    pub fn sell(mut self) -> Self {
        self.side = Some(OrderSide::Sell);
        self
    }
    
    /// Set order type
    pub fn order_type(mut self, order_type: OrderType) -> Self {
        self.order_type = Some(order_type);
        self
    }
    
    /// Set as market order
    pub fn market(mut self) -> Self {
        self.order_type = Some(OrderType::Market);
        self
    }
    
    /// Set as limit order with price
    pub fn limit(mut self, price: Price) -> Self {
        self.order_type = Some(OrderType::Limit);
        self.price = Some(price);
        self
    }
    
    /// Set as stop order
    pub fn stop(mut self, stop_price: Price) -> Self {
        self.order_type = Some(OrderType::Stop);
        self.stop_price = Some(stop_price);
        self
    }
    
    /// Set as stop-limit order
    pub fn stop_limit(mut self, stop_price: Price, limit_price: Price) -> Self {
        self.order_type = Some(OrderType::StopLimit);
        self.stop_price = Some(stop_price);
        self.price = Some(limit_price);
        self
    }
    
    /// Set quantity
    pub fn quantity(mut self, quantity: Quantity) -> Self {
        self.quantity = Some(quantity);
        self
    }
    
    /// Set limit price
    pub fn limit_price(mut self, price: Price) -> Self {
        self.price = Some(price);
        self
    }
    
    /// Set stop price
    pub fn stop_price(mut self, stop_price: Price) -> Self {
        self.stop_price = Some(stop_price);
        self
    }
    
    /// Set time in force
    pub fn time_in_force(mut self, tif: TimeInForce) -> Self {
        self.time_in_force = tif;
        self
    }
    
    /// Set account
    pub fn account<S: Into<AccountId>>(mut self, account: S) -> Self {
        self.account = Some(account.into());
        self
    }
    
    /// Set expiry time
    pub fn expire_time(mut self, expire_time: Timestamp) -> Self {
        self.expire_time = Some(expire_time);
        self
    }
    
    /// Set venue
    pub fn venue<S: Into<Venue>>(mut self, venue: S) -> Self {
        self.venue = Some(venue.into());
        self
    }
    
    /// Add attribute
    pub fn attribute<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.attributes.insert(key.into(), value.into());
        self
    }
    
    /// Set text/comments
    pub fn text<S: Into<String>>(mut self, text: S) -> Self {
        self.text = Some(text.into());
        self
    }
    
    /// Build the order
    pub fn build(self) -> OmsResult<Order> {
        let client_order_id = self.client_order_id
            .ok_or_else(|| OmsError::OrderValidation("Client order ID is required".to_string()))?;
        
        let symbol = self.symbol
            .ok_or_else(|| OmsError::OrderValidation("Symbol is required".to_string()))?;
        
        let side = self.side
            .ok_or_else(|| OmsError::OrderValidation("Order side is required".to_string()))?;
        
        let order_type = self.order_type
            .ok_or_else(|| OmsError::OrderValidation("Order type is required".to_string()))?;
        
        let quantity = self.quantity
            .ok_or_else(|| OmsError::OrderValidation("Quantity is required".to_string()))?;
        
        let account = self.account
            .ok_or_else(|| OmsError::OrderValidation("Account is required".to_string()))?;
        
        let mut order = Order::new(client_order_id, symbol, side, order_type, quantity, account);
        
        order.price = self.price;
        order.stop_price = self.stop_price;
        order.time_in_force = self.time_in_force;
        order.expire_time = self.expire_time;
        order.venue = self.venue;
        order.attributes = self.attributes;
        order.text = self.text;
        
        // Validate the built order
        order.validate()?;
        
        Ok(order)
    }
    
    /// Build an order request
    pub fn build_request(self) -> OmsResult<OrderRequest> {
        let client_order_id = self.client_order_id
            .ok_or_else(|| OmsError::OrderValidation("Client order ID is required".to_string()))?;
        
        let symbol = self.symbol
            .ok_or_else(|| OmsError::OrderValidation("Symbol is required".to_string()))?;
        
        let side = self.side
            .ok_or_else(|| OmsError::OrderValidation("Order side is required".to_string()))?;
        
        let order_type = self.order_type
            .ok_or_else(|| OmsError::OrderValidation("Order type is required".to_string()))?;
        
        let quantity = self.quantity
            .ok_or_else(|| OmsError::OrderValidation("Quantity is required".to_string()))?;
        
        let account = self.account
            .ok_or_else(|| OmsError::OrderValidation("Account is required".to_string()))?;
        
        let request = OrderRequest {
            client_order_id,
            symbol,
            side,
            order_type,
            quantity,
            price: self.price,
            stop_price: self.stop_price,
            time_in_force: self.time_in_force,
            account,
            expire_time: self.expire_time,
            venue: self.venue,
            attributes: if self.attributes.is_empty() { None } else { Some(self.attributes) },
            text: self.text,
        };
        
        // Validate the request
        request.validate()?;
        
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_order_side_conversions() {
        assert_eq!(OrderSide::Buy.to_fix_side(), "1");
        assert_eq!(OrderSide::Sell.to_fix_side(), "2");
        
        assert_eq!(OrderSide::from_fix_side("1").unwrap(), OrderSide::Buy);
        assert_eq!(OrderSide::from_fix_side("2").unwrap(), OrderSide::Sell);
        
        assert!(OrderSide::Buy.is_opposite(OrderSide::Sell));
        assert!(!OrderSide::Buy.is_opposite(OrderSide::Buy));
    }
    
    #[test]
    fn test_order_type_requirements() {
        assert!(OrderType::Limit.requires_limit_price());
        assert!(!OrderType::Market.requires_limit_price());
        assert!(OrderType::StopLimit.requires_stop_price());
        assert!(!OrderType::Limit.requires_stop_price());
        assert!(OrderType::Market.is_market_order());
        assert!(!OrderType::Limit.is_market_order());
    }
    
    #[test]
    fn test_order_status_transitions() {
        assert!(OrderStatus::New.can_transition_to(OrderStatus::PendingNew));
        assert!(!OrderStatus::Filled.can_transition_to(OrderStatus::New));
        assert!(OrderStatus::Active.can_be_canceled());
        assert!(!OrderStatus::Filled.can_be_canceled());
        assert!(OrderStatus::Filled.is_final());
        assert!(!OrderStatus::Active.is_final());
    }
    
    #[test]
    fn test_order_builder() {
        let order = OrderBuilder::new()
            .client_order_id("TEST_001")
            .symbol("AAPL")
            .buy()
            .limit(150.0)
            .quantity(100.0)
            .account("TEST_ACCOUNT")
            .build()
            .unwrap();
        
        assert_eq!(order.symbol, "AAPL");
        assert_eq!(order.side, OrderSide::Buy);
        assert_eq!(order.order_type, OrderType::Limit);
        assert_eq!(order.quantity, 100.0);
        assert_eq!(order.price, Some(150.0));
        assert_eq!(order.account, "TEST_ACCOUNT");
    }
    
    #[test]
    fn test_order_validation() {
        let mut order = Order::new(
            "TEST_001".to_string(),
            "AAPL".to_string(),
            OrderSide::Buy,
            OrderType::Limit,
            100.0,
            "TEST_ACCOUNT".to_string(),
        );
        
        // Should fail validation without price
        assert!(order.validate().is_err());
        
        // Should pass with price
        order.price = Some(150.0);
        assert!(order.validate().is_ok());
    }
    
    #[test]
    fn test_order_fills() {
        let mut order = Order::new(
            "TEST_001".to_string(),
            "AAPL".to_string(),
            OrderSide::Buy,
            OrderType::Market,
            100.0,
            "TEST_ACCOUNT".to_string(),
        );
        
        // Apply partial fill
        order.apply_fill(30.0, 150.0).unwrap();
        assert_eq!(order.filled_quantity, 30.0);
        assert_eq!(order.leaves_quantity, 70.0);
        assert_eq!(order.avg_price, Some(150.0));
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        
        // Apply second fill
        order.apply_fill(70.0, 151.0).unwrap();
        assert_eq!(order.filled_quantity, 100.0);
        assert_eq!(order.leaves_quantity, 0.0);
        assert_eq!(order.status, OrderStatus::Filled);
        
        // Average price should be weighted
        let expected_avg = (30.0 * 150.0 + 70.0 * 151.0) / 100.0;
        assert_eq!(order.avg_price, Some(expected_avg));
    }
    
    #[test]
    fn test_order_calculations() {
        let order = OrderBuilder::new()
            .client_order_id("TEST_001")
            .symbol("AAPL")
            .buy()
            .limit(150.0)
            .quantity(100.0)
            .account("TEST_ACCOUNT")
            .build()
            .unwrap();
        
        assert_eq!(order.order_value(), Some(15000.0));
        assert_eq!(order.fill_percentage(), 0.0);
        assert!(!order.is_filled());
        assert!(!order.is_partially_filled());
    }
}
