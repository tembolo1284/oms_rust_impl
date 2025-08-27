// src/engine/matching.rs - Core Matching Engine & Order Book
//! High-performance order matching engine with price-time priority
//!
//! This module implements a fast, lock-free order matching engine that handles:
//! - Price-time priority matching
//! - Order book management
//! - Execution generation
//! - Market data updates
//! - Multiple order types (Market, Limit, Stop, etc.)
//!
//! ## Matching Algorithm
//!
//! The engine uses price-time priority matching:
//! 1. **Price Priority**: Better prices are matched first
//! 2. **Time Priority**: Earlier orders at same price are matched first
//! 3. **Pro-Rata**: For same price/time, quantity is allocated proportionally
//!
//! ## Order Book Structure
//!
//! ```text
//! Ask (Sell) Side                    Bid (Buy) Side
//! Price  | Size | Orders             Price  | Size | Orders
//! -------|------|-------             -------|------|-------
//! 150.05 |  200 |   3                149.95 |  500 |   2
//! 150.04 |  100 |   1                149.94 |  300 |   4
//! 150.03 |  300 |   2                149.93 |  100 |   1
//! 150.02 |  150 |   1         <----> 149.92 |  400 |   3
//! 150.01 |  250 |   2                149.91 |  200 |   1
//! 150.00 |  100 |   1   Best Ask/Bid 149.90 |  600 |   5
//! ```

use crate::{
    core::{
        order::{Order, OrderSide, OrderStatus, OrderType, TimeInForce},
        types::{ExecutionId, OrderId, Price, Quantity, Symbol, Timestamp, Venue},
    },
    OmsError, OmsResult,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering as CmpOrdering,
    collections::{BTreeMap, HashMap, VecDeque},
    time::Instant,
};
use tracing::{debug, instrument, warn};
use uuid::Uuid;

/// Order book level containing price and aggregated quantity
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BookLevel {
    /// Price level
    pub price: Price,
    /// Total quantity at this price level
    pub quantity: Quantity,
    /// Number of orders at this price level
    pub order_count: u32,
    /// Timestamp of last update
    pub updated_at: Timestamp,
}

/// Order entry in the book with priority information
#[derive(Debug, Clone, PartialEq)]
struct OrderEntry {
    /// The order
    order: Order,
    /// Entry timestamp for time priority
    entry_time: Instant,
    /// Sequence number for tie-breaking
    sequence: u64,
}

impl OrderEntry {
    fn new(order: Order, sequence: u64) -> Self {
        Self {
            order,
            entry_time: Instant::now(),
            sequence,
        }
    }
    
    /// Get remaining quantity
    fn remaining_quantity(&self) -> Quantity {
        self.order.leaves_quantity
    }
}

/// Price level containing all orders at a specific price
#[derive(Debug, Clone)]
struct PriceLevel {
    /// Price for this level
    price: Price,
    /// Orders at this price level (in time priority order)
    orders: VecDeque<OrderEntry>,
    /// Total quantity at this level
    total_quantity: Quantity,
    /// Last update timestamp
    updated_at: Timestamp,
}

impl PriceLevel {
    fn new(price: Price) -> Self {
        Self {
            price,
            orders: VecDeque::new(),
            total_quantity: 0.0,
            updated_at: Utc::now(),
        }
    }
    
    /// Add order to this price level
    fn add_order(&mut self, order: Order, sequence: u64) {
        let quantity = order.leaves_quantity;
        self.orders.push_back(OrderEntry::new(order, sequence));
        self.total_quantity += quantity;
        self.updated_at = Utc::now();
    }
    
    /// Remove order from this price level
    fn remove_order(&mut self, order_id: OrderId) -> Option<OrderEntry> {
        if let Some(pos) = self.orders.iter().position(|entry| entry.order.id == order_id) {
            let entry = self.orders.remove(pos).unwrap();
            self.total_quantity -= entry.remaining_quantity();
            self.updated_at = Utc::now();
            Some(entry)
        } else {
            None
        }
    }
    
    /// Update order quantity in this price level
    fn update_order(&mut self, order_id: OrderId, new_quantity: Quantity) -> bool {
        if let Some(entry) = self.orders.iter_mut().find(|entry| entry.order.id == order_id) {
            let old_quantity = entry.remaining_quantity();
            entry.order.leaves_quantity = new_quantity;
            self.total_quantity = self.total_quantity - old_quantity + new_quantity;
            self.updated_at = Utc::now();
            true
        } else {
            false
        }
    }
    
    /// Get the first order in time priority
    fn front_order(&self) -> Option<&OrderEntry> {
        self.orders.front()
    }
    
    /// Get mutable reference to first order
    fn front_order_mut(&mut self) -> Option<&mut OrderEntry> {
        self.orders.front_mut()
    }
    
    /// Remove the first order
    fn pop_front(&mut self) -> Option<OrderEntry> {
        if let Some(entry) = self.orders.pop_front() {
            self.total_quantity -= entry.remaining_quantity();
            self.updated_at = Utc::now();
            Some(entry)
        } else {
            None
        }
    }
    
    /// Check if price level is empty
    fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }
    
    /// Get book level representation
    fn to_book_level(&self) -> BookLevel {
        BookLevel {
            price: self.price,
            quantity: self.total_quantity,
            order_count: self.orders.len() as u32,
            updated_at: self.updated_at,
        }
    }
}

/// Order book for a single symbol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    /// Symbol for this order book
    pub symbol: Symbol,
    /// Bid levels (buy orders) - sorted by price descending
    pub bids: Vec<BookLevel>,
    /// Ask levels (sell orders) - sorted by price ascending
    pub asks: Vec<BookLevel>,
    /// Best bid price
    pub best_bid: Option<Price>,
    /// Best ask price
    pub best_ask: Option<Price>,
    /// Mid price (average of best bid and ask)
    pub mid_price: Option<Price>,
    /// Spread (ask - bid)
    pub spread: Option<Price>,
    /// Last trade price
    pub last_price: Option<Price>,
    /// Last trade quantity
    pub last_quantity: Option<Quantity>,
    /// Last trade timestamp
    pub last_trade_time: Option<Timestamp>,
    /// Total volume traded today
    pub volume: Quantity,
    /// Book update sequence number
    pub sequence: u64,
    /// Last update timestamp
    pub updated_at: Timestamp,
    /// Minimum price increment
    pub tick_size: Price,
}

/// Internal order book with full order details
#[derive(Debug)]
pub struct InternalOrderBook {
    /// Symbol
    symbol: Symbol,
    /// Bid side price levels (price -> PriceLevel)
    bid_levels: BTreeMap<OrderedPrice, PriceLevel>,
    /// Ask side price levels (price -> PriceLevel)
    ask_levels: BTreeMap<OrderedPrice, PriceLevel>,
    /// Order ID to price mapping for quick lookup
    order_prices: HashMap<OrderId, Price>,
    /// Sequence number generator
    sequence: u64,
    /// Tick size
    tick_size: Price,
    /// Statistics
    stats: BookStats,
}

/// Wrapper for Price to enable proper ordering in BTreeMap
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrderedPrice(Price);

impl Eq for OrderedPrice {}

impl PartialOrd for OrderedPrice {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedPrice {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.0.partial_cmp(&other.0).unwrap_or(CmpOrdering::Equal)
    }
}

/// Order book statistics
#[derive(Debug, Clone, Default)]
struct BookStats {
    /// Total orders in book
    total_orders: u32,
    /// Total volume
    total_volume: Quantity,
    /// Last trade price
    last_price: Option<Price>,
    /// Last trade quantity
    last_quantity: Option<Quantity>,
    /// Last trade time
    last_trade_time: Option<Timestamp>,
}

impl InternalOrderBook {
    /// Create new order book
    pub fn new(symbol: Symbol, tick_size: Price) -> Self {
        Self {
            symbol,
            bid_levels: BTreeMap::new(),
            ask_levels: BTreeMap::new(),
            order_prices: HashMap::new(),
            sequence: 0,
            tick_size,
            stats: BookStats::default(),
        }
    }
    
    /// Add order to the book
    #[instrument(skip(self, order))]
    pub fn add_order(&mut self, order: Order) -> OmsResult<()> {
        debug!("Adding order {} to book {}", order.id, self.symbol);
        
        if order.symbol != self.symbol {
            return Err(OmsError::OrderValidation("Symbol mismatch".to_string()));
        }
        
        if order.leaves_quantity <= 0.0 {
            return Err(OmsError::OrderValidation("No remaining quantity to add".to_string()));
        }
        
        let price = match order.order_type {
            OrderType::Market => {
                // Market orders use best available price
                match order.side {
                    OrderSide::Buy => self.best_ask_price().unwrap_or(0.0),
                    OrderSide::Sell => self.best_bid_price().unwrap_or(f64::MAX),
                }
            }
            _ => {
                order.price.ok_or_else(|| {
                    OmsError::OrderValidation("Limit price required for non-market order".to_string())
                })?
            }
        };
        
        // Round price to tick size
        let rounded_price = self.round_to_tick(price);
        let ordered_price = OrderedPrice(rounded_price);
        
        self.sequence += 1;
        self.order_prices.insert(order.id, rounded_price);
        
        match order.side {
            OrderSide::Buy => {
                let level = self.bid_levels
                    .entry(ordered_price)
                    .or_insert_with(|| PriceLevel::new(rounded_price));
                level.add_order(order, self.sequence);
            }
            OrderSide::Sell => {
                let level = self.ask_levels
                    .entry(ordered_price)
                    .or_insert_with(|| PriceLevel::new(rounded_price));
                level.add_order(order, self.sequence);
            }
        }
        
        self.stats.total_orders += 1;
        Ok(())
    }
    
    /// Remove order from the book
    #[instrument(skip(self))]
    pub fn remove_order(&mut self, order_id: OrderId) -> Option<Order> {
        debug!("Removing order {} from book {}", order_id, self.symbol);
        
        let price = self.order_prices.remove(&order_id)?;
        let ordered_price = OrderedPrice(price);
        
        // Try bid side first
        if let Some(level) = self.bid_levels.get_mut(&ordered_price) {
            if let Some(entry) = level.remove_order(order_id) {
                if level.is_empty() {
                    self.bid_levels.remove(&ordered_price);
                }
                self.stats.total_orders -= 1;
                return Some(entry.order);
            }
        }
        
        // Try ask side
        if let Some(level) = self.ask_levels.get_mut(&ordered_price) {
            if let Some(entry) = level.remove_order(order_id) {
                if level.is_empty() {
                    self.ask_levels.remove(&ordered_price);
                }
                self.stats.total_orders -= 1;
                return Some(entry.order);
            }
        }
        
        None
    }
    
    /// Get best bid price
    pub fn best_bid_price(&self) -> Option<Price> {
        self.bid_levels.iter().next_back().map(|(price, _)| price.0)
    }
    
    /// Get best ask price
    pub fn best_ask_price(&self) -> Option<Price> {
        self.ask_levels.iter().next().map(|(price, _)| price.0)
    }
    
    /// Get mid price
    pub fn mid_price(&self) -> Option<Price> {
        match (self.best_bid_price(), self.best_ask_price()) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            _ => None,
        }
    }
    
    /// Get spread
    pub fn spread(&self) -> Option<Price> {
        match (self.best_bid_price(), self.best_ask_price()) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        }
    }
    
    /// Round price to tick size
    fn round_to_tick(&self, price: Price) -> Price {
        if self.tick_size <= 0.0 {
            price
        } else {
            (price / self.tick_size).round() * self.tick_size
        }
    }
    
    /// Convert to public order book representation
    pub fn to_public_book(&self) -> OrderBook {
        let bids: Vec<BookLevel> = self.bid_levels
            .iter()
            .rev() // Highest prices first
            .map(|(_, level)| level.to_book_level())
            .collect();
        
        let asks: Vec<BookLevel> = self.ask_levels
            .iter()
            .map(|(_, level)| level.to_book_level())
            .collect();
        
        let best_bid = self.best_bid_price();
        let best_ask = self.best_ask_price();
        
        OrderBook {
            symbol: self.symbol.clone(),
            bids,
            asks,
            best_bid,
            best_ask,
            mid_price: self.mid_price(),
            spread: self.spread(),
            last_price: self.stats.last_price,
            last_quantity: self.stats.last_quantity,
            last_trade_time: self.stats.last_trade_time,
            volume: self.stats.total_volume,
            sequence: self.sequence,
            updated_at: Utc::now(),
            tick_size: self.tick_size,
        }
    }
}

impl OrderBook {
    /// Create new empty order book
    pub fn new(symbol: Symbol, tick_size: Price) -> Self {
        Self {
            symbol,
            bids: Vec::new(),
            asks: Vec::new(),
            best_bid: None,
            best_ask: None,
            mid_price: None,
            spread: None,
            last_price: None,
            last_quantity: None,
            last_trade_time: None,
            volume: 0.0,
            sequence: 0,
            updated_at: Utc::now(),
            tick_size,
        }
    }
    
    /// Add order to book (placeholder - should be handled by InternalOrderBook)
    pub fn add_order(&mut self, _order: Order) -> OmsResult<()> {
        Err(OmsError::Internal("Use InternalOrderBook for order management".to_string()))
    }
    
    /// Remove order from book (placeholder - should be handled by InternalOrderBook)
    pub fn remove_order(&mut self, _order_id: OrderId) -> Option<Order> {
        None
    }
}

/// Trade execution details
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Execution {
    /// Execution ID
    pub id: ExecutionId,
    /// Aggressive order ID (the order that caused the match)
    pub aggressive_order_id: OrderId,
    /// Passive order ID (the order that was matched against)
    pub passive_order_id: OrderId,
    /// Symbol
    pub symbol: Symbol,
    /// Execution price
    pub price: Price,
    /// Execution quantity
    pub quantity: Quantity,
    /// Aggressive side
    pub aggressive_side: OrderSide,
    /// Execution timestamp
    pub timestamp: Timestamp,
    /// Venue (optional)
    pub venue: Option<Venue>,
    /// Trade flags
    pub flags: Vec<String>,
}

impl Execution {
    /// Create new execution
    pub fn new(
        aggressive_order: &Order,
        passive_order: &Order,
        price: Price,
        quantity: Quantity,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            aggressive_order_id: aggressive_order.id,
            passive_order_id: passive_order.id,
            symbol: aggressive_order.symbol.clone(),
            price,
            quantity,
            aggressive_side: aggressive_order.side,
            timestamp: Utc::now(),
            venue: aggressive_order.venue.clone(),
            flags: Vec::new(),
        }
    }
    
    /// Add execution flag
    pub fn with_flag(mut self, flag: String) -> Self {
        self.flags.push(flag);
        self
    }
}

/// Matching result containing executions and updated orders
#[derive(Debug, Clone, Default)]
pub struct MatchingResult {
    /// Generated executions
    pub executions: Vec<Execution>,
    /// Whether the incoming order was fully matched
    pub fully_matched: bool,
    /// Remaining quantity of incoming order
    pub remaining_quantity: Quantity,
    /// Updated orders (passive orders that were matched)
    pub updated_orders: Vec<Order>,
}

/// Configuration for the matching engine
#[derive(Debug, Clone)]
pub struct MatchingEngineConfig {
    /// Enable price improvement
    pub enable_price_improvement: bool,
    /// Enable pro-rata allocation
    pub enable_pro_rata: bool,
    /// Minimum execution size
    pub min_execution_size: Quantity,
    /// Maximum book depth to consider
    pub max_book_depth: usize,
}

impl Default for MatchingEngineConfig {
    fn default() -> Self {
        Self {
            enable_price_improvement: true,
            enable_pro_rata: false,
            min_execution_size: 1.0,
            max_book_depth: 1000,
        }
    }
}

/// High-performance matching engine
pub struct MatchingEngine {
    /// Configuration
    config: MatchingEngineConfig,
    /// Execution sequence number
    execution_sequence: u64,
}

impl MatchingEngine {
    /// Create new matching engine
    pub fn new(_config: std::sync::Arc<super::EngineConfig>) -> OmsResult<Self> {
        Ok(Self {
            config: MatchingEngineConfig::default(),
            execution_sequence: 0,
        })
    }
    
    /// Match an incoming order against the order book
    #[instrument(skip(self, book, incoming_order))]
    pub async fn match_order(
        &mut self,
        book: &mut InternalOrderBook,
        incoming_order: &mut Order,
    ) -> OmsResult<MatchingResult> {
        debug!(
            "Matching order {} {} {} @ {:?} for {}",
            incoming_order.id,
            incoming_order.side,
            incoming_order.symbol,
            incoming_order.price,
            incoming_order.leaves_quantity
        );
        
        let mut result = MatchingResult::default();
        
        // Handle different order types
        match incoming_order.order_type {
            OrderType::Market => {
                self.match_market_order(book, incoming_order, &mut result).await?;
            }
            OrderType::Limit => {
                self.match_limit_order(book, incoming_order, &mut result).await?;
            }
            OrderType::Stop => {
                // Stop orders become market orders when triggered
                self.match_market_order(book, incoming_order, &mut result).await?;
            }
            OrderType::StopLimit => {
                // Stop-limit orders become limit orders when triggered
                self.match_limit_order(book, incoming_order, &mut result).await?;
            }
            _ => {
                return Err(OmsError::OrderValidation(
                    "Unsupported order type for matching".to_string()
                ));
            }
        }
        
        // Update book statistics
        for execution in &result.executions {
            book.stats.total_volume += execution.quantity;
            book.stats.last_price = Some(execution.price);
            book.stats.last_quantity = Some(execution.quantity);
            book.stats.last_trade_time = Some(execution.timestamp);
        }
        
        // Add remaining quantity to book if not fully matched
        if !result.fully_matched && incoming_order.leaves_quantity > 0.0 {
            match incoming_order.time_in_force {
                TimeInForce::ImmediateOrCancel | TimeInForce::FillOrKill => {
                    // Don't add to book - cancel remaining
                    if incoming_order.time_in_force == TimeInForce::FillOrKill && !result.executions.is_empty() && !result.fully_matched {
                        // FOK order was partially filled - this should not happen
                        warn!("FOK order {} was partially filled", incoming_order.id);
                    }
                }
                _ => {
                    // Add remaining quantity to book
                    if incoming_order.leaves_quantity > 0.0 {
                        book.add_order(incoming_order.clone())?;
                    }
                }
            }
        }
        
        result.remaining_quantity = incoming_order.leaves_quantity;
        result.fully_matched = incoming_order.leaves_quantity == 0.0;
        
        Ok(result)
    }
    
    /// Match market order
    async fn match_market_order(
        &mut self,
        book: &mut InternalOrderBook,
        incoming_order: &mut Order,
        result: &mut MatchingResult,
    ) -> OmsResult<()> {
        let contra_levels = match incoming_order.side {
            OrderSide::Buy => &mut book.ask_levels,
            OrderSide::Sell => &mut book.bid_levels,
        };
        
        let mut remaining_qty = incoming_order.leaves_quantity;
        
        // Get price levels in the correct order
        let mut levels_to_process: Vec<_> = if incoming_order.side == OrderSide::Buy {
            // Buy orders match against asks (lowest price first)
            contra_levels.iter_mut().collect()
        } else {
            // Sell orders match against bids (highest price first)
            contra_levels.iter_mut().rev().collect()
        };
        
        for (_, level) in levels_to_process.iter_mut() {
            if remaining_qty <= 0.0 {
                break;
            }
            
            // Match against orders in time priority
            while let Some(passive_entry) = level.front_order_mut() {
                if remaining_qty <= 0.0 {
                    break;
                }
                
                let match_qty = remaining_qty.min(passive_entry.remaining_quantity());
                let match_price = level.price;
                
                // Create execution
                self.execution_sequence += 1;
                let execution = Execution::new(
                    incoming_order,
                    &passive_entry.order,
                    match_price,
                    match_qty,
                );
                
                result.executions.push(execution);
                
                // Update quantities
                remaining_qty -= match_qty;
                incoming_order.leaves_quantity -= match_qty;
                passive_entry.order.leaves_quantity -= match_qty;
                level.total_quantity -= match_qty;
                
                // Update passive order
                if passive_entry.order.leaves_quantity <= 0.0 {
                    // Order fully filled - remove from book
                    let filled_order = level.pop_front().unwrap();
                    book.order_prices.remove(&filled_order.order.id);
                    result.updated_orders.push(filled_order.order);
                } else {
                    // Partial fill
                    result.updated_orders.push(passive_entry.order.clone());
                }
                
                // Check if we need more liquidity
                if match_qty < remaining_qty {
                    continue; // More orders at this level
                } else {
                    break; // Move to next price level
                }
            }
        }
        
        // Clean up empty levels
        contra_levels.retain(|_, level| !level.is_empty());
        
        Ok(())
    }
    
    /// Match limit order
    async fn match_limit_order(
        &mut self,
        book: &mut InternalOrderBook,
        incoming_order: &mut Order,
        result: &mut MatchingResult,
    ) -> OmsResult<()> {
        let order_price = incoming_order.price.ok_or_else(|| {
            OmsError::OrderValidation("Limit price required".to_string())
        })?;
        
        let contra_levels = match incoming_order.side {
            OrderSide::Buy => &mut book.ask_levels,
            OrderSide::Sell => &mut book.bid_levels,
        };
        
        let mut remaining_qty = incoming_order.leaves_quantity;
        
        // For limit orders, only match at prices that are equal or better
        let mut levels_to_remove = Vec::new();
        
        for (ordered_price, level) in contra_levels.iter_mut() {
            let level_price = ordered_price.0;
            
            // Check if we can match at this price level
            let can_match = match incoming_order.side {
                OrderSide::Buy => level_price <= order_price,   // Buy: match at or below our limit
                OrderSide::Sell => level_price >= order_price,  // Sell: match at or above our limit
            };
            
            if !can_match {
                if incoming_order.side == OrderSide::Buy {
                    break; // Asks are sorted ascending, no more matches possible
                } else {
                    continue; // Bids are processed in descending order
                }
            }
            
            if remaining_qty <= 0.0 {
                break;
            }
            
            // Match against orders in time priority at this level
            while let Some(passive_entry) = level.front_order_mut() {
                if remaining_qty <= 0.0 {
                    break;
                }
                
                let match_qty = remaining_qty.min(passive_entry.remaining_quantity());
                let match_price = level.price; // Use the passive order's price
                
                // Create execution
                self.execution_sequence += 1;
                let execution = Execution::new(
                    incoming_order,
                    &passive_entry.order,
                    match_price,
                    match_qty,
                );
                
                result.executions.push(execution);
                
                // Update quantities
                remaining_qty -= match_qty;
                incoming_order.leaves_quantity -= match_qty;
                passive_entry.order.leaves_quantity -= match_qty;
                level.total_quantity -= match_qty;
                
                // Update passive order
                if passive_entry.order.leaves_quantity <= 0.0 {
                    // Order fully filled - remove from book
                    let filled_order = level.pop_front().unwrap();
                    book.order_prices.remove(&filled_order.order.id);
                    result.updated_orders.push(filled_order.order);
                } else {
                    // Partial fill
                    result.updated_orders.push(passive_entry.order.clone());
                }
            }
            
            // Mark level for removal if empty
            if level.is_empty() {
                levels_to_remove.push(*ordered_price);
            }
        }
        
        // Remove empty levels
        for price in levels_to_remove {
            contra_levels.remove(&price);
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::order::{OrderBuilder, OrderSide, OrderType};
    
    #[test]
    fn test_order_book_creation() {
        let book = InternalOrderBook::new("AAPL".to_string(), 0.01);
        assert_eq!(book.symbol, "AAPL");
        assert_eq!(book.tick_size, 0.01);
        assert!(book.bid_levels.is_empty());
        assert!(book.ask_levels.is_empty());
    }
    
    #[test]
    fn test_add_limit_order() {
        let mut book = InternalOrderBook::new("AAPL".to_string(), 0.01);
        
        let order = OrderBuilder::new()
            .client_order_id("TEST_001")
            .symbol("AAPL")
            .side(OrderSide::Buy)
            .order_type(OrderType::Limit)
            .quantity(100.0)
            .limit_price(150.0)
            .account("TEST")
            .build()
            .unwrap();
        
        let result = book.add_order(order);
        assert!(result.is_ok());
        assert_eq!(book.bid_levels.len(), 1);
        assert_eq!(book.best_bid_price(), Some(150.0));
    }
    
    #[test]
    fn test_remove_order() {
        let mut book = InternalOrderBook::new("AAPL".to_string(), 0.01);
        
        let order = OrderBuilder::new()
            .client_order_id("TEST_001")
            .symbol("AAPL")
            .side(OrderSide::Buy)
            .order_type(OrderType::Limit)
            .quantity(100.0)
            .limit_price(150.0)
            .account("TEST")
            .build()
            .unwrap();
        
        let order_id = order.id;
        book.add_order(order).unwrap();
        
        let removed_order = book.remove_order(order_id);
        assert!(removed_order.is_some());
        assert!(book.bid_levels.is_empty());
    }
    
    #[test]
    fn test_best_prices() {
        let mut book = InternalOrderBook::new("AAPL".to_string(), 0.01);
        
        // Add bid orders
        let bid1 = OrderBuilder::new()
            .client_order_id("BID1")
            .symbol("AAPL")
            .side(OrderSide::Buy)
            .order_type(OrderType::Limit)
            .quantity(100.0)
            .limit_price(149.99)
            .account("TEST")
            .build()
            .unwrap();
        
        let bid2 = OrderBuilder::new()
            .client_order_id("BID2")
            .symbol("AAPL")
            .side(OrderSide::Buy)
            .order_type(OrderType::Limit)
            .quantity(200.0)
            .limit_price(150.01) // Better bid
            .account("TEST")
            .build()
            .unwrap();
        
        // Add ask orders
        let ask1 = OrderBuilder::new()
            .client_order_id("ASK1")
            .symbol("AAPL")
            .side(OrderSide::Sell)
            .order_type(OrderType::Limit)
            .quantity(150.0)
            .limit_price(150.05)
            .account("TEST")
            .build()
            .unwrap();
        
        let ask2 = OrderBuilder::new()
            .client_order_id("ASK2")
            .symbol("AAPL")
            .side(OrderSide::Sell)
            .order_type(OrderType::Limit)
            .quantity(100.0)
            .limit_price(150.03) // Better ask
            .account("TEST")
            .build()
            .unwrap();
        
        book.add_order(bid1).unwrap();
        book.add_order(bid2).unwrap();
        book.add_order(ask1).unwrap();
        book.add_order(ask2).unwrap();
        
        assert_eq!(book.best_bid_price(), Some(150.01));
        assert_eq!(book.best_ask_price(), Some(150.03));
        assert_eq!(book.mid_price(), Some(150.02));
        assert_eq!(book.spread(), Some(0.02));
    }
    
    #[tokio::test]
    async fn test_market_order_matching() {
        use crate::engine::EngineConfig;
        use std::sync::Arc;
        
        let mut book = InternalOrderBook::new("AAPL".to_string(), 0.01);
        let mut engine = MatchingEngine::new(Arc::new(EngineConfig::default())).unwrap();
        
        // Add a sell order to the book
        let ask_order = OrderBuilder::new()
            .client_order_id("ASK1")
            .symbol("AAPL")
            .side(OrderSide::Sell)
            .order_type(OrderType::Limit)
            .quantity(200.0)
            .limit_price(150.00)
            .account("SELLER")
            .build()
            .unwrap();
        
        book.add_order(ask_order).unwrap();
        
        // Create market buy order
        let mut market_order = OrderBuilder::new()
            .client_order_id("MKT1")
            .symbol("AAPL")
            .side(OrderSide::Buy)
            .order_type(OrderType::Market)
            .quantity(100.0)
            .account("BUYER")
            .build()
            .unwrap();
        
        let result = engine.match_order(&mut book, &mut market_order).await.unwrap();
        
        assert_eq!(result.executions.len(), 1);
        assert_eq!(result.executions[0].quantity, 100.0);
        assert_eq!(result.executions[0].price, 150.00);
        assert!(result.fully_matched);
        assert_eq!(result.remaining_quantity, 0.0);
    }
    
    #[test]
    fn test_price_level_operations() {
        let mut level = PriceLevel::new(150.0);
        assert!(level.is_empty());
        assert_eq!(level.total_quantity, 0.0);
        
        let order = OrderBuilder::new()
            .client_order_id("TEST")
            .symbol("AAPL")
            .side(OrderSide::Buy)
            .order_type(OrderType::Limit)
            .quantity(100.0)
            .limit_price(150.0)
            .account("TEST")
            .build()
            .unwrap();
        
        level.add_order(order.clone(), 1);
        assert!(!level.is_empty());
        assert_eq!(level.total_quantity, 100.0);
        
        let removed = level.remove_order(order.id);
        assert!(removed.is_some());
        assert!(level.is_empty());
        assert_eq!(level.total_quantity, 0.0);
    }
}
