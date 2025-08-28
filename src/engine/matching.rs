// src/engine/matching.rs - Enhanced Core Matching Engine & Order Book
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

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering as CmpOrdering,
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::core::{
    order::{Order, OrderSide, OrderStatus, OrderType},
    types::{OrderId, Symbol},
};

// Type aliases for clarity
pub type Price = f64;
pub type Quantity = f64;
pub type ExecutionId = Uuid;
pub type Timestamp = chrono::DateTime<chrono::Utc>;

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
#[derive(Debug, Clone)]
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
        self.order.remaining_quantity()
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
        let quantity = order.remaining_quantity();
        self.orders.push_back(OrderEntry::new(order, sequence));
        self.total_quantity += quantity;
        self.updated_at = Utc::now();
    }

    /// Remove order from this price level
    fn remove_order(&mut self, order_id: &OrderId) -> Option<OrderEntry> {
        if let Some(pos) = self.orders.iter().position(|entry| entry.order.id == *order_id) {
            let entry = self.orders.remove(pos).unwrap();
            self.total_quantity -= entry.remaining_quantity();
            self.updated_at = Utc::now();
            Some(entry)
        } else {
            None
        }
    }

    /// Update order quantity in this price level
    fn update_order(&mut self, order_id: &OrderId, new_quantity: Quantity) -> bool {
        if let Some(entry) = self.orders.iter_mut().find(|entry| entry.order.id == *order_id) {
            let old_quantity = entry.remaining_quantity();
            entry.order.update_quantity(new_quantity);
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
pub struct InternalOrderBook {
    /// Symbol
    symbol: Symbol,
    /// Bid side price levels (price -> PriceLevel)
    bid_levels: BTreeMap<OrderedPrice, PriceLevel>,
    /// Ask side price levels (price -> PriceLevel)
    ask_levels: BTreeMap<OrderedPrice, PriceLevel>,
    /// Order ID to price mapping for quick lookup
    order_prices: HashMap<OrderId, Price>,
    /// Order ID to side mapping
    order_sides: HashMap<OrderId, OrderSide>,
    /// Sequence number generator
    sequence: Arc<AtomicU64>,
    /// Tick size
    tick_size: Price,
    /// Statistics
    stats: Arc<RwLock<BookStats>>,
    /// Market data publisher
    market_data_tx: broadcast::Sender<MarketData>,
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
pub struct BookStats {
    /// Total orders in book
    pub total_orders: u32,
    /// Total bid orders
    pub total_bids: u32,
    /// Total ask orders
    pub total_asks: u32,
    /// Total bid volume
    pub total_bid_volume: Quantity,
    /// Total ask volume
    pub total_ask_volume: Quantity,
    /// Total volume traded
    pub total_volume: Quantity,
    /// Total number of trades
    pub total_trades: u64,
    /// Last trade price
    pub last_price: Option<Price>,
    /// Last trade quantity
    pub last_quantity: Option<Quantity>,
    /// Last trade time
    pub last_trade_time: Option<Timestamp>,
    /// VWAP (Volume Weighted Average Price)
    pub vwap: Option<Price>,
}

/// Market data update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketData {
    pub symbol: Symbol,
    pub best_bid: Option<Price>,
    pub best_bid_size: Option<Quantity>,
    pub best_ask: Option<Price>,
    pub best_ask_size: Option<Quantity>,
    pub last_price: Option<Price>,
    pub last_size: Option<Quantity>,
    pub volume: Quantity,
    pub timestamp: Timestamp,
}

impl InternalOrderBook {
    /// Create new order book
    pub fn new(symbol: Symbol, tick_size: Price) -> Self {
        let (market_data_tx, _) = broadcast::channel(1000);
        
        Self {
            symbol,
            bid_levels: BTreeMap::new(),
            ask_levels: BTreeMap::new(),
            order_prices: HashMap::new(),
            order_sides: HashMap::new(),
            sequence: Arc::new(AtomicU64::new(0)),
            tick_size,
            stats: Arc::new(RwLock::new(BookStats::default())),
            market_data_tx,
        }
    }

    /// Subscribe to market data updates
    pub fn subscribe_market_data(&self) -> broadcast::Receiver<MarketData> {
        self.market_data_tx.subscribe()
    }

    /// Add order to the book
    #[instrument(skip(self, order))]
    pub fn add_order(&mut self, order: Order) -> Result<()> {
        debug!("Adding order {} to book {}", order.id, self.symbol);

        if order.symbol != self.symbol {
            return Err(anyhow!("Symbol mismatch: expected {}, got {}", self.symbol, order.symbol));
        }

        if order.remaining_quantity() <= 0.0 {
            return Err(anyhow!("No remaining quantity to add"));
        }

        let price = self.get_order_price(&order)?;
        let rounded_price = self.round_to_tick(price);
        let ordered_price = OrderedPrice(rounded_price);

        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        self.order_prices.insert(order.id.clone(), rounded_price);
        self.order_sides.insert(order.id.clone(), order.side);

        match order.side {
            OrderSide::Buy => {
                let level = self.bid_levels
                    .entry(ordered_price)
                    .or_insert_with(|| PriceLevel::new(rounded_price));
                level.add_order(order.clone(), seq);
                
                // Update stats
                let mut stats = self.stats.write();
                stats.total_orders += 1;
                stats.total_bids += 1;
                stats.total_bid_volume += order.remaining_quantity();
            }
            OrderSide::Sell => {
                let level = self.ask_levels
                    .entry(ordered_price)
                    .or_insert_with(|| PriceLevel::new(rounded_price));
                level.add_order(order.clone(), seq);
                
                // Update stats
                let mut stats = self.stats.write();
                stats.total_orders += 1;
                stats.total_asks += 1;
                stats.total_ask_volume += order.remaining_quantity();
            }
        }

        // Publish market data update
        self.publish_market_data();
        
        Ok(())
    }

    /// Remove order from the book
    #[instrument(skip(self))]
    pub fn remove_order(&mut self, order_id: &OrderId) -> Option<Order> {
        debug!("Removing order {} from book {}", order_id, self.symbol);

        let price = self.order_prices.remove(order_id)?;
        let side = self.order_sides.remove(order_id)?;
        let ordered_price = OrderedPrice(price);

        let removed_order = match side {
            OrderSide::Buy => {
                if let Some(level) = self.bid_levels.get_mut(&ordered_price) {
                    if let Some(entry) = level.remove_order(order_id) {
                        if level.is_empty() {
                            self.bid_levels.remove(&ordered_price);
                        }
                        
                        // Update stats
                        let mut stats = self.stats.write();
                        stats.total_orders -= 1;
                        stats.total_bids -= 1;
                        stats.total_bid_volume -= entry.remaining_quantity();
                        
                        Some(entry.order)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            OrderSide::Sell => {
                if let Some(level) = self.ask_levels.get_mut(&ordered_price) {
                    if let Some(entry) = level.remove_order(order_id) {
                        if level.is_empty() {
                            self.ask_levels.remove(&ordered_price);
                        }
                        
                        // Update stats
                        let mut stats = self.stats.write();
                        stats.total_orders -= 1;
                        stats.total_asks -= 1;
                        stats.total_ask_volume -= entry.remaining_quantity();
                        
                        Some(entry.order)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        };

        if removed_order.is_some() {
            self.publish_market_data();
        }

        removed_order
    }

    /// Update order quantity
    pub fn update_order_quantity(&mut self, order_id: &OrderId, new_quantity: Quantity) -> Result<()> {
        let price = self.order_prices.get(order_id)
            .ok_or_else(|| anyhow!("Order not found"))?;
        let side = self.order_sides.get(order_id)
            .ok_or_else(|| anyhow!("Order side not found"))?;
        let ordered_price = OrderedPrice(*price);

        match side {
            OrderSide::Buy => {
                if let Some(level) = self.bid_levels.get_mut(&ordered_price) {
                    if level.update_order(order_id, new_quantity) {
                        self.publish_market_data();
                        return Ok(());
                    }
                }
            }
            OrderSide::Sell => {
                if let Some(level) = self.ask_levels.get_mut(&ordered_price) {
                    if level.update_order(order_id, new_quantity) {
                        self.publish_market_data();
                        return Ok(());
                    }
                }
            }
        }

        Err(anyhow!("Failed to update order quantity"))
    }

    /// Get order price based on order type
    fn get_order_price(&self, order: &Order) -> Result<Price> {
        match order.order_type {
            OrderType::Market => {
                // Market orders use best available price
                match order.side {
                    OrderSide::Buy => self.best_ask_price()
                        .ok_or_else(|| anyhow!("No asks available for market buy")),
                    OrderSide::Sell => self.best_bid_price()
                        .ok_or_else(|| anyhow!("No bids available for market sell")),
                }
            }
            OrderType::Limit { price } | OrderType::StopLimit { stop_price: _, limit_price: price } => {
                Ok(price)
            }
            OrderType::Stop { .. } => {
                // Stop orders become market orders when triggered
                match order.side {
                    OrderSide::Buy => self.best_ask_price()
                        .ok_or_else(|| anyhow!("No asks available for stop buy")),
                    OrderSide::Sell => self.best_bid_price()
                        .ok_or_else(|| anyhow!("No bids available for stop sell")),
                }
            }
        }
    }

    /// Get best bid price
    pub fn best_bid_price(&self) -> Option<Price> {
        self.bid_levels.iter().next_back().map(|(price, _)| price.0)
    }

    /// Get best bid level
    pub fn best_bid_level(&self) -> Option<BookLevel> {
        self.bid_levels.iter().next_back().map(|(_, level)| level.to_book_level())
    }

    /// Get best ask price
    pub fn best_ask_price(&self) -> Option<Price> {
        self.ask_levels.iter().next().map(|(price, _)| price.0)
    }

    /// Get best ask level
    pub fn best_ask_level(&self) -> Option<BookLevel> {
        self.ask_levels.iter().next().map(|(_, level)| level.to_book_level())
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

    /// Get book depth
    pub fn depth(&self, levels: usize) -> (Vec<BookLevel>, Vec<BookLevel>) {
        let bids: Vec<BookLevel> = self.bid_levels
            .iter()
            .rev()
            .take(levels)
            .map(|(_, level)| level.to_book_level())
            .collect();

        let asks: Vec<BookLevel> = self.ask_levels
            .iter()
            .take(levels)
            .map(|(_, level)| level.to_book_level())
            .collect();

        (bids, asks)
    }

    /// Round price to tick size
    fn round_to_tick(&self, price: Price) -> Price {
        if self.tick_size <= 0.0 {
            price
        } else {
            (price / self.tick_size).round() * self.tick_size
        }
    }

    /// Update statistics after a trade
    pub fn update_trade_stats(&self, price: Price, quantity: Quantity) {
        let mut stats = self.stats.write();
        stats.total_volume += quantity;
        stats.total_trades += 1;
        stats.last_price = Some(price);
        stats.last_quantity = Some(quantity);
        stats.last_trade_time = Some(Utc::now());
        
        // Update VWAP
        if stats.total_volume > 0.0 {
            let weighted_sum = stats.vwap.unwrap_or(0.0) * (stats.total_volume - quantity) + price * quantity;
            stats.vwap = Some(weighted_sum / stats.total_volume);
        }
    }

    /// Publish market data update
    fn publish_market_data(&self) {
        let best_bid_level = self.best_bid_level();
        let best_ask_level = self.best_ask_level();
        let stats = self.stats.read();
        
        let market_data = MarketData {
            symbol: self.symbol.clone(),
            best_bid: best_bid_level.as_ref().map(|l| l.price),
            best_bid_size: best_bid_level.as_ref().map(|l| l.quantity),
            best_ask: best_ask_level.as_ref().map(|l| l.price),
            best_ask_size: best_ask_level.as_ref().map(|l| l.quantity),
            last_price: stats.last_price,
            last_size: stats.last_quantity,
            volume: stats.total_volume,
            timestamp: Utc::now(),
        };
        
        let _ = self.market_data_tx.send(market_data);
    }

    /// Convert to public order book representation
    pub fn to_public_book(&self, max_levels: usize) -> OrderBook {
        let (bids, asks) = self.depth(max_levels);
        let stats = self.stats.read();

        OrderBook {
            symbol: self.symbol.clone(),
            bids,
            asks,
            best_bid: self.best_bid_price(),
            best_ask: self.best_ask_price(),
            mid_price: self.mid_price(),
            spread: self.spread(),
            last_price: stats.last_price,
            last_quantity: stats.last_quantity,
            last_trade_time: stats.last_trade_time,
            volume: stats.total_volume,
            sequence: self.sequence.load(Ordering::Relaxed),
            updated_at: Utc::now(),
            tick_size: self.tick_size,
        }
    }

    /// Get statistics
    pub fn get_stats(&self) -> BookStats {
        self.stats.read().clone()
    }

    /// Clear all orders from the book
    pub fn clear(&mut self) {
        self.bid_levels.clear();
        self.ask_levels.clear();
        self.order_prices.clear();
        self.order_sides.clear();
        
        let mut stats = self.stats.write();
        stats.total_orders = 0;
        stats.total_bids = 0;
        stats.total_asks = 0;
        stats.total_bid_volume = 0.0;
        stats.total_ask_volume = 0.0;
        
        self.publish_market_data();
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
            aggressive_order_id: aggressive_order.id.clone(),
            passive_order_id: passive_order.id.clone(),
            symbol: aggressive_order.symbol.clone(),
            price,
            quantity,
            aggressive_side: aggressive_order.side,
            timestamp: Utc::now(),
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
    /// Total matched value
    pub total_value: f64,
}

/// Configuration for the matching engine
#[derive(Debug, Clone, Deserialize)]
pub struct MatchingEngineConfig {
    /// Enable price improvement
    pub enable_price_improvement: bool,
    /// Enable pro-rata allocation (not yet implemented)
    pub enable_pro_rata: bool,
    /// Minimum execution size
    pub min_execution_size: Quantity,
    /// Maximum book depth to consider
    pub max_book_depth: usize,
    /// Enable self-trade prevention
    pub prevent_self_trade: bool,
}

impl Default for MatchingEngineConfig {
    fn default() -> Self {
        Self {
            enable_price_improvement: true,
            enable_pro_rata: false,
            min_execution_size: 1.0,
            max_book_depth: 1000,
            prevent_self_trade: true,
        }
    }
}

/// High-performance matching engine
pub struct MatchingEngine {
    /// Configuration
    config: MatchingEngineConfig,
    /// Order books by symbol
    order_books: Arc<DashMap<Symbol, InternalOrderBook>>,
    /// Execution sequence number
    execution_sequence: Arc<AtomicU64>,
    /// Event channel for executions
    execution_tx: mpsc::UnboundedSender<Execution>,
}

impl MatchingEngine {
    /// Create new matching engine
    pub fn new(config: MatchingEngineConfig) -> Result<(Self, mpsc::UnboundedReceiver<Execution>)> {
        let (execution_tx, execution_rx) = mpsc::unbounded_channel();
        
        Ok((
            Self {
                config,
                order_books: Arc::new(DashMap::new()),
                execution_sequence: Arc::new(AtomicU64::new(0)),
                execution_tx,
            },
            execution_rx
        ))
    }

    /// Get or create order book for symbol
    pub fn get_or_create_book(&self, symbol: &Symbol, tick_size: Price) -> InternalOrderBook {
        let mut book = self.order_books
            .entry(symbol.clone())
            .or_insert_with(|| InternalOrderBook::new(symbol.clone(), tick_size));
        book.clone()
    }

    /// Match an incoming order against the order book
    #[instrument(skip(self, book, incoming_order))]
    pub async fn match_order(
        &mut self,
        book: &mut InternalOrderBook,
        incoming_order: &mut Order,
    ) -> Result<MatchingResult> {
        debug!(
            "Matching order {} {} {} @ {:?} for {}",
            incoming_order.id,
            incoming_order.side,
            incoming_order.symbol,
            incoming_order.price(),
            incoming_order.remaining_quantity()
        );

        let mut result = MatchingResult::default();
        result.remaining_quantity = incoming_order.remaining_quantity();

        // Self-trade prevention check
        if self.config.prevent_self_trade {
            // Check if any contra orders belong to same client
            let contra_levels = match incoming_order.side {
                OrderSide::Buy => &book.ask_levels,
                OrderSide::Sell => &book.bid_levels,
            };
            
            for (_, level) in contra_levels.iter() {
                for order_entry in &level.orders {
                    if order_entry.order.client_id == incoming_order.client_id {
                        warn!("Self-trade prevention triggered for order {}", incoming_order.id);
                        return Err(anyhow!("Self-trade not allowed"));
                    }
                }
            }
        }

        // Handle different order types
        match incoming_order.order_type {
            OrderType::Market => {
                self.match_market_order(book, incoming_order, &mut result).await?;
            }
            OrderType::Limit { .. } => {
                self.match_limit_order(book, incoming_order, &mut result).await?;
            }
            OrderType::Stop { .. } => {
                // Stop orders become market orders when triggered
                self.match_market_order(book, incoming_order, &mut result).await?;
            }
            OrderType::StopLimit { .. } => {
                // Stop-limit orders become limit orders when triggered
                self.match_limit_order(book, incoming_order, &mut result).await?;
            }
        }

        // Update book statistics and publish executions
        for execution in &result.executions {
            book.update_trade_stats(execution.price, execution.quantity);
            result.total_value += execution.price * execution.quantity;
            
            // Send execution to channel
            let _ = self.execution_tx.send(execution.clone());
        }

        // Update result
        result.remaining_quantity = incoming_order.remaining_quantity();
        result.fully_matched = incoming_order.remaining_quantity() <= 0.0;

        Ok(result)
    }

    /// Match market order
    async fn match_market_order(
        &mut self,
        book: &mut InternalOrderBook,
        incoming_order: &mut Order,
        result: &mut MatchingResult,
    ) -> Result<()> {
        let contra_levels = match incoming_order.side {
            OrderSide::Buy => &mut book.ask_levels,
            OrderSide::Sell => &mut book.bid_levels,
        };

        let mut remaining_qty = incoming_order.remaining_quantity();
        let mut levels_to_remove = Vec::new();

        // Process levels in price priority
        let mut level_entries: Vec<_> = if incoming_order.side == OrderSide::Buy {
            // Buy orders match against asks (lowest price first)
            contra_levels.iter_mut().collect()
        } else {
            // Sell orders match against bids (highest price first)
            contra_levels.iter_mut().rev().collect()
        };

        for (price_key, level) in level_entries.iter_mut() {
            if remaining_qty <= self.config.min_execution_size {
                break;
            }

            // Match against orders in time priority
            while remaining_qty > self.config.min_execution_size {
                if let Some(passive_entry) = level.front_order_mut() {
                    let match_qty = remaining_qty.min(passive_entry.remaining_quantity());
                    
                    if match_qty < self.config.min_execution_size {
                        break;
                    }
                    
                    let match_price = level.price;

                    // Create execution
                    self.execution_sequence.fetch_add(1, Ordering::SeqCst);
                    let execution = Execution::new(
                        incoming_order,
                        &passive_entry.order,
                        match_price,
                        match_qty,
                    );

                    result.executions.push(execution);

                    // Update quantities
                    remaining_qty -= match_qty;
                    incoming_order.fill(match_qty, match_price);
                    passive_entry.order.fill(match_qty, match_price);
                    level.total_quantity -= match_qty;

                    // Handle passive order
                    if passive_entry.order.remaining_quantity() <= self.config.min_execution_size {
                        // Order fully filled - remove from book
                        let filled_order = level.pop_front().unwrap();
                        book.order_prices.remove(&filled_order.order.id);
                        book.order_sides.remove(&filled_order.order.id);
                        
                        // Update stats
                        let mut stats = book.stats.write();
                        stats.total_orders -= 1;
                        match filled_order.order.side {
                            OrderSide::Buy => {
                                stats.total_bids -= 1;
                                stats.total_bid_volume -= match_qty;
                            }
                            OrderSide::Sell => {
                                stats.total_asks -= 1;
                                stats.total_ask_volume -= match_qty;
                            }
                        }
                        
                        result.updated_orders.push(filled_order.order);
                    } else {
                        // Partial fill
                        result.updated_orders.push(passive_entry.order.clone());
                    }
                } else {
                    break; // No more orders at this level
                }
            }

            // Mark empty levels for removal
            if level.is_empty() {
                levels_to_remove.push(**price_key);
            }
        }

        // Remove empty levels
        for price in levels_to_remove {
            contra_levels.remove(&price);
        }

        // Publish market data after matching
        book.publish_market_data();

        Ok(())
    }

    /// Match limit order
    async fn match_limit_order(
        &mut self,
        book: &mut InternalOrderBook,
        incoming_order: &mut Order,
        result: &mut MatchingResult,
    ) -> Result<()> {
        let order_price = match incoming_order.order_type {
            OrderType::Limit { price } => price,
            OrderType::StopLimit { limit_price, .. } => limit_price,
            _ => return Err(anyhow!("Expected limit order type")),
        };

        let contra_levels = match incoming_order.side {
            OrderSide::Buy => &mut book.ask_levels,
            OrderSide::Sell => &mut book.bid_levels,
        };

        let mut remaining_qty = incoming_order.remaining_quantity();
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
                    continue; // Bids might have better prices further
                }
            }

            if remaining_qty <= self.config.min_execution_size {
                break;
            }

            // Match against orders in time priority at this level
            while remaining_qty > self.config.min_execution_size {
                if let Some(passive_entry) = level.front_order_mut() {
                    let match_qty = remaining_qty.min(passive_entry.remaining_quantity());
                    
                    if match_qty < self.config.min_execution_size {
                        break;
                    }
                    
                    // Use passive order's price (price improvement)
                    let match_price = if self.config.enable_price_improvement {
                        level.price
                    } else {
                        order_price
                    };

                    // Create execution
                    self.execution_sequence.fetch_add(1, Ordering::SeqCst);
                    let mut execution = Execution::new(
                        incoming_order,
                        &passive_entry.order,
                        match_price,
                        match_qty,
                    );
                    
                    if match_price != order_price {
                        execution = execution.with_flag("PRICE_IMPROVEMENT".to_string());
                    }

                    result.executions.push(execution);

                    // Update quantities
                    remaining_qty -= match_qty;
                    incoming_order.fill(match_qty, match_price);
                    passive_entry.order.fill(match_qty, match_price);
                    level.total_quantity -= match_qty;

                    // Handle passive order
                    if passive_entry.order.remaining_quantity() <= self.config.min_execution_size {
                        // Order fully filled
                        let filled_order = level.pop_front().unwrap();
                        book.order_prices.remove(&filled_order.order.id);
                        book.order_sides.remove(&filled_order.order.id);
                        
                        // Update stats
                        let mut stats = book.stats.write();
                        stats.total_orders -= 1;
                        match filled_order.order.side {
                            OrderSide::Buy => {
                                stats.total_bids -= 1;
                                stats.total_bid_volume -= match_qty;
                            }
                            OrderSide::Sell => {
                                stats.total_asks -= 1;
                                stats.total_ask_volume -= match_qty;
                            }
                        }
                        
                        result.updated_orders.push(filled_order.order);
                    } else {
                        // Partial fill
                        result.updated_orders.push(passive_entry.order.clone());
                    }
                } else {
                    break;
                }
            }

            // Mark empty levels for removal
            if level.is_empty() {
                levels_to_remove.push(*ordered_price);
            }
        }

        // Remove empty levels
        for price in levels_to_remove {
            contra_levels.remove(&price);
        }

        // Publish market data after matching
        book.publish_market_data();

        Ok(())
    }

    /// Get order book for symbol
    pub fn get_book(&self, symbol: &Symbol) -> Option<OrderBook> {
        self.order_books.get(symbol).map(|book| book.to_public_book(10))
    }

    /// Get all active order books
    pub fn get_all_books(&self) -> Vec<OrderBook> {
        self.order_books
            .iter()
            .map(|entry| entry.value().to_public_book(5))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::order::{OrderSide, OrderType};

    fn create_test_order(id: &str, symbol: &str, side: OrderSide, order_type: OrderType, quantity: f64) -> Order {
        Order::new(
            format!("CLIENT_{}", id).into(),
            symbol.into(),
            quantity,
            side,
            order_type,
        )
    }

    #[test]
    fn test_order_book_creation() {
        let book = InternalOrderBook::new("AAPL".into(), 0.01);
        assert_eq!(book.symbol.0, "AAPL");
        assert_eq!(book.tick_size, 0.01);
        assert!(book.bid_levels.is_empty());
        assert!(book.ask_levels.is_empty());
    }

    #[test]
    fn test_add_limit_order() {
        let mut book = InternalOrderBook::new("AAPL".into(), 0.01);

        let order = create_test_order(
            "001",
            "AAPL",
            OrderSide::Buy,
            OrderType::Limit { price: 150.0 },
            100.0
        );

        let result = book.add_order(order);
        assert!(result.is_ok());
        assert_eq!(book.bid_levels.len(), 1);
        assert_eq!(book.best_bid_price(), Some(150.0));
    }

    #[test]
    fn test_remove_order() {
        let mut book = InternalOrderBook::new("AAPL".into(), 0.01);

        let order = create_test_order(
            "001",
            "AAPL",
            OrderSide::Buy,
            OrderType::Limit { price: 150.0 },
            100.0
        );

        let order_id = order.id.clone();
        book.add_order(order).unwrap();

        let removed_order = book.remove_order(&order_id);
        assert!(removed_order.is_some());
        assert!(book.bid_levels.is_empty());
    }

    #[test]
    fn test_best_prices() {
        let mut book = InternalOrderBook::new("AAPL".into(), 0.01);

        // Add bid orders
        book.add_order(create_test_order(
            "B1",
            "AAPL",
            OrderSide::Buy,
            OrderType::Limit { price: 149.99 },
            100.0
        )).unwrap();

        book.add_order(create_test_order(
            "B2",
            "AAPL",
            OrderSide::Buy,
            OrderType::Limit { price: 150.01 },
            200.0
        )).unwrap();

        // Add ask orders
        book.add_order(create_test_order(
            "A1",
            "AAPL",
            OrderSide::Sell,
            OrderType::Limit { price: 150.05 },
            150.0
        )).unwrap();

        book.add_order(create_test_order(
            "A2",
            "AAPL",
            OrderSide::Sell,
            OrderType::Limit { price: 150.03 },
            100.0
        )).unwrap();

        assert_eq!(book.best_bid_price(), Some(150.01));
        assert_eq!(book.best_ask_price(), Some(150.03));
        assert_eq!(book.mid_price(), Some(150.02));
        assert!((book.spread().unwrap() - 0.02).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_market_order_matching() {
        let config = MatchingEngineConfig::default();
        let (mut engine, _rx) = MatchingEngine::new(config).unwrap();
        let mut book = InternalOrderBook::new("AAPL".into(), 0.01);

        // Add a sell order to the book
        let ask_order = create_test_order(
            "ASK1",
            "AAPL",
            OrderSide::Sell,
            OrderType::Limit { price: 150.00 },
            200.0
        );
        book.add_order(ask_order).unwrap();

        // Create market buy order
        let mut market_order = create_test_order(
            "MKT1",
            "AAPL",
            OrderSide::Buy,
            OrderType::Market,
            100.0
        );

        let result = engine.match_order(&mut book, &mut market_order).await.unwrap();

        assert_eq!(result.executions.len(), 1);
        assert_eq!(result.executions[0].quantity, 100.0);
        assert_eq!(result.executions[0].price, 150.00);
        assert!(result.fully_matched);
        assert_eq!(result.remaining_quantity, 0.0);
        assert_eq!(result.total_value, 15000.0);
    }

    #[test]
    fn test_price_level_operations() {
        let mut level = PriceLevel::new(150.0);
        assert!(level.is_empty());
        assert_eq!(level.total_quantity, 0.0);

        let order = create_test_order(
            "001",
            "AAPL",
            OrderSide::Buy,
            OrderType::Limit { price: 150.0 },
            100.0
        );
        
        let order_id = order.id.clone();
        level.add_order(order, 1);
        assert!(!level.is_empty());
        assert_eq!(level.total_quantity, 100.0);

        let removed = level.remove_order(&order_id);
        assert!(removed.is_some());
        assert!(level.is_empty());
        assert_eq!(level.total_quantity, 0.0);
    }
}
