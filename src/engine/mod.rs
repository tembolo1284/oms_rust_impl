// src/engine/mod.rs - Order Processing Engine
//! # Order Processing Engine
//! 
//! Core engine that orchestrates order processing, validation, matching, and lifecycle management.
//! Designed for ultra-low latency with lock-free operations where possible.

pub mod matching;
pub mod lifecycle;
pub mod validator;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{broadcast, mpsc, oneshot, Mutex},
    time::interval,
};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::{
    core::{
        events::{OrderEvent, RiskEvent, SystemEvent},
        order::{Order, OrderSide, OrderStatus, OrderType},
        types::{ClientId, OrderId, Symbol, Venue},
    },
    storage::{OrderStorage, StorageManager},
};

/// Engine configuration
#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    /// Maximum orders per second
    pub max_orders_per_second: u32,
    /// Enable order validation
    pub enable_validation: bool,
    /// Enable risk checks
    pub enable_risk_checks: bool,
    /// Enable matching
    pub enable_matching: bool,
    /// Matching interval in milliseconds
    pub matching_interval_ms: u64,
    /// Order book depth
    pub book_depth: usize,
    /// Maximum pending orders
    pub max_pending_orders: usize,
    /// Enable market data publishing
    pub publish_market_data: bool,
    /// Session timeout
    pub session_timeout: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_orders_per_second: 10_000,
            enable_validation: true,
            enable_risk_checks: true,
            enable_matching: true,
            matching_interval_ms: 1, // 1ms for continuous matching
            book_depth: 10,
            max_pending_orders: 100_000,
            publish_market_data: true,
            session_timeout: Duration::from_secs(300),
        }
    }
}

/// Engine statistics
#[derive(Debug, Default, Clone, Serialize)]
pub struct EngineStats {
    pub orders_received: u64,
    pub orders_validated: u64,
    pub orders_rejected: u64,
    pub orders_matched: u64,
    pub orders_partially_filled: u64,
    pub orders_fully_filled: u64,
    pub orders_canceled: u64,
    pub total_volume: f64,
    pub avg_latency_us: u64,
    pub peak_latency_us: u64,
    pub active_orders: u64,
    pub active_symbols: usize,
}

/// Engine events for pub/sub
#[derive(Debug, Clone)]
pub enum EngineEvent {
    OrderAccepted { order: Order },
    OrderRejected { order_id: OrderId, reason: String },
    OrderMatched { order_id: OrderId, matched_id: OrderId, quantity: f64, price: f64 },
    OrderFilled { order_id: OrderId },
    OrderPartiallyFilled { order_id: OrderId, filled_quantity: f64 },
    OrderCanceled { order_id: OrderId },
    MarketData { symbol: Symbol, bid: Option<f64>, ask: Option<f64>, last: Option<f64> },
}

/// Order validation result
#[derive(Debug)]
pub enum ValidationResult {
    Valid,
    Invalid(String),
}

/// Risk check result
#[derive(Debug)]
pub enum RiskCheckResult {
    Approved,
    Rejected(String),
    RequiresApproval(String),
}

/// Main order processing engine
pub struct OrderEngine {
    /// Engine configuration
    config: EngineConfig,
    
    /// Storage layer
    storage: Arc<StorageManager>,
    
    /// Order books by symbol
    order_books: Arc<DashMap<Symbol, OrderBook>>,
    
    /// Pending orders queue
    pending_orders: Arc<Mutex<VecDeque<Order>>>,
    
    /// Active sessions
    sessions: Arc<DashMap<ClientId, Session>>,
    
    /// Engine state
    is_running: Arc<AtomicBool>,
    
    /// Statistics
    stats: Arc<RwLock<EngineStats>>,
    stats_atomic: Arc<EngineStatsAtomic>,
    
    /// Event channels
    event_tx: broadcast::Sender<EngineEvent>,
    order_tx: mpsc::UnboundedSender<Order>,
    order_rx: Arc<Mutex<mpsc::UnboundedReceiver<Order>>>,
    
    /// Command channel for control
    command_tx: mpsc::Sender<EngineCommand>,
    command_rx: Arc<Mutex<mpsc::Receiver<EngineCommand>>>,
}

/// Atomic counters for lock-free stats
struct EngineStatsAtomic {
    orders_received: AtomicU64,
    orders_validated: AtomicU64,
    orders_rejected: AtomicU64,
    orders_matched: AtomicU64,
    total_latency_ns: AtomicU64,
    peak_latency_ns: AtomicU64,
}

impl Default for EngineStatsAtomic {
    fn default() -> Self {
        Self {
            orders_received: AtomicU64::new(0),
            orders_validated: AtomicU64::new(0),
            orders_rejected: AtomicU64::new(0),
            orders_matched: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
            peak_latency_ns: AtomicU64::new(0),
        }
    }
}

/// Engine control commands
#[derive(Debug)]
pub enum EngineCommand {
    Start,
    Stop,
    Pause,
    Resume,
    ClearBook(Symbol),
    CancelAllOrders(ClientId),
    UpdateConfig(EngineConfig),
    GetStats(oneshot::Sender<EngineStats>),
}

/// Client session tracking
#[derive(Debug, Clone)]
pub struct Session {
    pub client_id: ClientId,
    pub connected_at: DateTime<Utc>,
    pub last_activity: DateTime<Utc>,
    pub orders_count: u64,
    pub filled_count: u64,
    pub rejected_count: u64,
    pub total_volume: f64,
}

impl OrderEngine {
    /// Create a new order engine
    pub async fn new(
        config: EngineConfig,
        storage: Arc<StorageManager>,
    ) -> Result<Self> {
        info!("Initializing order engine");
        
        let (event_tx, _) = broadcast::channel(10000);
        let (order_tx, order_rx) = mpsc::unbounded_channel();
        let (command_tx, command_rx) = mpsc::channel(100);
        
        Ok(Self {
            config: config.clone(),
            storage,
            order_books: Arc::new(DashMap::new()),
            pending_orders: Arc::new(Mutex::new(VecDeque::with_capacity(
                config.max_pending_orders
            ))),
            sessions: Arc::new(DashMap::new()),
            is_running: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(RwLock::new(EngineStats::default())),
            stats_atomic: Arc::new(EngineStatsAtomic::default()),
            event_tx,
            order_tx,
            order_rx: Arc::new(Mutex::new(order_rx)),
            command_tx,
            command_rx: Arc::new(Mutex::new(command_rx)),
        })
    }
    
    /// Submit an order to the engine
    #[instrument(skip(self, order))]
    pub async fn submit_order(&self, order: Order) -> Result<OrderId> {
        let start = Instant::now();
        let order_id = order.id.clone();
        
        // Update stats
        self.stats_atomic.orders_received.fetch_add(1, Ordering::Relaxed);
        
        // Check if engine is running
        if !self.is_running.load(Ordering::Acquire) {
            return Err(anyhow::anyhow!("Engine is not running"));
        }
        
        // Check pending queue capacity
        if self.pending_orders.lock().await.len() >= self.config.max_pending_orders {
            self.stats_atomic.orders_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(anyhow::anyhow!("Order queue is full"));
        }
        
        // Send to processing pipeline
        self.order_tx.send(order.clone())
            .context("Failed to send order to processing pipeline")?;
        
        // Update session
        self.update_session(&order.client_id).await;
        
        // Record latency
        let latency = start.elapsed().as_nanos() as u64;
        self.stats_atomic.total_latency_ns.fetch_add(latency, Ordering::Relaxed);
        self.update_peak_latency(latency);
        
        debug!("Order {} submitted in {}ns", order_id, latency);
        Ok(order_id)
    }
    
    /// Cancel an order
    #[instrument(skip(self))]
    pub async fn cancel_order(&self, order_id: &OrderId) -> Result<()> {
        // Retrieve order
        let order = self.storage
            .get_order(order_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Order not found"))?;
        
        // Check if order can be canceled
        if !matches!(
            order.status,
            OrderStatus::New | OrderStatus::Active | OrderStatus::PartiallyFilled
        ) {
            return Err(anyhow::anyhow!("Order cannot be canceled in current state"));
        }
        
        // Remove from order book
        if let Some(mut book) = self.order_books.get_mut(&order.symbol) {
            book.remove_order(&order);
        }
        
        // Update order status
        let mut updated_order = order.clone();
        updated_order.status = OrderStatus::Canceled;
        updated_order.updated_at = Utc::now();
        
        // Store updated order
        self.storage.update_order(updated_order).await?;
        
        // Publish event
        let _ = self.event_tx.send(EngineEvent::OrderCanceled {
            order_id: order_id.clone(),
        });
        
        // Update stats
        let mut stats = self.stats.write();
        stats.orders_canceled += 1;
        stats.active_orders = stats.active_orders.saturating_sub(1);
        
        Ok(())
    }
    
    /// Start the engine
    pub async fn start(&self) -> Result<()> {
        if self.is_running.swap(true, Ordering::AcqRel) {
            return Ok(()); // Already running
        }
        
        info!("Starting order engine");
        
        // Start worker tasks
        self.spawn_order_processor();
        self.spawn_matching_engine();
        self.spawn_command_handler();
        self.spawn_stats_aggregator();
        self.spawn_session_cleanup();
        
        Ok(())
    }
    
    /// Stop the engine
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping order engine");
        self.is_running.store(false, Ordering::Release);
        Ok(())
    }
    
    /// Get engine statistics
    pub async fn get_stats(&self) -> EngineStats {
        let stats = self.stats.read();
        let mut result = stats.clone();
        
        // Update from atomic counters
        result.orders_received = self.stats_atomic.orders_received.load(Ordering::Relaxed);
        result.orders_validated = self.stats_atomic.orders_validated.load(Ordering::Relaxed);
        result.orders_rejected = self.stats_atomic.orders_rejected.load(Ordering::Relaxed);
        result.orders_matched = self.stats_atomic.orders_matched.load(Ordering::Relaxed);
        
        // Calculate average latency
        if result.orders_received > 0 {
            let total_latency = self.stats_atomic.total_latency_ns.load(Ordering::Relaxed);
            result.avg_latency_us = (total_latency / result.orders_received) / 1000;
        }
        
        result.peak_latency_us = self.stats_atomic.peak_latency_ns.load(Ordering::Relaxed) / 1000;
        result.active_symbols = self.order_books.len();
        
        result
    }
    
    /// Subscribe to engine events
    pub fn subscribe(&self) -> broadcast::Receiver<EngineEvent> {
        self.event_tx.subscribe()
    }
    
    /// Send a control command
    pub async fn send_command(&self, command: EngineCommand) -> Result<()> {
        self.command_tx.send(command).await
            .context("Failed to send command")
    }
    
    /// Process a single order through the pipeline
    async fn process_order(&self, order: Order) -> Result<()> {
        let start = Instant::now();
        
        // Step 1: Validation
        if self.config.enable_validation {
            match self.validate_order(&order).await {
                ValidationResult::Invalid(reason) => {
                    self.reject_order(order, reason).await?;
                    return Ok(());
                }
                ValidationResult::Valid => {
                    self.stats_atomic.orders_validated.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        
        // Step 2: Risk checks
        if self.config.enable_risk_checks {
            match self.check_risk(&order).await {
                RiskCheckResult::Rejected(reason) => {
                    self.reject_order(order, reason).await?;
                    return Ok(());
                }
                RiskCheckResult::RequiresApproval(reason) => {
                    warn!("Order {} requires manual approval: {}", order.id, reason);
                    // Could implement manual approval workflow
                }
                RiskCheckResult::Approved => {}
            }
        }
        
        // Step 3: Accept order
        self.accept_order(order.clone()).await?;
        
        // Step 4: Add to order book for matching
        if self.config.enable_matching {
            self.add_to_order_book(order).await?;
        }
        
        // Record processing time
        let duration = start.elapsed();
        debug!("Order processed in {:?}", duration);
        
        Ok(())
    }
    
    /// Validate an order
    async fn validate_order(&self, order: &Order) -> ValidationResult {
        // Basic validations
        if order.quantity <= 0.0 {
            return ValidationResult::Invalid("Invalid quantity".to_string());
        }
        
        if let OrderType::Limit { price } = order.order_type {
            if price <= 0.0 {
                return ValidationResult::Invalid("Invalid limit price".to_string());
            }
        }
        
        if order.symbol.0.is_empty() {
            return ValidationResult::Invalid("Invalid symbol".to_string());
        }
        
        ValidationResult::Valid
    }
    
    /// Check risk for an order
    async fn check_risk(&self, order: &Order) -> RiskCheckResult {
        // Placeholder for risk checks
        // In production, this would check:
        // - Position limits
        // - Credit limits
        // - Margin requirements
        // - Regulatory restrictions
        
        RiskCheckResult::Approved
    }
    
    /// Accept an order
    async fn accept_order(&self, mut order: Order) -> Result<()> {
        order.status = OrderStatus::Active;
        order.updated_at = Utc::now();
        
        // Store order
        self.storage.insert_order(order.clone()).await?;
        
        // Publish event
        let _ = self.event_tx.send(EngineEvent::OrderAccepted {
            order: order.clone(),
        });
        
        // Update stats
        let mut stats = self.stats.write();
        stats.active_orders += 1;
        
        Ok(())
    }
    
    /// Reject an order
    async fn reject_order(&self, mut order: Order, reason: String) -> Result<()> {
        order.status = OrderStatus::Rejected;
        order.updated_at = Utc::now();
        
        // Store rejected order
        self.storage.insert_order(order.clone()).await?;
        
        // Publish event
        let _ = self.event_tx.send(EngineEvent::OrderRejected {
            order_id: order.id.clone(),
            reason: reason.clone(),
        });
        
        // Update stats
        self.stats_atomic.orders_rejected.fetch_add(1, Ordering::Relaxed);
        
        warn!("Order {} rejected: {}", order.id, reason);
        Ok(())
    }
    
    /// Add order to order book
    async fn add_to_order_book(&self, order: Order) -> Result<()> {
        let symbol = order.symbol.clone();
        
        // Get or create order book
        let mut book = self.order_books
            .entry(symbol.clone())
            .or_insert_with(|| OrderBook::new(symbol));
        
        // Add order to book
        book.add_order(order);
        
        Ok(())
    }
    
    /// Update session activity
    async fn update_session(&self, client_id: &ClientId) {
        self.sessions
            .entry(client_id.clone())
            .and_modify(|s| {
                s.last_activity = Utc::now();
                s.orders_count += 1;
            })
            .or_insert_with(|| Session {
                client_id: client_id.clone(),
                connected_at: Utc::now(),
                last_activity: Utc::now(),
                orders_count: 1,
                filled_count: 0,
                rejected_count: 0,
                total_volume: 0.0,
            });
    }
    
    /// Update peak latency
    fn update_peak_latency(&self, latency_ns: u64) {
        let mut peak = self.stats_atomic.peak_latency_ns.load(Ordering::Relaxed);
        while latency_ns > peak {
            match self.stats_atomic.peak_latency_ns.compare_exchange_weak(
                peak,
                latency_ns,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => peak = x,
            }
        }
    }
    
    /// Spawn order processor task
    fn spawn_order_processor(&self) {
        let engine = self.clone();
        tokio::spawn(async move {
            let mut order_rx = engine.order_rx.lock().await;
            
            while engine.is_running.load(Ordering::Acquire) {
                tokio::select! {
                    Some(order) = order_rx.recv() => {
                        if let Err(e) = engine.process_order(order).await {
                            error!("Failed to process order: {}", e);
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        // Check periodically
                    }
                }
            }
        });
    }
    
    /// Spawn matching engine task
    fn spawn_matching_engine(&self) {
        let engine = self.clone();
        let interval_ms = self.config.matching_interval_ms;
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(interval_ms));
            
            while engine.is_running.load(Ordering::Acquire) {
                interval.tick().await;
                
                // Process matching for each order book
                for mut book in engine.order_books.iter_mut() {
                    if let Some(matches) = book.match_orders() {
                        for (buy_order, sell_order, quantity, price) in matches {
                            engine.process_match(buy_order, sell_order, quantity, price).await;
                        }
                    }
                }
            }
        });
    }
    
    /// Process a match
    async fn process_match(
        &self,
        buy_order: Order,
        sell_order: Order,
        quantity: f64,
        price: f64,
    ) {
        // Update orders
        // ... (implementation depends on matching logic)
        
        // Publish event
        let _ = self.event_tx.send(EngineEvent::OrderMatched {
            order_id: buy_order.id.clone(),
            matched_id: sell_order.id.clone(),
            quantity,
            price,
        });
        
        // Update stats
        self.stats_atomic.orders_matched.fetch_add(2, Ordering::Relaxed);
    }
    
    /// Spawn command handler task
    fn spawn_command_handler(&self) {
        let engine = self.clone();
        
        tokio::spawn(async move {
            let mut command_rx = engine.command_rx.lock().await;
            
            while let Some(command) = command_rx.recv().await {
                match command {
                    EngineCommand::Stop => {
                        engine.is_running.store(false, Ordering::Release);
                        break;
                    }
                    EngineCommand::GetStats(tx) => {
                        let _ = tx.send(engine.get_stats().await);
                    }
                    // Handle other commands...
                    _ => {}
                }
            }
        });
    }
    
    /// Spawn stats aggregator task
    fn spawn_stats_aggregator(&self) {
        let engine = self.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(1));
            
            while engine.is_running.load(Ordering::Acquire) {
                interval.tick().await;
                
                // Aggregate stats from atomic counters
                let mut stats = engine.stats.write();
                stats.orders_received = engine.stats_atomic.orders_received.load(Ordering::Relaxed);
                stats.orders_validated = engine.stats_atomic.orders_validated.load(Ordering::Relaxed);
                stats.orders_rejected = engine.stats_atomic.orders_rejected.load(Ordering::Relaxed);
                stats.orders_matched = engine.stats_atomic.orders_matched.load(Ordering::Relaxed);
            }
        });
    }
    
    /// Spawn session cleanup task
    fn spawn_session_cleanup(&self) {
        let engine = self.clone();
        let timeout = self.config.session_timeout;
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60));
            
            while engine.is_running.load(Ordering::Acquire) {
                interval.tick().await;
                
                let now = Utc::now();
                engine.sessions.retain(|_, session| {
                    now.signed_duration_since(session.last_activity).to_std().unwrap() < timeout
                });
            }
        });
    }
}

// Clone implementation for engine
impl Clone for OrderEngine {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            storage: Arc::clone(&self.storage),
            order_books: Arc::clone(&self.order_books),
            pending_orders: Arc::clone(&self.pending_orders),
            sessions: Arc::clone(&self.sessions),
            is_running: Arc::clone(&self.is_running),
            stats: Arc::clone(&self.stats),
            stats_atomic: Arc::clone(&self.stats_atomic),
            event_tx: self.event_tx.clone(),
            order_tx: self.order_tx.clone(),
            order_rx: Arc::clone(&self.order_rx),
            command_tx: self.command_tx.clone(),
            command_rx: Arc::clone(&self.command_rx),
        }
    }
}

/// Simple order book structure (will be expanded in matching.rs)
pub struct OrderBook {
    pub symbol: Symbol,
    pub bids: Vec<Order>,
    pub asks: Vec<Order>,
    pub last_match_price: Option<f64>,
    pub last_match_time: Option<DateTime<Utc>>,
}

impl OrderBook {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            bids: Vec::new(),
            asks: Vec::new(),
            last_match_price: None,
            last_match_time: None,
        }
    }
    
    pub fn add_order(&mut self, order: Order) {
        match order.side {
            OrderSide::Buy => {
                self.bids.push(order);
                // Sort bids by price descending, then time ascending
                self.bids.sort_by(|a, b| {
                    let price_cmp = match (&a.order_type, &b.order_type) {
                        (OrderType::Limit { price: pa }, OrderType::Limit { price: pb }) => {
                            pb.partial_cmp(pa).unwrap()
                        }
                        _ => std::cmp::Ordering::Equal,
                    };
                    
                    if price_cmp == std::cmp::Ordering::Equal {
                        a.created_at.cmp(&b.created_at)
                    } else {
                        price_cmp
                    }
                });
            }
            OrderSide::Sell => {
                self.asks.push(order);
                // Sort asks by price ascending, then time ascending
                self.asks.sort_by(|a, b| {
                    let price_cmp = match (&a.order_type, &b.order_type) {
                        (OrderType::Limit { price: pa }, OrderType::Limit { price: pb }) => {
                            pa.partial_cmp(pb).unwrap()
                        }
                        _ => std::cmp::Ordering::Equal,
                    };
                    
                    if price_cmp == std::cmp::Ordering::Equal {
                        a.created_at.cmp(&b.created_at)
                    } else {
                        price_cmp
                    }
                });
            }
        }
    }
    
    pub fn remove_order(&mut self, order: &Order) {
        match order.side {
            OrderSide::Buy => {
                self.bids.retain(|o| o.id != order.id);
            }
            OrderSide::Sell => {
                self.asks.retain(|o| o.id != order.id);
            }
        }
    }
    
    pub fn match_orders(&mut self) -> Option<Vec<(Order, Order, f64, f64)>> {
        // Simple matching logic (will be expanded in matching.rs)
        let mut matches = Vec::new();
        
        while !self.bids.is_empty() && !self.asks.is_empty() {
            let bid = &self.bids[0];
            let ask = &self.asks[0];
            
            // Check if orders can match
            let can_match = match (&bid.order_type, &ask.order_type) {
                (OrderType::Market, _) | (_, OrderType::Market) => true,
                (OrderType::Limit { price: bid_price }, OrderType::Limit { price: ask_price }) => {
                    bid_price >= ask_price
                }
                _ => false,
            };
            
            if can_match {
                let match_quantity = bid.quantity.min(ask.quantity);
                let match_price = match &ask.order_type {
                    OrderType::Limit { price } => *price,
                    _ => match &bid.order_type {
                        OrderType::Limit { price } => *price,
                        _ => 0.0, // Market orders - need last traded price
                    }
                };
                
                matches.push((bid.clone(), ask.clone(), match_quantity, match_price));
                
                // Remove fully filled orders
                if bid.quantity <= match_quantity {
                    self.bids.remove(0);
                }
                if ask.quantity <= match_quantity {
                    self.asks.remove(0);
                }
                
                self.last_match_price = Some(match_price);
                self.last_match_time = Some(Utc::now());
            } else {
                break;
            }
        }
        
        if matches.is_empty() {
            None
        } else {
            Some(matches)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::order::{Order, OrderSide, OrderType},
        storage::StorageManagerConfig,
    };
    
    #[tokio::test]
    async fn test_engine_creation() {
        let config = EngineConfig::default();
        let storage_config = StorageManagerConfig {
            enable_persistence: false,
            ..Default::default()
        };
        let storage = Arc::new(StorageManager::new(storage_config).await.unwrap());
        
        let engine = OrderEngine::new(config, storage).await;
        assert!(engine.is_ok());
    }
    
    #[tokio::test]
    async fn test_order_submission() {
        let config = EngineConfig::default();
        let storage_config = StorageManagerConfig {
            enable_persistence: false,
            ..Default::default()
        };
        let storage = Arc::new(StorageManager::new(storage_config).await.unwrap());
        let engine = OrderEngine::new(config, storage).await.unwrap();
        
        // Start engine
        engine.start().await.unwrap();
        
        // Submit order
        let order = Order::new(
            "CLIENT_1".into(),
            "AAPL".into(),
            100.0,
            OrderSide::Buy,
            OrderType::Limit { price: 150.0 },
        );
        
        let result = engine.submit_order(order.clone()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), order.id);
        
        // Wait a bit for processing
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Check stats
        let stats = engine.get_stats().await;
        assert_eq!(stats.orders_received, 1);
        
        // Stop engine
        engine.stop().await.unwrap();
    }
    
    #[tokio::test]
    async fn test_order_book() {
        let mut book = OrderBook::new("AAPL".into());
        
        // Add buy orders
        let buy1 = Order::new(
            "C1".into(),
            "AAPL".into(),
            100.0,
            OrderSide::Buy,
            OrderType::Limit { price: 150.0 },
        );
        let buy2 = Order::new(
            "C2".into(),
            "AAPL".into(),
            50.0,
            OrderSide::Buy,
            OrderType::Limit { price: 151.0 },
        );
        
        book.add_order(buy1);
        book.add_order(buy2);
        
        // Add sell order that matches
        let sell1 = Order::new(
            "C3".into(),
            "AAPL".into(),
            75.0,
            OrderSide::Sell,
            OrderType::Limit { price: 149.0 },
        );
        
        book.add_order(sell1);
        
        // Check matching
        let matches = book.match_orders();
        assert!(matches.is_some());
        
        let matches = matches.unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].2, 75.0); // Match quantity
        assert_eq!(matches[0].3, 149.0); // Match price
    }
}
