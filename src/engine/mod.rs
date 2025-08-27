// src/engine/mod.rs - Engine Module & Order Processing Engine
//! Order processing engine and execution management
//!
//! This module contains the core order processing engine that handles:
//! - Order matching and execution
//! - Order lifecycle management
//! - Order validation and risk checks
//! - Market data integration
//! - Venue routing and connectivity
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
//! │   Order Input   │───▶│   Validator     │───▶│  Risk Engine    │
//! │   (REST/FIX)    │    │   (Business)    │    │   (Limits)      │
//! └─────────────────┘    └─────────────────┘    └─────────────────┘
//!                                                        │
//!                                                        ▼
//! ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
//! │   Execution     │◀───│  Matching       │◀───│   Lifecycle     │
//! │   (Fills)       │    │   Engine        │    │   (States)      │
//! └─────────────────┘    └─────────────────┘    └─────────────────┘
//!                                │
//!                                ▼
//!                    ┌─────────────────┐
//!                    │   Market Data   │
//!                    │   (Pricing)     │
//!                    └─────────────────┘
//! ```

use crate::{
    core::{
        events::{OrderEvent, OrderEventType},
        order::{Order, OrderRequest, OrderResponse, OrderStatus, OrderUpdate, OrderUpdateType},
        types::{AccountId, OrderId, Price, Quantity, Symbol, Timestamp, Venue},
    },
    OmsConfig, OmsError, OmsResult,
};
use chrono::Utc;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    time::sleep,
};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

// Module declarations
pub mod matching;
// pub mod lifecycle;   // Not yet implemented
// pub mod validator;   // Not yet implemented

// Re-export from submodules
pub use matching::{MatchingEngine, MatchingResult, OrderBook};

/// Order processing engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Maximum orders per second processing rate
    pub max_orders_per_second: u32,
    /// Order processing timeout in milliseconds
    pub processing_timeout_ms: u64,
    /// Enable order matching
    pub enable_matching: bool,
    /// Enable market making
    pub enable_market_making: bool,
    /// Maximum order book depth
    pub max_book_depth: usize,
    /// Enable order aggregation
    pub enable_aggregation: bool,
    /// Tick size for price levels
    pub tick_size: Price,
    /// Minimum order size
    pub min_order_size: Quantity,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_orders_per_second: 10_000,
            processing_timeout_ms: 1000,
            enable_matching: true,
            enable_market_making: false,
            max_book_depth: 1000,
            enable_aggregation: true,
            tick_size: 0.01,
            min_order_size: 1.0,
        }
    }
}

/// Order processing command
#[derive(Debug, Clone)]
pub enum OrderCommand {
    /// Submit new order
    SubmitOrder {
        order: Order,
        response_tx: oneshot::Sender<OmsResult<OrderResponse>>,
    },
    /// Cancel existing order
    CancelOrder {
        order_id: OrderId,
        response_tx: oneshot::Sender<OmsResult<OrderResponse>>,
    },
    /// Replace/modify existing order
    ReplaceOrder {
        order_id: OrderId,
        new_order: Order,
        response_tx: oneshot::Sender<OmsResult<OrderResponse>>,
    },
    /// Query order status
    QueryOrder {
        order_id: OrderId,
        response_tx: oneshot::Sender<OmsResult<Order>>,
    },
    /// Get order book snapshot
    GetOrderBook {
        symbol: Symbol,
        response_tx: oneshot::Sender<OmsResult<OrderBook>>,
    },
}

/// Order processing statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineStats {
    /// Total orders processed
    pub orders_processed: u64,
    /// Orders processed per second
    pub orders_per_second: f64,
    /// Total executions
    pub total_executions: u64,
    /// Total volume traded
    pub total_volume: Quantity,
    /// Total value traded
    pub total_value: Price,
    /// Average processing latency in microseconds
    pub avg_latency_us: f64,
    /// Active orders count
    pub active_orders: u64,
    /// Active symbols count
    pub active_symbols: u64,
    /// Engine uptime in seconds
    pub uptime_seconds: u64,
    /// Last update timestamp
    pub last_updated: Timestamp,
}

/// Main order processing engine
pub struct OmsEngine {
    /// Engine configuration
    config: Arc<EngineConfig>,
    /// Order storage
    orders: Arc<DashMap<OrderId, Order>>,
    /// Order books by symbol
    order_books: Arc<DashMap<Symbol, Arc<RwLock<OrderBook>>>>,
    /// Matching engine
    matching_engine: Arc<matching::MatchingEngine>,
    /// Command channel for order processing
    command_tx: mpsc::UnboundedSender<OrderCommand>,
    /// Event broadcast channel
    event_tx: broadcast::Sender<OrderEvent>,
    /// Order update broadcast channel
    update_tx: broadcast::Sender<OrderUpdate>,
    /// Engine statistics
    stats: Arc<RwLock<EngineStats>>,
    /// Sequence number generator
    sequence_generator: Arc<AtomicU64>,
    /// Engine start time
    start_time: Instant,
}

impl OmsEngine {
    /// Create a new OMS engine
    #[instrument(skip(oms_config))]
    pub async fn new(oms_config: Arc<OmsConfig>) -> OmsResult<Self> {
        info!("Initializing OMS Engine");
        
        let engine_config = Arc::new(EngineConfig::default());
        let orders = Arc::new(DashMap::new());
        let order_books = Arc::new(DashMap::new());
        let matching_engine = Arc::new(matching::MatchingEngine::new(engine_config.clone())?);
        
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (event_tx, _) = broadcast::channel(10000);
        let (update_tx, _) = broadcast::channel(10000);
        
        let stats = Arc::new(RwLock::new(EngineStats {
            orders_processed: 0,
            orders_per_second: 0.0,
            total_executions: 0,
            total_volume: 0.0,
            total_value: 0.0,
            avg_latency_us: 0.0,
            active_orders: 0,
            active_symbols: 0,
            uptime_seconds: 0,
            last_updated: Utc::now(),
        }));
        
        let engine = Self {
            config: engine_config,
            orders: orders.clone(),
            order_books: order_books.clone(),
            matching_engine: matching_engine.clone(),
            command_tx,
            event_tx: event_tx.clone(),
            update_tx: update_tx.clone(),
            stats: stats.clone(),
            sequence_generator: Arc::new(AtomicU64::new(1)),
            start_time: Instant::now(),
        };
        
        // Start the order processing task
        engine.start_processing_task(command_rx).await;
        
        // Start statistics update task
        engine.start_stats_task().await;
        
        info!("OMS Engine initialized successfully");
        Ok(engine)
    }
    
    /// Submit a new order
    #[instrument(skip(self, order))]
    pub async fn submit_order(&self, order: Order) -> OmsResult<OrderResponse> {
        let start_time = Instant::now();
        
        debug!("Submitting order: {}", order.id);
        
        let (response_tx, response_rx) = oneshot::channel();
        
        self.command_tx
            .send(OrderCommand::SubmitOrder { order, response_tx })
            .map_err(|_| OmsError::Internal("Failed to send submit command".to_string()))?;
        
        let result = response_rx
            .await
            .map_err(|_| OmsError::Internal("Failed to receive submit response".to_string()))?;
        
        // Update latency statistics
        let latency = start_time.elapsed().as_micros() as f64;
        self.update_latency_stats(latency);
        
        result
    }
    
    /// Cancel an existing order
    #[instrument(skip(self))]
    pub async fn cancel_order(&self, order_id: OrderId) -> OmsResult<OrderResponse> {
        debug!("Canceling order: {}", order_id);
        
        let (response_tx, response_rx) = oneshot::channel();
        
        self.command_tx
            .send(OrderCommand::CancelOrder { order_id, response_tx })
            .map_err(|_| OmsError::Internal("Failed to send cancel command".to_string()))?;
        
        response_rx
            .await
            .map_err(|_| OmsError::Internal("Failed to receive cancel response".to_string()))?
    }
    
    /// Replace an existing order
    #[instrument(skip(self, new_order))]
    pub async fn replace_order(&self, order_id: OrderId, new_order: Order) -> OmsResult<OrderResponse> {
        debug!("Replacing order: {}", order_id);
        
        let (response_tx, response_rx) = oneshot::channel();
        
        self.command_tx
            .send(OrderCommand::ReplaceOrder { order_id, new_order, response_tx })
            .map_err(|_| OmsError::Internal("Failed to send replace command".to_string()))?;
        
        response_rx
            .await
            .map_err(|_| OmsError::Internal("Failed to receive replace response".to_string()))?
    }
    
    /// Query order status
    #[instrument(skip(self))]
    pub async fn query_order(&self, order_id: OrderId) -> OmsResult<Order> {
        debug!("Querying order: {}", order_id);
        
        let (response_tx, response_rx) = oneshot::channel();
        
        self.command_tx
            .send(OrderCommand::QueryOrder { order_id, response_tx })
            .map_err(|_| OmsError::Internal("Failed to send query command".to_string()))?;
        
        response_rx
            .await
            .map_err(|_| OmsError::Internal("Failed to receive query response".to_string()))?
    }
    
    /// Get order book snapshot
    #[instrument(skip(self))]
    pub async fn get_order_book(&self, symbol: &str) -> OmsResult<OrderBook> {
        debug!("Getting order book for symbol: {}", symbol);
        
        let (response_tx, response_rx) = oneshot::channel();
        
        self.command_tx
            .send(OrderCommand::GetOrderBook { 
                symbol: symbol.to_string(), 
                response_tx 
            })
            .map_err(|_| OmsError::Internal("Failed to send get book command".to_string()))?;
        
        response_rx
            .await
            .map_err(|_| OmsError::Internal("Failed to receive get book response".to_string()))?
    }
    
    /// Subscribe to order events
    pub fn subscribe_events(&self) -> broadcast::Receiver<OrderEvent> {
        self.event_tx.subscribe()
    }
    
    /// Subscribe to order updates
    pub fn subscribe_updates(&self) -> broadcast::Receiver<OrderUpdate> {
        self.update_tx.subscribe()
    }
    
    /// Get engine statistics
    pub fn get_stats(&self) -> EngineStats {
        let mut stats = self.stats.read().clone();
        stats.uptime_seconds = self.start_time.elapsed().as_secs();
        stats.active_orders = self.orders.len() as u64;
        stats.active_symbols = self.order_books.len() as u64;
        stats.last_updated = Utc::now();
        stats
    }
    
    /// Start the order processing task
    async fn start_processing_task(&self, mut command_rx: mpsc::UnboundedReceiver<OrderCommand>) {
        let orders = self.orders.clone();
        let order_books = self.order_books.clone();
        let matching_engine = self.matching_engine.clone();
        let event_tx = self.event_tx.clone();
        let update_tx = self.update_tx.clone();
        let stats = self.stats.clone();
        let sequence_generator = self.sequence_generator.clone();
        let config = self.config.clone();
        
        tokio::spawn(async move {
            info!("Order processing task started");
            
            while let Some(command) = command_rx.recv().await {
                let start_time = Instant::now();
                
                match command {
                    OrderCommand::SubmitOrder { mut order, response_tx } => {
                        let result = Self::process_submit_order(
                            &mut order,
                            &orders,
                            &order_books,
                            &matching_engine,
                            &event_tx,
                            &update_tx,
                            &sequence_generator,
                            &config,
                        ).await;
                        
                        let _ = response_tx.send(result);
                    }
                    
                    OrderCommand::CancelOrder { order_id, response_tx } => {
                        let result = Self::process_cancel_order(
                            order_id,
                            &orders,
                            &order_books,
                            &event_tx,
                            &update_tx,
                            &sequence_generator,
                        ).await;
                        
                        let _ = response_tx.send(result);
                    }
                    
                    OrderCommand::ReplaceOrder { order_id, new_order, response_tx } => {
                        let result = Self::process_replace_order(
                            order_id,
                            new_order,
                            &orders,
                            &order_books,
                            &matching_engine,
                            &event_tx,
                            &update_tx,
                            &sequence_generator,
                        ).await;
                        
                        let _ = response_tx.send(result);
                    }
                    
                    OrderCommand::QueryOrder { order_id, response_tx } => {
                        let result = orders.get(&order_id)
                            .map(|order| order.clone())
                            .ok_or_else(|| OmsError::OrderValidation("Order not found".to_string()));
                        
                        let _ = response_tx.send(result);
                    }
                    
                    OrderCommand::GetOrderBook { symbol, response_tx } => {
                        let result = order_books.get(&symbol)
                            .map(|book| book.read().clone())
                            .ok_or_else(|| OmsError::OrderValidation("Order book not found".to_string()));
                        
                        let _ = response_tx.send(result);
                    }
                }
                
                // Update processing statistics
                let processing_time = start_time.elapsed().as_micros() as f64;
                let mut stats_guard = stats.write();
                stats_guard.orders_processed += 1;
                stats_guard.avg_latency_us = (stats_guard.avg_latency_us * 0.95) + (processing_time * 0.05);
            }
            
            warn!("Order processing task terminated");
        });
    }
    
    /// Process submit order command
    #[instrument(skip(order, orders, order_books, matching_engine, event_tx, update_tx, sequence_generator, config))]
    async fn process_submit_order(
        order: &mut Order,
        orders: &Arc<DashMap<OrderId, Order>>,
        order_books: &Arc<DashMap<Symbol, Arc<RwLock<OrderBook>>>>,
        matching_engine: &Arc<matching::MatchingEngine>,
        event_tx: &broadcast::Sender<OrderEvent>,
        update_tx: &broadcast::Sender<OrderUpdate>,
        sequence_generator: &Arc<AtomicU64>,
        config: &Arc<EngineConfig>,
    ) -> OmsResult<OrderResponse> {
        // Validate order
        order.validate()?;
        
        // Store order
        orders.insert(order.id, order.clone());
        
        // Create order event
        let sequence = sequence_generator.fetch_add(1, Ordering::Relaxed);
        let event = OrderEvent::order_created(
            order.id,
            order.client_order_id.clone(),
            order.account.clone(),
            order.symbol.clone(),
            sequence,
        );
        
        let _ = event_tx.send(event);
        
        // Get or create order book
        let order_book = order_books
            .entry(order.symbol.clone())
            .or_insert_with(|| Arc::new(RwLock::new(OrderBook::new(order.symbol.clone(), config.tick_size))))
            .clone();
        
        // Try to match the order if matching is enabled
        if config.enable_matching {
            let matching_result = {
                let mut book = order_book.write();
                matching_engine.match_order(&mut book, order).await?
            };
            
            // Process matching results
            for execution in matching_result.executions {
                // Apply fill to order
                order.apply_fill(execution.quantity, execution.price)?;
                
                // Update order in storage
                orders.insert(order.id, order.clone());
                
                // Send order update
                let update = OrderUpdate {
                    order_id: order.id,
                    status: order.status,
                    filled_quantity: order.filled_quantity,
                    avg_price: order.avg_price,
                    last_price: Some(execution.price),
                    last_quantity: Some(execution.quantity),
                    message: Some("Order filled".to_string()),
                    updated_at: Utc::now(),
                    update_type: if order.status == OrderStatus::Filled {
                        OrderUpdateType::Fill
                    } else {
                        OrderUpdateType::PartialFill
                    },
                };
                
                let _ = update_tx.send(update);
            }
        }
        
        Ok(OrderResponse::success(order.clone(), "Order submitted successfully".to_string()))
    }
    
    /// Process cancel order command
    #[instrument(skip(orders, order_books, event_tx, update_tx, sequence_generator))]
    async fn process_cancel_order(
        order_id: OrderId,
        orders: &Arc<DashMap<OrderId, Order>>,
        order_books: &Arc<DashMap<Symbol, Arc<RwLock<OrderBook>>>>,
        event_tx: &broadcast::Sender<OrderEvent>,
        update_tx: &broadcast::Sender<OrderUpdate>,
        sequence_generator: &Arc<AtomicU64>,
    ) -> OmsResult<OrderResponse> {
        let mut order = orders.get_mut(&order_id)
            .ok_or_else(|| OmsError::OrderValidation("Order not found".to_string()))?;
        
        if !order.can_cancel() {
            return Err(OmsError::OrderValidation("Order cannot be canceled".to_string()));
        }
        
        let previous_status = order.status;
        order.update_status(OrderStatus::Canceled)?;
        
        // Remove from order book if it exists
        if let Some(book_entry) = order_books.get(&order.symbol) {
            let mut book = book_entry.write();
            book.remove_order(order_id);
        }
        
        // Create cancel event
        let sequence = sequence_generator.fetch_add(1, Ordering::Relaxed);
        let event = OrderEvent::order_canceled(
            order.id,
            order.client_order_id.clone(),
            order.account.clone(),
            order.symbol.clone(),
            previous_status,
            order.venue.clone(),
            sequence,
        );
        
        let _ = event_tx.send(event);
        
        // Send order update
        let update = OrderUpdate {
            order_id: order.id,
            status: order.status,
            filled_quantity: order.filled_quantity,
            avg_price: order.avg_price,
            last_price: None,
            last_quantity: None,
            message: Some("Order canceled".to_string()),
            updated_at: order.updated_at,
            update_type: OrderUpdateType::Canceled,
        };
        
        let _ = update_tx.send(update);
        
        Ok(OrderResponse::success(order.clone(), "Order canceled successfully".to_string()))
    }
    
    /// Process replace order command
    #[instrument(skip(new_order, orders, order_books, matching_engine, event_tx, update_tx, sequence_generator))]
    async fn process_replace_order(
        order_id: OrderId,
        mut new_order: Order,
        orders: &Arc<DashMap<OrderId, Order>>,
        order_books: &Arc<DashMap<Symbol, Arc<RwLock<OrderBook>>>>,
        matching_engine: &Arc<matching::MatchingEngine>,
        event_tx: &broadcast::Sender<OrderEvent>,
        update_tx: &broadcast::Sender<OrderUpdate>,
        sequence_generator: &Arc<AtomicU64>,
    ) -> OmsResult<OrderResponse> {
        let mut order = orders.get_mut(&order_id)
            .ok_or_else(|| OmsError::OrderValidation("Order not found".to_string()))?;
        
        if !order.can_replace() {
            return Err(OmsError::OrderValidation("Order cannot be replaced".to_string()));
        }
        
        // Preserve certain fields from original order
        new_order.id = order.id;
        new_order.created_at = order.created_at;
        new_order.filled_quantity = order.filled_quantity;
        new_order.avg_price = order.avg_price;
        new_order.leaves_quantity = new_order.quantity - new_order.filled_quantity;
        new_order.updated_at = Utc::now();
        
        // Validate new order
        new_order.validate()?;
        
        // Replace order in storage
        *order = new_order.clone();
        
        // Update in order book
        if let Some(book_entry) = order_books.get(&order.symbol) {
            let mut book = book_entry.write();
            book.remove_order(order_id);
            book.add_order(new_order.clone())?;
        }
        
        // Create replace event
        let sequence = sequence_generator.fetch_add(1, Ordering::Relaxed);
        // Note: We'd need a proper replace event type in the events module
        
        // Send order update
        let update = OrderUpdate {
            order_id: order.id,
            status: order.status,
            filled_quantity: order.filled_quantity,
            avg_price: order.avg_price,
            last_price: None,
            last_quantity: None,
            message: Some("Order replaced".to_string()),
            updated_at: order.updated_at,
            update_type: OrderUpdateType::Replaced,
        };
        
        let _ = update_tx.send(update);
        
        Ok(OrderResponse::success(order.clone(), "Order replaced successfully".to_string()))
    }
    
    /// Start statistics update task
    async fn start_stats_task(&self) {
        let stats = self.stats.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let mut last_processed = 0u64;
            
            loop {
                interval.tick().await;
                
                let mut stats_guard = stats.write();
                let current_processed = stats_guard.orders_processed;
                stats_guard.orders_per_second = (current_processed - last_processed) as f64;
                last_processed = current_processed;
            }
        });
    }
    
    /// Update latency statistics
    fn update_latency_stats(&self, latency_us: f64) {
        let mut stats = self.stats.write();
        stats.avg_latency_us = (stats.avg_latency_us * 0.95) + (latency_us * 0.05);
    }
}

/// Order processing result
#[derive(Debug)]
pub struct ProcessingResult {
    /// Processing success
    pub success: bool,
    /// Result message
    pub message: String,
    /// Processing latency in microseconds
    pub latency_us: u64,
    /// Generated events
    pub events: Vec<OrderEvent>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::order::{OrderBuilder, OrderSide, OrderType};
    
    #[tokio::test]
    async fn test_engine_creation() {
        let config = Arc::new(OmsConfig::default());
        let engine = OmsEngine::new(config).await;
        assert!(engine.is_ok());
    }
    
    #[tokio::test]
    async fn test_order_submission() {
        let config = Arc::new(OmsConfig::default());
        let engine = OmsEngine::new(config).await.unwrap();
        
        let order = OrderBuilder::new()
            .client_order_id("TEST_001")
            .symbol("AAPL")
            .side(OrderSide::Buy)
            .order_type(OrderType::Limit)
            .quantity(100.0)
            .limit_price(150.0)
            .account("TEST_ACCOUNT")
            .build()
            .unwrap();
        
        let response = engine.submit_order(order).await;
        assert!(response.is_ok());
        
        let response = response.unwrap();
        assert!(response.success);
    }
    
    #[tokio::test]
    async fn test_order_query() {
        let config = Arc::new(OmsConfig::default());
        let engine = OmsEngine::new(config).await.unwrap();
        
        let order = OrderBuilder::new()
            .client_order_id("TEST_002")
            .symbol("AAPL")
            .side(OrderSide::Buy)
            .order_type(OrderType::Limit)
            .quantity(100.0)
            .limit_price(150.0)
            .account("TEST_ACCOUNT")
            .build()
            .unwrap();
        
        let order_id = order.id;
        let _ = engine.submit_order(order).await.unwrap();
        
        let queried_order = engine.query_order(order_id).await;
        assert!(queried_order.is_ok());
        assert_eq!(queried_order.unwrap().id, order_id);
    }
    
    #[tokio::test]
    async fn test_engine_stats() {
        let config = Arc::new(OmsConfig::default());
        let engine = OmsEngine::new(config).await.unwrap();
        
        let stats = engine.get_stats();
        assert_eq!(stats.orders_processed, 0);
        assert!(stats.uptime_seconds >= 0);
    }
}
