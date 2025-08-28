// src/storage/memory.rs - High-Performance In-Memory Storage
//! # In-Memory Storage Backend
//! 
//! Lock-free, concurrent in-memory storage using DashMap for ultra-low latency access.
//! Provides multiple indexes for efficient queries and supports millions of orders.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::{DashMap, DashSet};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, BTreeMap},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::sync::broadcast;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::core::{
    events::OrderEvent,
    order::{Order, OrderStatus},
    types::{ClientId, OrderId, Symbol},
};

use super::{OrderStorage, StorageError, StorageMetrics};

/// Configuration for in-memory storage
#[derive(Debug, Clone, Deserialize)]
pub struct MemoryConfig {
    /// Maximum number of orders to keep in memory
    pub max_orders: usize,
    /// Enable metrics collection
    pub enable_metrics: bool,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            max_orders: 1_000_000,
            enable_metrics: true,
        }
    }
}

/// Statistics for memory storage
#[derive(Debug, Clone, Default)]
pub struct MemoryStats {
    pub total_orders: AtomicU64,
    pub active_orders: AtomicU64,
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub updates: AtomicU64,
    pub deletes: AtomicU64,
    pub cache_hits: AtomicU64,
    pub avg_read_ns: AtomicU64,
    pub avg_write_ns: AtomicU64,
    pub memory_bytes: AtomicUsize,
}

/// In-memory storage implementation using lock-free data structures
pub struct InMemoryStorage {
    /// Primary storage - order ID to Order mapping
    orders: Arc<DashMap<OrderId, Order>>,
    
    /// Secondary indexes for fast queries
    client_index: Arc<DashMap<ClientId, DashSet<OrderId>>>,
    symbol_index: Arc<DashMap<Symbol, DashSet<OrderId>>>,
    status_index: Arc<DashMap<OrderStatus, DashSet<OrderId>>>,
    
    /// Time-based index for range queries (using BTreeMap for ordering)
    time_index: Arc<RwLock<BTreeMap<DateTime<Utc>, DashSet<OrderId>>>>,
    
    /// Active orders for quick access
    active_orders: Arc<DashSet<OrderId>>,
    
    /// Configuration
    config: MemoryConfig,
    
    /// Statistics
    stats: Arc<MemoryStats>,
    
    /// Event broadcast for real-time updates
    event_tx: broadcast::Sender<OrderEvent>,
}

impl InMemoryStorage {
    /// Create a new in-memory storage instance
    pub fn new(config: MemoryConfig) -> Self {
        let (event_tx, _) = broadcast::channel(10000);
        
        info!("Initializing in-memory storage with capacity: {}", config.max_orders);
        
        Self {
            orders: Arc::new(DashMap::with_capacity(config.max_orders)),
            client_index: Arc::new(DashMap::new()),
            symbol_index: Arc::new(DashMap::new()),
            status_index: Arc::new(DashMap::new()),
            time_index: Arc::new(RwLock::new(BTreeMap::new())),
            active_orders: Arc::new(DashSet::new()),
            config,
            stats: Arc::new(MemoryStats::default()),
            event_tx,
        }
    }
    
    /// Subscribe to order events
    pub fn subscribe(&self) -> broadcast::Receiver<OrderEvent> {
        self.event_tx.subscribe()
    }
    
    /// Update all indexes for an order
    #[instrument(skip(self, order))]
    fn update_indexes(&self, order: &Order) {
        let order_id = order.id.clone();
        
        // Update client index
        self.client_index
            .entry(order.client_id.clone())
            .or_insert_with(DashSet::new)
            .insert(order_id.clone());
        
        // Update symbol index
        self.symbol_index
            .entry(order.symbol.clone())
            .or_insert_with(DashSet::new)
            .insert(order_id.clone());
        
        // Update status index
        self.status_index
            .entry(order.status.clone())
            .or_insert_with(DashSet::new)
            .insert(order_id.clone());
        
        // Update time index
        {
            let mut time_index = self.time_index.write();
            time_index
                .entry(order.created_at)
                .or_insert_with(DashSet::new)
                .insert(order_id.clone());
        }
        
        // Update active orders
        match order.status {
            OrderStatus::New | OrderStatus::Active | OrderStatus::PartiallyFilled => {
                self.active_orders.insert(order_id);
            }
            _ => {
                self.active_orders.remove(&order_id);
            }
        }
    }
    
    /// Remove order from all indexes
    #[instrument(skip(self))]
    fn remove_from_indexes(&self, order: &Order) {
        let order_id = &order.id;
        
        // Remove from client index
        if let Some(mut client_orders) = self.client_index.get_mut(&order.client_id) {
            client_orders.remove(order_id);
        }
        
        // Remove from symbol index
        if let Some(mut symbol_orders) = self.symbol_index.get_mut(&order.symbol) {
            symbol_orders.remove(order_id);
        }
        
        // Remove from status index
        if let Some(mut status_orders) = self.status_index.get_mut(&order.status) {
            status_orders.remove(order_id);
        }
        
        // Remove from time index
        {
            let mut time_index = self.time_index.write();
            if let Some(time_orders) = time_index.get_mut(&order.created_at) {
                time_orders.remove(order_id);
            }
        }
        
        // Remove from active orders
        self.active_orders.remove(order_id);
    }
    
    /// Check if storage is at capacity
    fn check_capacity(&self) -> Result<(), StorageError> {
        if self.orders.len() >= self.config.max_orders {
            warn!("Storage capacity exceeded: {} orders", self.orders.len());
            return Err(StorageError::CapacityExceeded);
        }
        Ok(())
    }
    
    /// Record operation timing
    fn record_timing(&self, operation: &str, duration_ns: u64) {
        if self.config.enable_metrics {
            match operation {
                "read" => {
                    let reads = self.stats.reads.fetch_add(1, Ordering::Relaxed) + 1;
                    let current_avg = self.stats.avg_read_ns.load(Ordering::Relaxed);
                    let new_avg = ((current_avg * (reads - 1)) + duration_ns) / reads;
                    self.stats.avg_read_ns.store(new_avg, Ordering::Relaxed);
                }
                "write" => {
                    let writes = self.stats.writes.fetch_add(1, Ordering::Relaxed) + 1;
                    let current_avg = self.stats.avg_write_ns.load(Ordering::Relaxed);
                    let new_avg = ((current_avg * (writes - 1)) + duration_ns) / writes;
                    self.stats.avg_write_ns.store(new_avg, Ordering::Relaxed);
                }
                _ => {}
            }
        }
    }
    
    /// Estimate memory usage
    fn estimate_memory_usage(&self) -> usize {
        // Rough estimation: 1KB per order + overhead
        self.orders.len() * 1024
    }
    
    /// Get statistics
    pub async fn get_stats(&self) -> MemoryStats {
        MemoryStats {
            total_orders: AtomicU64::new(self.orders.len() as u64),
            active_orders: AtomicU64::new(self.active_orders.len() as u64),
            reads: AtomicU64::new(self.stats.reads.load(Ordering::Relaxed)),
            writes: AtomicU64::new(self.stats.writes.load(Ordering::Relaxed)),
            updates: AtomicU64::new(self.stats.updates.load(Ordering::Relaxed)),
            deletes: AtomicU64::new(self.stats.deletes.load(Ordering::Relaxed)),
            cache_hits: AtomicU64::new(self.stats.cache_hits.load(Ordering::Relaxed)),
            avg_read_ns: AtomicU64::new(self.stats.avg_read_ns.load(Ordering::Relaxed)),
            avg_write_ns: AtomicU64::new(self.stats.avg_write_ns.load(Ordering::Relaxed)),
            memory_bytes: AtomicUsize::new(self.estimate_memory_usage()),
        }
    }
    
    /// Perform garbage collection to free memory
    pub async fn garbage_collect(&self) -> Result<usize, StorageError> {
        info!("Starting garbage collection");
        let start = Instant::now();
        
        // Find and remove canceled/rejected orders older than 1 hour
        let cutoff = Utc::now() - chrono::Duration::hours(1);
        let mut removed = 0;
        
        // Collect orders to remove
        let orders_to_remove: Vec<OrderId> = self.orders
            .iter()
            .filter(|entry| {
                let order = entry.value();
                (order.status == OrderStatus::Canceled || 
                 order.status == OrderStatus::Rejected) &&
                order.updated_at < cutoff
            })
            .map(|entry| entry.key().clone())
            .collect();
        
        // Remove collected orders
        for order_id in orders_to_remove {
            if let Some((_, order)) = self.orders.remove(&order_id) {
                self.remove_from_indexes(&order);
                removed += 1;
            }
        }
        
        let duration = start.elapsed();
        info!("Garbage collection completed: removed {} orders in {:?}", removed, duration);
        
        Ok(removed)
    }
    
    /// Create a snapshot of current state
    pub async fn snapshot(&self) -> Vec<Order> {
        self.orders
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }
    
    /// Restore from a snapshot
    pub async fn restore(&self, orders: Vec<Order>) -> Result<(), StorageError> {
        info!("Restoring {} orders from snapshot", orders.len());
        
        // Clear existing data
        self.clear().await?;
        
        // Batch insert orders
        for order in orders {
            self.orders.insert(order.id.clone(), order.clone());
            self.update_indexes(&order);
        }
        
        // Update statistics
        self.stats.total_orders.store(self.orders.len() as u64, Ordering::Relaxed);
        self.stats.active_orders.store(self.active_orders.len() as u64, Ordering::Relaxed);
        
        info!("Snapshot restore completed");
        Ok(())
    }
}

#[async_trait]
impl OrderStorage for InMemoryStorage {
    #[instrument(skip(self, order))]
    async fn insert_order(&self, order: Order) -> Result<(), StorageError> {
        let start = Instant::now();
        
        // Check capacity
        self.check_capacity()?;
        
        // Check for duplicate
        if self.orders.contains_key(&order.id) {
            return Err(StorageError::BackendError(
                format!("Order {} already exists", order.id)
            ));
        }
        
        // Insert order
        let order_id = order.id.clone();
        self.orders.insert(order_id.clone(), order.clone());
        
        // Update indexes
        self.update_indexes(&order);
        
        // Update stats
        self.stats.total_orders.fetch_add(1, Ordering::Relaxed);
        if matches!(order.status, OrderStatus::New | OrderStatus::Active) {
            self.stats.active_orders.fetch_add(1, Ordering::Relaxed);
        }
        
        // Record timing
        let duration_ns = start.elapsed().as_nanos() as u64;
        self.record_timing("write", duration_ns);
        
        // Broadcast event
        let _ = self.event_tx.send(OrderEvent::Created {
            order_id,
            timestamp: Utc::now(),
        });
        
        debug!("Inserted order in {}ns", duration_ns);
        Ok(())
    }
    
    #[instrument(skip(self))]
    async fn get_order(&self, order_id: &OrderId) -> Result<Option<Order>, StorageError> {
        let start = Instant::now();
        
        let result = self.orders
            .get(order_id)
            .map(|entry| entry.value().clone());
        
        // Update stats
        if result.is_some() {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
        }
        
        // Record timing
        let duration_ns = start.elapsed().as_nanos() as u64;
        self.record_timing("read", duration_ns);
        
        debug!("Retrieved order in {}ns", duration_ns);
        Ok(result)
    }
    
    #[instrument(skip(self, order))]
    async fn update_order(&self, order: Order) -> Result<(), StorageError> {
        let start = Instant::now();
        let order_id = order.id.clone();
        
        // Get old order for index cleanup
        let old_order = self.orders
            .get(&order_id)
            .ok_or_else(|| StorageError::OrderNotFound(order_id.0))?
            .clone();
        
        // Remove old indexes
        self.remove_from_indexes(&old_order);
        
        // Update order
        self.orders.insert(order_id.clone(), order.clone());
        
        // Update new indexes
        self.update_indexes(&order);
        
        // Update stats
        self.stats.updates.fetch_add(1, Ordering::Relaxed);
        
        // Record timing
        let duration_ns = start.elapsed().as_nanos() as u64;
        self.record_timing("write", duration_ns);
        
        // Broadcast event
        let _ = self.event_tx.send(OrderEvent::Updated {
            order_id,
            old_status: old_order.status,
            new_status: order.status,
            timestamp: Utc::now(),
        });
        
        debug!("Updated order in {}ns", duration_ns);
        Ok(())
    }
    
    #[instrument(skip(self))]
    async fn delete_order(&self, order_id: &OrderId) -> Result<(), StorageError> {
        let start = Instant::now();
        
        // Remove order and get it for index cleanup
        let order = self.orders
            .remove(order_id)
            .ok_or_else(|| StorageError::OrderNotFound(order_id.0))?
            .1;
        
        // Remove from indexes
        self.remove_from_indexes(&order);
        
        // Update stats
        self.stats.total_orders.fetch_sub(1, Ordering::Relaxed);
        if matches!(order.status, OrderStatus::New | OrderStatus::Active) {
            self.stats.active_orders.fetch_sub(1, Ordering::Relaxed);
        }
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        
        // Record timing
        let duration_ns = start.elapsed().as_nanos() as u64;
        self.record_timing("write", duration_ns);
        
        // Broadcast event
        let _ = self.event_tx.send(OrderEvent::Deleted {
            order_id: order_id.clone(),
            timestamp: Utc::now(),
        });
        
        debug!("Deleted order in {}ns", duration_ns);
        Ok(())
    }
    
    #[instrument(skip(self))]
    async fn get_client_orders(&self, client_id: &ClientId) -> Result<Vec<Order>, StorageError> {
        let start = Instant::now();
        
        let orders = if let Some(order_ids) = self.client_index.get(client_id) {
            order_ids
                .iter()
                .filter_map(|id| self.orders.get(id).map(|o| o.value().clone()))
                .collect()
        } else {
            Vec::new()
        };
        
        // Record timing
        let duration_ns = start.elapsed().as_nanos() as u64;
        self.record_timing("read", duration_ns);
        
        debug!("Retrieved {} client orders in {}ns", orders.len(), duration_ns);
        Ok(orders)
    }
    
    #[instrument(skip(self))]
    async fn get_active_orders(&self) -> Result<Vec<Order>, StorageError> {
        let start = Instant::now();
        
        let orders: Vec<Order> = self.active_orders
            .iter()
            .filter_map(|id| self.orders.get(&*id).map(|o| o.value().clone()))
            .collect();
        
        // Record timing
        let duration_ns = start.elapsed().as_nanos() as u64;
        self.record_timing("read", duration_ns);
        
        debug!("Retrieved {} active orders in {}ns", orders.len(), duration_ns);
        Ok(orders)
    }
    
    #[instrument(skip(self))]
    async fn get_symbol_orders(&self, symbol: &Symbol) -> Result<Vec<Order>, StorageError> {
        let start = Instant::now();
        
        let orders = if let Some(order_ids) = self.symbol_index.get(symbol) {
            order_ids
                .iter()
                .filter_map(|id| self.orders.get(id).map(|o| o.value().clone()))
                .collect()
        } else {
            Vec::new()
        };
        
        // Record timing
        let duration_ns = start.elapsed().as_nanos() as u64;
        self.record_timing("read", duration_ns);
        
        debug!("Retrieved {} symbol orders in {}ns", orders.len(), duration_ns);
        Ok(orders)
    }
    
    #[instrument(skip(self, orders))]
    async fn batch_insert(&self, orders: Vec<Order>) -> Result<(), StorageError> {
        let start = Instant::now();
        let count = orders.len();
        
        // Check capacity
        if self.orders.len() + count > self.config.max_orders {
            return Err(StorageError::CapacityExceeded);
        }
        
        // Insert all orders
        for order in orders {
            self.orders.insert(order.id.clone(), order.clone());
            self.update_indexes(&order);
            
            // Update stats
            if matches!(order.status, OrderStatus::New | OrderStatus::Active) {
                self.stats.active_orders.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        // Update total count
        self.stats.total_orders.fetch_add(count as u64, Ordering::Relaxed);
        
        // Record timing
        let duration_ns = start.elapsed().as_nanos() as u64;
        let avg_ns = duration_ns / count as u64;
        self.record_timing("write", avg_ns);
        
        info!("Batch inserted {} orders in {}ns", count, duration_ns);
        Ok(())
    }
    
    async fn clear(&self) -> Result<(), StorageError> {
        info!("Clearing all in-memory storage");
        
        // Clear all data structures
        self.orders.clear();
        self.client_index.clear();
        self.symbol_index.clear();
        self.status_index.clear();
        self.time_index.write().clear();
        self.active_orders.clear();
        
        // Reset stats
        self.stats.total_orders.store(0, Ordering::Relaxed);
        self.stats.active_orders.store(0, Ordering::Relaxed);
        
        info!("In-memory storage cleared");
        Ok(())
    }
    
    async fn get_metrics(&self) -> Result<StorageMetrics, StorageError> {
        let stats = self.get_stats().await;
        
        Ok(StorageMetrics {
            total_orders: stats.total_orders.load(Ordering::Relaxed),
            active_orders: stats.active_orders.load(Ordering::Relaxed),
            cache_hits: stats.cache_hits.load(Ordering::Relaxed),
            cache_misses: stats.reads.load(Ordering::Relaxed) - stats.cache_hits.load(Ordering::Relaxed),
            disk_reads: 0, // N/A for memory storage
            disk_writes: 0, // N/A for memory storage
            avg_read_latency_ns: stats.avg_read_ns.load(Ordering::Relaxed),
            avg_write_latency_ns: stats.avg_write_ns.load(Ordering::Relaxed),
            storage_size_bytes: stats.memory_bytes.load(Ordering::Relaxed) as u64,
            last_snapshot: None,
        })
    }
}

/// Time-based range query support
impl InMemoryStorage {
    /// Get orders within a time range
    pub async fn get_orders_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Order>, StorageError> {
        let start_instant = Instant::now();
        
        let time_index = self.time_index.read();
        let mut orders = Vec::new();
        
        // Use BTreeMap's range query
        for (_, order_ids) in time_index.range(start..=end) {
            for order_id in order_ids.iter() {
                if let Some(order) = self.orders.get(&*order_id) {
                    orders.push(order.value().clone());
                }
            }
        }
        
        // Record timing
        let duration_ns = start_instant.elapsed().as_nanos() as u64;
        self.record_timing("read", duration_ns);
        
        debug!("Retrieved {} orders by time range in {}ns", orders.len(), duration_ns);
        Ok(orders)
    }
    
    /// Get top N orders by volume
    pub async fn get_top_orders_by_volume(&self, limit: usize) -> Vec<Order> {
        let mut orders: Vec<Order> = self.orders
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        
        // Sort by quantity (volume) in descending order
        orders.sort_by(|a, b| {
            b.quantity.partial_cmp(&a.quantity).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        orders.truncate(limit);
        orders
    }
    
    /// Get order count by status
    pub async fn get_order_count_by_status(&self) -> HashMap<OrderStatus, usize> {
        let mut counts = HashMap::new();
        
        for entry in self.status_index.iter() {
            counts.insert(entry.key().clone(), entry.value().len());
        }
        
        counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::order::{Order, OrderSide, OrderType};
    
    #[tokio::test]
    async fn test_memory_storage_basic_operations() {
        let config = MemoryConfig::default();
        let storage = InMemoryStorage::new(config);
        
        // Create test order
        let order = Order::new(
            "TEST_CLIENT".into(),
            "AAPL".into(),
            100.0,
            OrderSide::Buy,
            OrderType::Limit { price: 150.0 },
        );
        
        // Test insert
        assert!(storage.insert_order(order.clone()).await.is_ok());
        
        // Test get
        let retrieved = storage.get_order(&order.id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, order.id);
        
        // Test update
        let mut updated_order = order.clone();
        updated_order.status = OrderStatus::Filled;
        assert!(storage.update_order(updated_order).await.is_ok());
        
        // Test delete
        assert!(storage.delete_order(&order.id).await.is_ok());
        
        // Verify deletion
        let deleted = storage.get_order(&order.id).await.unwrap();
        assert!(deleted.is_none());
    }
    
    #[tokio::test]
    async fn test_memory_storage_indexes() {
        let config = MemoryConfig::default();
        let storage = InMemoryStorage::new(config);
        
        // Insert multiple orders
        for i in 0..10 {
            let order = Order::new(
                "CLIENT_1".into(),
                if i < 5 { "AAPL" } else { "GOOGL" }.into(),
                100.0 * (i + 1) as f64,
                OrderSide::Buy,
                OrderType::Market,
            );
            storage.insert_order(order).await.unwrap();
        }
        
        // Test client index
        let client_orders = storage.get_client_orders(&"CLIENT_1".into()).await.unwrap();
        assert_eq!(client_orders.len(), 10);
        
        // Test symbol index
        let aapl_orders = storage.get_symbol_orders(&"AAPL".into()).await.unwrap();
        assert_eq!(aapl_orders.len(), 5);
        
        let googl_orders = storage.get_symbol_orders(&"GOOGL".into()).await.unwrap();
        assert_eq!(googl_orders.len(), 5);
    }
    
    #[tokio::test]
    async fn test_memory_storage_capacity() {
        let config = MemoryConfig {
            max_orders: 5,
            enable_metrics: true,
        };
        let storage = InMemoryStorage::new(config);
        
        // Fill to capacity
        for i in 0..5 {
            let order = Order::new(
                format!("CLIENT_{}", i).into(),
                "TEST".into(),
                100.0,
                OrderSide::Buy,
                OrderType::Market,
            );
            assert!(storage.insert_order(order).await.is_ok());
        }
        
        // Try to exceed capacity
        let extra_order = Order::new(
            "EXTRA".into(),
            "TEST".into(),
            100.0,
            OrderSide::Buy,
            OrderType::Market,
        );
        
        let result = storage.insert_order(extra_order).await;
        assert!(matches!(result, Err(StorageError::CapacityExceeded)));
    }
    
    #[tokio::test]
    async fn test_batch_operations() {
        let config = MemoryConfig::default();
        let storage = InMemoryStorage::new(config);
        
        // Create batch of orders
        let orders: Vec<Order> = (0..100)
            .map(|i| {
                Order::new(
                    format!("CLIENT_{}", i % 10).into(),
                    format!("SYMBOL_{}", i % 5).into(),
                    100.0 * i as f64,
                    if i % 2 == 0 { OrderSide::Buy } else { OrderSide::Sell },
                    OrderType::Market,
                )
            })
            .collect();
        
        // Batch insert
        assert!(storage.batch_insert(orders).await.is_ok());
        
        // Verify insertion
        let all_orders = storage.get_active_orders().await.unwrap();
        assert_eq!(all_orders.len(), 100);
    }
}
