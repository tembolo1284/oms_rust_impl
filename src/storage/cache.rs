// src/storage/cache.rs - Multi-Level Cache Layer
//! # Cache Layer for Trading OMS
//! 
//! High-performance multi-level caching with:
//! - L1 Cache: Ultra-fast in-process cache using Arc<DashMap>
//! - L2 Cache: Larger capacity with LRU eviction
//! - TTL support for time-based expiration
//! - Cache warming and preloading
//! - Statistics and monitoring

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::{
    sync::broadcast,
    time::{interval, sleep},
};
use tracing::{debug, error, info, instrument, warn};

use crate::core::{
    order::Order,
    types::OrderId,
};

/// Cache configuration
#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    /// L1 cache size (number of entries)
    pub l1_size: usize,
    /// L2 cache size (number of entries)
    pub l2_size: usize,
    /// Time-to-live for cache entries (seconds)
    pub ttl_seconds: Option<u64>,
    /// Enable cache warming on startup
    pub enable_warming: bool,
    /// Enable statistics collection
    pub enable_stats: bool,
    /// Eviction policy
    pub eviction_policy: EvictionPolicy,
    /// Cache write policy
    pub write_policy: WritePolicy,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            l1_size: 10_000,      // 10K hot entries
            l2_size: 100_000,      // 100K warm entries
            ttl_seconds: Some(300), // 5 minutes default TTL
            enable_warming: true,
            enable_stats: true,
            eviction_policy: EvictionPolicy::Lru,
            write_policy: WritePolicy::WriteThrough,
        }
    }
}

/// Eviction policy for cache
#[derive(Debug, Clone, Deserialize)]
pub enum EvictionPolicy {
    Lru,  // Least Recently Used
    Lfu,  // Least Frequently Used
    Fifo, // First In First Out
    Ttl,  // Time-based only
}

/// Write policy for cache
#[derive(Debug, Clone, Deserialize)]
pub enum WritePolicy {
    WriteThrough, // Write to cache and backing store
    WriteBack,    // Write to cache, async to backing
    WriteAround,  // Skip cache on writes
}

/// Cache statistics
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub l1_hits: u64,
    pub l2_hits: u64,
    pub evictions: u64,
    pub expirations: u64,
    pub size: usize,
    pub l1_size: usize,
    pub l2_size: usize,
    pub avg_get_ns: u64,
    pub avg_put_ns: u64,
}

/// Cached entry with metadata
#[derive(Debug, Clone)]
struct CacheEntry<V: Clone> {
    value: V,
    inserted_at: DateTime<Utc>,
    accessed_at: DateTime<Utc>,
    access_count: u64,
    size_bytes: usize,
}

impl<V: Clone> CacheEntry<V> {
    fn new(value: V, size_bytes: usize) -> Self {
        let now = Utc::now();
        Self {
            value,
            inserted_at: now,
            accessed_at: now,
            access_count: 1,
            size_bytes,
        }
    }
    
    fn is_expired(&self, ttl: Duration) -> bool {
        Utc::now() - self.inserted_at > ttl
    }
    
    fn touch(&mut self) {
        self.accessed_at = Utc::now();
        self.access_count += 1;
    }
}

/// L1 Cache - Ultra-fast, lock-free
pub struct L1Cache<K: Eq + Hash + Clone, V: Clone> {
    data: Arc<DashMap<K, CacheEntry<V>>>,
    capacity: usize,
    stats: Arc<L1Stats>,
}

#[derive(Debug, Default)]
struct L1Stats {
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    evictions: AtomicU64,
}

impl<K: Eq + Hash + Clone, V: Clone> L1Cache<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            data: Arc::new(DashMap::with_capacity(capacity)),
            capacity,
            stats: Arc::new(L1Stats::default()),
        }
    }
    
    fn get(&self, key: &K) -> Option<V> {
        if let Some(mut entry) = self.data.get_mut(key) {
            entry.touch();
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.value.clone())
        } else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
    
    fn put(&self, key: K, value: V, size_bytes: usize) {
        // Simple eviction if at capacity
        if self.data.len() >= self.capacity {
            // Remove random entry (could be improved with LRU)
            if let Some(entry) = self.data.iter().next() {
                self.data.remove(entry.key());
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        self.data.insert(key, CacheEntry::new(value, size_bytes));
        self.stats.inserts.fetch_add(1, Ordering::Relaxed);
    }
    
    fn remove(&self, key: &K) -> Option<V> {
        self.data.remove(key).map(|(_, entry)| entry.value)
    }
    
    fn clear(&self) {
        self.data.clear();
    }
    
    fn size(&self) -> usize {
        self.data.len()
    }
}

/// L2 Cache - Larger capacity with LRU eviction
pub struct L2Cache<K: Eq + Hash + Clone, V: Clone> {
    data: Arc<Mutex<LruCache<K, CacheEntry<V>>>>,
    stats: Arc<L2Stats>,
}

#[derive(Debug, Default)]
struct L2Stats {
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    evictions: AtomicU64,
}

impl<K: Eq + Hash + Clone, V: Clone> L2Cache<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            data: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity).unwrap()
            ))),
            stats: Arc::new(L2Stats::default()),
        }
    }
    
    fn get(&self, key: &K) -> Option<V> {
        let mut cache = self.data.lock();
        if let Some(entry) = cache.get_mut(key) {
            entry.touch();
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.value.clone())
        } else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
    
    fn put(&self, key: K, value: V, size_bytes: usize) {
        let mut cache = self.data.lock();
        if cache.len() >= cache.cap().get() {
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
        cache.put(key, CacheEntry::new(value, size_bytes));
        self.stats.inserts.fetch_add(1, Ordering::Relaxed);
    }
    
    fn remove(&self, key: &K) -> Option<V> {
        self.data.lock().pop(key).map(|entry| entry.value)
    }
    
    fn clear(&self) {
        self.data.lock().clear();
    }
    
    fn size(&self) -> usize {
        self.data.lock().len()
    }
}

/// Main cache layer coordinating L1 and L2
pub struct CacheLayer {
    l1: L1Cache<OrderId, Order>,
    l2: L2Cache<OrderId, Order>,
    config: CacheConfig,
    stats: Arc<RwLock<CacheStats>>,
    ttl: Option<Duration>,
    invalidation_tx: broadcast::Sender<OrderId>,
}

impl CacheLayer {
    /// Create a new cache layer
    pub fn new(config: CacheConfig) -> Self {
        let ttl = config.ttl_seconds.map(|s| Duration::seconds(s as i64));
        let (invalidation_tx, _) = broadcast::channel(1000);
        
        let cache = Self {
            l1: L1Cache::new(config.l1_size),
            l2: L2Cache::new(config.l2_size),
            config: config.clone(),
            stats: Arc::new(RwLock::new(CacheStats::default())),
            ttl,
            invalidation_tx,
        };
        
        // Start background tasks
        if config.ttl_seconds.is_some() {
            cache.start_ttl_cleanup();
        }
        
        if config.enable_stats {
            cache.start_stats_collector();
        }
        
        info!("Cache layer initialized: L1={}, L2={}", config.l1_size, config.l2_size);
        cache
    }
    
    /// Get an order from cache
    #[instrument(skip(self))]
    pub async fn get(&self, key: &OrderId) -> Option<Order> {
        let start = Instant::now();
        
        // Try L1 first
        if let Some(value) = self.l1.get(key) {
            self.update_stats(true, true, false, start.elapsed().as_nanos() as u64).await;
            return Some(value);
        }
        
        // Try L2
        if let Some(value) = self.l2.get(key) {
            // Promote to L1
            self.l1.put(key.clone(), value.clone(), std::mem::size_of::<Order>());
            self.update_stats(true, false, true, start.elapsed().as_nanos() as u64).await;
            return Some(value);
        }
        
        // Cache miss
        self.update_stats(false, false, false, start.elapsed().as_nanos() as u64).await;
        None
    }
    
    /// Put an order into cache
    #[instrument(skip(self, value))]
    pub async fn put(&self, key: OrderId, value: Order) {
        let start = Instant::now();
        let size = std::mem::size_of::<Order>();
        
        match self.config.write_policy {
            WritePolicy::WriteThrough | WritePolicy::WriteBack => {
                // Add to L1
                self.l1.put(key.clone(), value.clone(), size);
                
                // Optionally add to L2 if evicted from L1
                if self.l1.size() >= self.config.l1_size {
                    self.l2.put(key, value, size);
                }
            }
            WritePolicy::WriteAround => {
                // Skip cache on writes
                return;
            }
        }
        
        let duration = start.elapsed().as_nanos() as u64;
        self.update_put_stats(duration).await;
    }
    
    /// Invalidate a cache entry
    pub async fn invalidate(&self, key: &OrderId) {
        self.l1.remove(key);
        self.l2.remove(key);
        let _ = self.invalidation_tx.send(key.clone());
        debug!("Invalidated cache entry: {}", key);
    }
    
    /// Clear all cache entries
    pub async fn clear(&self) {
        self.l1.clear();
        self.l2.clear();
        
        let mut stats = self.stats.write();
        *stats = CacheStats::default();
        
        info!("Cache cleared");
    }
    
    /// Get cache statistics
    pub async fn get_stats(&self) -> CacheStats {
        let stats = self.stats.read();
        CacheStats {
            l1_size: self.l1.size(),
            l2_size: self.l2.size(),
            size: self.l1.size() + self.l2.size(),
            ..stats.clone()
        }
    }
    
    /// Warm the cache with frequently accessed orders
    pub async fn warm_cache(&self, orders: Vec<Order>) -> Result<()> {
        if !self.config.enable_warming {
            return Ok(());
        }
        
        info!("Warming cache with {} orders", orders.len());
        
        for order in orders {
            self.put(order.id.clone(), order).await;
        }
        
        Ok(())
    }
    
    /// Start TTL cleanup task
    fn start_ttl_cleanup(&self) {
        if let Some(ttl) = self.ttl {
            let l1 = Arc::clone(&self.l1.data);
            let l2 = Arc::clone(&self.l2.data);
            let stats = Arc::clone(&self.stats);
            
            tokio::spawn(async move {
                let mut interval = interval(tokio::time::Duration::from_secs(60));
                
                loop {
                    interval.tick().await;
                    
                    let mut expired = 0;
                    
                    // Clean L1
                    let expired_keys: Vec<_> = l1
                        .iter()
                        .filter(|entry| entry.value().is_expired(ttl))
                        .map(|entry| entry.key().clone())
                        .collect();
                    
                    for key in expired_keys {
                        l1.remove(&key);
                        expired += 1;
                    }
                    
                    // Clean L2
                    {
                        let mut cache = l2.lock();
                        // Note: LRU cache doesn't provide iteration, so we'd need a different approach
                        // This is simplified for the example
                    }
                    
                    if expired > 0 {
                        let mut s = stats.write();
                        s.expirations += expired;
                        debug!("Expired {} cache entries", expired);
                    }
                }
            });
        }
    }
    
    /// Start statistics collector task
    fn start_stats_collector(&self) {
        let stats = Arc::clone(&self.stats);
        let l1_stats = Arc::clone(&self.l1.stats);
        let l2_stats = Arc::clone(&self.l2.stats);
        
        tokio::spawn(async move {
            let mut interval = interval(tokio::time::Duration::from_secs(10));
            
            loop {
                interval.tick().await;
                
                let mut s = stats.write();
                s.l1_hits = l1_stats.hits.load(Ordering::Relaxed);
                s.l2_hits = l2_stats.hits.load(Ordering::Relaxed);
                s.hits = s.l1_hits + s.l2_hits;
                s.misses = l1_stats.misses.load(Ordering::Relaxed);
                s.evictions = l1_stats.evictions.load(Ordering::Relaxed) + 
                             l2_stats.evictions.load(Ordering::Relaxed);
            }
        });
    }
    
    /// Update cache statistics
    async fn update_stats(&self, hit: bool, l1_hit: bool, l2_hit: bool, duration_ns: u64) {
        if !self.config.enable_stats {
            return;
        }
        
        let mut stats = self.stats.write();
        
        if hit {
            stats.hits += 1;
            if l1_hit {
                stats.l1_hits += 1;
            } else if l2_hit {
                stats.l2_hits += 1;
            }
        } else {
            stats.misses += 1;
        }
        
        // Update average get time
        let total_gets = stats.hits + stats.misses;
        if total_gets > 0 {
            stats.avg_get_ns = ((stats.avg_get_ns * (total_gets - 1)) + duration_ns) / total_gets;
        }
    }
    
    /// Update put statistics
    async fn update_put_stats(&self, duration_ns: u64) {
        if !self.config.enable_stats {
            return;
        }
        
        let mut stats = self.stats.write();
        // Simplified averaging
        stats.avg_put_ns = (stats.avg_put_ns + duration_ns) / 2;
    }
}

/// Specialized cache for order books (symbol -> orders)
pub struct OrderBookCache {
    data: Arc<DashMap<String, Arc<Vec<Order>>>>,
    capacity: usize,
    ttl: Option<Duration>,
}

impl OrderBookCache {
    pub fn new(capacity: usize, ttl_seconds: Option<u64>) -> Self {
        Self {
            data: Arc::new(DashMap::with_capacity(capacity)),
            capacity,
            ttl: ttl_seconds.map(|s| Duration::seconds(s as i64)),
        }
    }
    
    pub fn get(&self, symbol: &str) -> Option<Arc<Vec<Order>>> {
        self.data.get(symbol).map(|entry| entry.clone())
    }
    
    pub fn put(&self, symbol: String, orders: Vec<Order>) {
        if self.data.len() >= self.capacity {
            // Simple FIFO eviction
            if let Some(entry) = self.data.iter().next() {
                self.data.remove(entry.key());
            }
        }
        
        self.data.insert(symbol, Arc::new(orders));
    }
    
    pub fn invalidate(&self, symbol: &str) {
        self.data.remove(symbol);
    }
    
    pub fn clear(&self) {
        self.data.clear();
    }
}

/// Cache for frequently accessed aggregations
pub struct AggregationCache {
    volume_by_symbol: Arc<DashMap<String, f64>>,
    order_count_by_client: Arc<DashMap<String, u64>>,
    active_orders_count: AtomicU64,
    last_update: Arc<RwLock<DateTime<Utc>>>,
}

impl AggregationCache {
    pub fn new() -> Self {
        Self {
            volume_by_symbol: Arc::new(DashMap::new()),
            order_count_by_client: Arc::new(DashMap::new()),
            active_orders_count: AtomicU64::new(0),
            last_update: Arc::new(RwLock::new(Utc::now())),
        }
    }
    
    pub async fn update_volume(&self, symbol: String, volume: f64) {
        self.volume_by_symbol
            .entry(symbol)
            .and_modify(|v| *v += volume)
            .or_insert(volume);
        
        *self.last_update.write() = Utc::now();
    }
    
    pub async fn update_client_count(&self, client: String, delta: i64) {
        self.order_count_by_client
            .entry(client)
            .and_modify(|c| {
                if delta > 0 {
                    *c += delta as u64;
                } else {
                    *c = c.saturating_sub((-delta) as u64);
                }
            })
            .or_insert(if delta > 0 { delta as u64 } else { 0 });
        
        *self.last_update.write() = Utc::now();
    }
    
    pub fn get_volume(&self, symbol: &str) -> Option<f64> {
        self.volume_by_symbol.get(symbol).map(|v| *v)
    }
    
    pub fn get_client_order_count(&self, client: &str) -> Option<u64> {
        self.order_count_by_client.get(client).map(|c| *c)
    }
    
    pub fn set_active_orders(&self, count: u64) {
        self.active_orders_count.store(count, Ordering::Relaxed);
    }
    
    pub fn get_active_orders(&self) -> u64 {
        self.active_orders_count.load(Ordering::Relaxed)
    }
    
    pub async fn is_stale(&self, max_age_seconds: u64) -> bool {
        let last = *self.last_update.read();
        (Utc::now() - last).num_seconds() as u64 > max_age_seconds
    }
    
    pub fn clear(&self) {
        self.volume_by_symbol.clear();
        self.order_count_by_client.clear();
        self.active_orders_count.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::order::{Order, OrderSide, OrderType};
    
    #[tokio::test]
    async fn test_cache_layer_operations() {
        let config = CacheConfig {
            l1_size: 100,
            l2_size: 1000,
            ttl_seconds: Some(60),
            ..Default::default()
        };
        
        let cache = CacheLayer::new(config);
        
        // Create test order
        let order = Order::new(
            "CLIENT_1".into(),
            "AAPL".into(),
            100.0,
            OrderSide::Buy,
            OrderType::Market,
        );
        
        // Test put
        cache.put(order.id.clone(), order.clone()).await;
        
        // Test get - should be L1 hit
        let retrieved = cache.get(&order.id).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, order.id);
        
        // Test stats
        let stats = cache.get_stats().await;
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.l1_hits, 1);
        
        // Test invalidation
        cache.invalidate(&order.id).await;
        let after_invalidate = cache.get(&order.id).await;
        assert!(after_invalidate.is_none());
    }
    
    #[tokio::test]
    async fn test_l1_l2_promotion() {
        let config = CacheConfig {
            l1_size: 2,  // Small L1
            l2_size: 10,
            ttl_seconds: None,
            ..Default::default()
        };
        
        let cache = CacheLayer::new(config);
        
        // Fill L1 cache
        let order1 = Order::new("C1".into(), "A".into(), 100.0, OrderSide::Buy, OrderType::Market);
        let order2 = Order::new("C2".into(), "B".into(), 200.0, OrderSide::Buy, OrderType::Market);
        let order3 = Order::new("C3".into(), "C".into(), 300.0, OrderSide::Buy, OrderType::Market);
        
        cache.put(order1.id.clone(), order1.clone()).await;
        cache.put(order2.id.clone(), order2.clone()).await;
        cache.put(order3.id.clone(), order3.clone()).await; // This should evict order1 to L2
        
        // order1 should be in L2, not L1
        let stats_before = cache.get_stats().await;
        
        // Getting order1 should be an L2 hit and promote to L1
        let retrieved = cache.get(&order1.id).await;
        assert!(retrieved.is_some());
        
        let stats_after = cache.get_stats().await;
        assert!(stats_after.l2_hits > stats_before.l2_hits);
    }
    
    #[tokio::test]
    async fn test_aggregation_cache() {
        let agg_cache = AggregationCache::new();
        
        // Test volume updates
        agg_cache.update_volume("AAPL".to_string(), 1000.0).await;
        agg_cache.update_volume("AAPL".to_string(), 500.0).await;
        
        let volume = agg_cache.get_volume("AAPL");
        assert_eq!(volume, Some(1500.0));
        
        // Test client count updates
        agg_cache.update_client_count("CLIENT_1".to_string(), 5).await;
        agg_cache.update_client_count("CLIENT_1".to_string(), -2).await;
        
        let count = agg_cache.get_client_order_count("CLIENT_1");
        assert_eq!(count, Some(3));
        
        // Test active orders
        agg_cache.set_active_orders(42);
        assert_eq!(agg_cache.get_active_orders(), 42);
    }
}
