// src/storage/persistent.rs - Persistent Storage Backend
//! # Persistent Storage Layer
//! 
//! Durable storage using RocksDB for high-performance disk persistence.
//! Provides ACID guarantees, snapshots, and write-ahead logging.

use anyhow::{Context, Result};
use async_trait::async_trait;
use bincode::{deserialize, serialize};
use chrono::{DateTime, Utc};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, DBCompactionStyle,
    DBCompressionType, Direction, IteratorMode, Options as RocksOptions, WriteBatch, DB,
};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::{
    sync::{Mutex, RwLock},
    task::spawn_blocking,
};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::core::{
    events::OrderEvent,
    order::{Order, OrderStatus},
    types::{ClientId, OrderId, Symbol},
};

use super::{
    EventStorage, OrderStorage, SnapshotInfo, SnapshotStorage, StorageError, StorageMetrics,
};

/// Configuration for persistent storage
#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    /// Base directory for storage files
    pub data_dir: PathBuf,
    /// Enable compression
    pub compression: bool,
    /// Cache size in MB
    pub cache_size_mb: usize,
    /// Write buffer size in MB
    pub write_buffer_size_mb: usize,
    /// Maximum write buffer number
    pub max_write_buffer_number: i32,
    /// Enable write-ahead log
    pub enable_wal: bool,
    /// Sync writes to disk
    pub sync_writes: bool,
    /// Compaction style
    pub compaction_style: CompactionStyle,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data/oms"),
            compression: true,
            cache_size_mb: 256,
            write_buffer_size_mb: 64,
            max_write_buffer_number: 4,
            enable_wal: true,
            sync_writes: false, // Set to true for maximum durability
            compaction_style: CompactionStyle::Level,
        }
    }
}

/// Compaction style for RocksDB
#[derive(Debug, Clone, Deserialize)]
pub enum CompactionStyle {
    Level,
    Universal,
    Fifo,
}

impl From<CompactionStyle> for DBCompactionStyle {
    fn from(style: CompactionStyle) -> Self {
        match style {
            CompactionStyle::Level => DBCompactionStyle::Level,
            CompactionStyle::Universal => DBCompactionStyle::Universal,
            CompactionStyle::Fifo => DBCompactionStyle::Fifo,
        }
    }
}

/// Column families for organizing data
const CF_ORDERS: &str = "orders";
const CF_EVENTS: &str = "events";
const CF_INDEXES: &str = "indexes";
const CF_SNAPSHOTS: &str = "snapshots";
const CF_METADATA: &str = "metadata";

/// Index key prefixes
const INDEX_CLIENT: &[u8] = b"client:";
const INDEX_SYMBOL: &[u8] = b"symbol:";
const INDEX_STATUS: &[u8] = b"status:";
const INDEX_TIME: &[u8] = b"time:";

/// Statistics for persistent storage
#[derive(Debug, Default)]
pub struct PersistentStats {
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub deletes: AtomicU64,
    pub compactions: AtomicU64,
    pub snapshots_created: AtomicU64,
    pub bytes_written: AtomicU64,
    pub bytes_read: AtomicU64,
}

/// Persistent storage implementation using RocksDB
pub struct PersistentStorage {
    db: Arc<DB>,
    config: StorageConfig,
    stats: Arc<PersistentStats>,
    write_lock: Arc<Mutex<()>>, // Ensures write ordering
}

impl PersistentStorage {
    /// Create a new persistent storage instance
    pub async fn new(config: StorageConfig) -> Result<Self> {
        info!("Initializing persistent storage at {:?}", config.data_dir);
        
        // Create data directory if it doesn't exist
        tokio::fs::create_dir_all(&config.data_dir)
            .await
            .context("Failed to create data directory")?;
        
        let config_clone = config.clone();
        let db = spawn_blocking(move || Self::open_database(config_clone))
            .await
            .context("Failed to spawn database opening task")??;
        
        Ok(Self {
            db: Arc::new(db),
            config,
            stats: Arc::new(PersistentStats::default()),
            write_lock: Arc::new(Mutex::new(())),
        })
    }
    
    /// Open RocksDB with optimized settings
    fn open_database(config: StorageConfig) -> Result<DB> {
        let mut db_opts = RocksOptions::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        
        // Performance optimizations
        db_opts.set_write_buffer_size(config.write_buffer_size_mb * 1024 * 1024);
        db_opts.set_max_write_buffer_number(config.max_write_buffer_number);
        db_opts.set_compaction_style(config.compaction_style.into());
        
        // Compression settings
        if config.compression {
            db_opts.set_compression_type(DBCompressionType::Lz4);
            db_opts.set_bottommost_compression_type(DBCompressionType::Zstd);
        }
        
        // Block cache for reads
        if config.cache_size_mb > 0 {
            let cache = Cache::new_lru_cache(config.cache_size_mb * 1024 * 1024);
            let mut block_opts = BlockBasedOptions::default();
            block_opts.set_block_cache(&cache);
            block_opts.set_cache_index_and_filter_blocks(true);
            db_opts.set_block_based_table_factory(&block_opts);
        }
        
        // WAL settings
        if !config.enable_wal {
            db_opts.set_manual_wal_flush(!config.enable_wal);
        }
        
        // Parallelism
        db_opts.increase_parallelism(num_cpus::get() as i32);
        db_opts.set_max_background_jobs(4);
        
        // Column families
        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_ORDERS, db_opts.clone()),
            ColumnFamilyDescriptor::new(CF_EVENTS, db_opts.clone()),
            ColumnFamilyDescriptor::new(CF_INDEXES, db_opts.clone()),
            ColumnFamilyDescriptor::new(CF_SNAPSHOTS, db_opts.clone()),
            ColumnFamilyDescriptor::new(CF_METADATA, db_opts.clone()),
        ];
        
        DB::open_cf_descriptors(&db_opts, &config.data_dir, cfs)
            .context("Failed to open RocksDB")
    }
    
    /// Get column family handle
    fn cf_handle(&self, cf_name: &str) -> Result<&ColumnFamily, StorageError> {
        self.db
            .cf_handle(cf_name)
            .ok_or_else(|| StorageError::BackendError(format!("Column family {} not found", cf_name)))
    }
    
    /// Build index key
    fn build_index_key(prefix: &[u8], primary: &str, secondary: &OrderId) -> Vec<u8> {
        let mut key = Vec::with_capacity(prefix.len() + primary.len() + 36); // UUID is 36 chars
        key.extend_from_slice(prefix);
        key.extend_from_slice(primary.as_bytes());
        key.push(b':');
        key.extend_from_slice(secondary.0.to_string().as_bytes());
        key
    }
    
    /// Build time index key
    fn build_time_index_key(timestamp: DateTime<Utc>, order_id: &OrderId) -> Vec<u8> {
        let mut key = Vec::with_capacity(INDEX_TIME.len() + 8 + 36); // 8 bytes timestamp + UUID
        key.extend_from_slice(INDEX_TIME);
        key.extend_from_slice(&timestamp.timestamp_nanos_opt().unwrap_or(0).to_be_bytes());
        key.push(b':');
        key.extend_from_slice(order_id.0.to_string().as_bytes());
        key
    }
    
    /// Update all indexes for an order
    async fn update_indexes(&self, order: &Order) -> Result<(), StorageError> {
        let cf_indexes = self.cf_handle(CF_INDEXES)?;
        let mut batch = WriteBatch::default();
        
        // Client index
        let client_key = Self::build_index_key(INDEX_CLIENT, &order.client_id.0, &order.id);
        batch.put_cf(cf_indexes, client_key, b"");
        
        // Symbol index
        let symbol_key = Self::build_index_key(INDEX_SYMBOL, &order.symbol.0, &order.id);
        batch.put_cf(cf_indexes, symbol_key, b"");
        
        // Status index
        let status_key = Self::build_index_key(
            INDEX_STATUS,
            &format!("{:?}", order.status),
            &order.id,
        );
        batch.put_cf(cf_indexes, status_key, b"");
        
        // Time index
        let time_key = Self::build_time_index_key(order.created_at, &order.id);
        batch.put_cf(cf_indexes, time_key, b"");
        
        let db = Arc::clone(&self.db);
        spawn_blocking(move || db.write(batch))
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        
        Ok(())
    }
    
    /// Remove indexes for an order
    async fn remove_indexes(&self, order: &Order) -> Result<(), StorageError> {
        let cf_indexes = self.cf_handle(CF_INDEXES)?;
        let mut batch = WriteBatch::default();
        
        // Remove all index entries
        let client_key = Self::build_index_key(INDEX_CLIENT, &order.client_id.0, &order.id);
        batch.delete_cf(cf_indexes, client_key);
        
        let symbol_key = Self::build_index_key(INDEX_SYMBOL, &order.symbol.0, &order.id);
        batch.delete_cf(cf_indexes, symbol_key);
        
        let status_key = Self::build_index_key(
            INDEX_STATUS,
            &format!("{:?}", order.status),
            &order.id,
        );
        batch.delete_cf(cf_indexes, status_key);
        
        let time_key = Self::build_time_index_key(order.created_at, &order.id);
        batch.delete_cf(cf_indexes, time_key);
        
        let db = Arc::clone(&self.db);
        spawn_blocking(move || db.write(batch))
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        
        Ok(())
    }
    
    /// Query orders using an index
    async fn query_by_index(&self, prefix: &[u8], key: &str) -> Result<Vec<Order>, StorageError> {
        let cf_indexes = self.cf_handle(CF_INDEXES)?;
        let cf_orders = self.cf_handle(CF_ORDERS)?;
        
        let mut search_key = Vec::with_capacity(prefix.len() + key.len());
        search_key.extend_from_slice(prefix);
        search_key.extend_from_slice(key.as_bytes());
        search_key.push(b':');
        
        let db = Arc::clone(&self.db);
        let search_key_clone = search_key.clone();
        
        let order_ids = spawn_blocking(move || {
            let mut order_ids = Vec::new();
            let iter = db.iterator_cf(cf_indexes, IteratorMode::From(&search_key_clone, Direction::Forward));
            
            for item in iter {
                let (k, _) = item.map_err(|e| StorageError::BackendError(e.to_string()))?;
                
                // Check if key still has our prefix
                if !k.starts_with(&search_key_clone) {
                    break;
                }
                
                // Extract order ID from key
                if let Some(id_part) = k.splitn(3, |&b| b == b':').nth(2) {
                    if let Ok(id_str) = std::str::from_utf8(id_part) {
                        if let Ok(uuid) = Uuid::parse_str(id_str) {
                            order_ids.push(OrderId(uuid));
                        }
                    }
                }
            }
            
            Ok::<Vec<OrderId>, StorageError>(order_ids)
        })
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))??;
        
        // Fetch all orders
        let mut orders = Vec::new();
        for order_id in order_ids {
            if let Some(order) = self.get_order(&order_id).await? {
                orders.push(order);
            }
        }
        
        Ok(orders)
    }
    
    /// Compact the database
    pub async fn compact(&self) -> Result<(), StorageError> {
        info!("Starting database compaction");
        let db = Arc::clone(&self.db);
        
        spawn_blocking(move || {
            // Compact all column families
            for cf_name in &[CF_ORDERS, CF_EVENTS, CF_INDEXES, CF_SNAPSHOTS, CF_METADATA] {
                if let Some(cf) = db.cf_handle(cf_name) {
                    db.compact_range_cf(cf, None::<&[u8]>, None::<&[u8]>);
                }
            }
        })
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))?;
        
        self.stats.compactions.fetch_add(1, Ordering::Relaxed);
        info!("Database compaction completed");
        Ok(())
    }
    
    /// Get database statistics
    pub async fn get_db_stats(&self) -> Result<String, StorageError> {
        let db = Arc::clone(&self.db);
        
        spawn_blocking(move || {
            db.property("rocksdb.stats")
                .ok_or_else(|| StorageError::BackendError("Failed to get DB stats".into()))
        })
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))?
    }
    
    /// Flush write-ahead log
    pub async fn flush_wal(&self) -> Result<(), StorageError> {
        if self.config.enable_wal {
            let db = Arc::clone(&self.db);
            spawn_blocking(move || db.flush_wal(true))
                .await
                .map_err(|e| StorageError::BackendError(e.to_string()))?
                .map_err(|e| StorageError::BackendError(e.to_string()))?;
        }
        Ok(())
    }
}

#[async_trait]
impl OrderStorage for PersistentStorage {
    #[instrument(skip(self, order))]
    async fn insert_order(&self, order: Order) -> Result<(), StorageError> {
        let start = Instant::now();
        let _lock = self.write_lock.lock().await; // Ensure write ordering
        
        let cf = self.cf_handle(CF_ORDERS)?;
        let key = order.id.0.as_bytes().to_vec();
        let value = serialize(&order)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        
        // Write order
        let db = Arc::clone(&self.db);
        let cf_clone = cf as *const ColumnFamily;
        spawn_blocking(move || unsafe {
            db.put_cf(&*cf_clone, &key, &value)
        })
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))?
        .map_err(|e| StorageError::BackendError(e.to_string()))?;
        
        // Update indexes
        self.update_indexes(&order).await?;
        
        // Update stats
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_written.fetch_add(value.len() as u64, Ordering::Relaxed);
        
        debug!("Persisted order {} in {:?}", order.id, start.elapsed());
        Ok(())
    }
    
    #[instrument(skip(self))]
    async fn get_order(&self, order_id: &OrderId) -> Result<Option<Order>, StorageError> {
        let start = Instant::now();
        
        let cf = self.cf_handle(CF_ORDERS)?;
        let key = order_id.0.as_bytes().to_vec();
        
        let db = Arc::clone(&self.db);
        let cf_clone = cf as *const ColumnFamily;
        let value = spawn_blocking(move || unsafe {
            db.get_cf(&*cf_clone, &key)
        })
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))?
        .map_err(|e| StorageError::BackendError(e.to_string()))?;
        
        let result = match value {
            Some(bytes) => {
                let order = deserialize(&bytes)
                    .map_err(|e| StorageError::SerializationError(e.to_string()))?;
                self.stats.bytes_read.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                Some(order)
            }
            None => None,
        };
        
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        debug!("Retrieved order {} in {:?}", order_id, start.elapsed());
        Ok(result)
    }
    
    #[instrument(skip(self, order))]
    async fn update_order(&self, order: Order) -> Result<(), StorageError> {
        let _lock = self.write_lock.lock().await;
        
        // Get old order for index cleanup
        let old_order = self.get_order(&order.id).await?
            .ok_or_else(|| StorageError::OrderNotFound(order.id.0))?;
        
        // Remove old indexes
        self.remove_indexes(&old_order).await?;
        
        // Insert updated order
        let cf = self.cf_handle(CF_ORDERS)?;
        let key = order.id.0.as_bytes().to_vec();
        let value = serialize(&order)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        
        let db = Arc::clone(&self.db);
        let cf_clone = cf as *const ColumnFamily;
        spawn_blocking(move || unsafe {
            db.put_cf(&*cf_clone, &key, &value)
        })
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))?
        .map_err(|e| StorageError::BackendError(e.to_string()))?;
        
        // Update new indexes
        self.update_indexes(&order).await?;
        
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    
    #[instrument(skip(self))]
    async fn delete_order(&self, order_id: &OrderId) -> Result<(), StorageError> {
        let _lock = self.write_lock.lock().await;
        
        // Get order for index cleanup
        let order = self.get_order(order_id).await?
            .ok_or_else(|| StorageError::OrderNotFound(order_id.0))?;
        
        // Remove indexes
        self.remove_indexes(&order).await?;
        
        // Delete order
        let cf = self.cf_handle(CF_ORDERS)?;
        let key = order_id.0.as_bytes().to_vec();
        
        let db = Arc::clone(&self.db);
        let cf_clone = cf as *const ColumnFamily;
        spawn_blocking(move || unsafe {
            db.delete_cf(&*cf_clone, &key)
        })
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))?
        .map_err(|e| StorageError::BackendError(e.to_string()))?;
        
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    
    async fn get_client_orders(&self, client_id: &ClientId) -> Result<Vec<Order>, StorageError> {
        self.query_by_index(INDEX_CLIENT, &client_id.0).await
    }
    
    async fn get_active_orders(&self) -> Result<Vec<Order>, StorageError> {
        let mut orders = Vec::new();
        
        // Query active statuses
        for status in &[OrderStatus::New, OrderStatus::Active, OrderStatus::PartiallyFilled] {
            let status_orders = self.query_by_index(INDEX_STATUS, &format!("{:?}", status)).await?;
            orders.extend(status_orders);
        }
        
        Ok(orders)
    }
    
    async fn get_symbol_orders(&self, symbol: &Symbol) -> Result<Vec<Order>, StorageError> {
        self.query_by_index(INDEX_SYMBOL, &symbol.0).await
    }
    
    #[instrument(skip(self, orders))]
    async fn batch_insert(&self, orders: Vec<Order>) -> Result<(), StorageError> {
        let start = Instant::now();
        let _lock = self.write_lock.lock().await;
        
        let cf = self.cf_handle(CF_ORDERS)?;
        let mut batch = WriteBatch::default();
        let mut total_bytes = 0u64;
        
        for order in &orders {
            let key = order.id.0.as_bytes().to_vec();
            let value = serialize(order)
                .map_err(|e| StorageError::SerializationError(e.to_string()))?;
            total_bytes += value.len() as u64;
            batch.put_cf(cf, key, value);
        }
        
        // Write batch
        let db = Arc::clone(&self.db);
        spawn_blocking(move || db.write(batch))
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        
        // Update indexes for all orders
        for order in &orders {
            self.update_indexes(order).await?;
        }
        
        // Update stats
        self.stats.writes.fetch_add(orders.len() as u64, Ordering::Relaxed);
        self.stats.bytes_written.fetch_add(total_bytes, Ordering::Relaxed);
        
        info!("Batch inserted {} orders in {:?}", orders.len(), start.elapsed());
        Ok(())
    }
    
    async fn clear(&self) -> Result<(), StorageError> {
        warn!("Clearing all persistent storage");
        
        // Clear all column families
        for cf_name in &[CF_ORDERS, CF_EVENTS, CF_INDEXES, CF_SNAPSHOTS] {
            let cf = self.cf_handle(cf_name)?;
            let db = Arc::clone(&self.db);
            let cf_clone = cf as *const ColumnFamily;
            
            spawn_blocking(move || unsafe {
                let mut batch = WriteBatch::default();
                let iter = db.iterator_cf(&*cf_clone, IteratorMode::Start);
                for item in iter {
                    if let Ok((key, _)) = item {
                        batch.delete_cf(&*cf_clone, key);
                    }
                }
                db.write(batch)
            })
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        }
        
        info!("Persistent storage cleared");
        Ok(())
    }
    
    async fn get_metrics(&self) -> Result<StorageMetrics, StorageError> {
        // Get DB stats for size estimation
        let db_stats = self.get_db_stats().await?;
        let size_bytes = db_stats.lines()
            .find(|line| line.contains("Total SST files size"))
            .and_then(|line| line.split_whitespace().nth(4))
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        
        Ok(StorageMetrics {
            total_orders: 0, // Would need to maintain count
            active_orders: 0, // Would need to query
            cache_hits: 0, // RocksDB handles internally
            cache_misses: 0,
            disk_reads: self.stats.reads.load(Ordering::Relaxed),
            disk_writes: self.stats.writes.load(Ordering::Relaxed),
            avg_read_latency_ns: 0, // Would need timing
            avg_write_latency_ns: 0,
            storage_size_bytes: size_bytes,
            last_snapshot: None,
        })
    }
}

#[async_trait]
impl EventStorage for PersistentStorage {
    async fn append_event(&self, event: OrderEvent) -> Result<(), StorageError> {
        let cf = self.cf_handle(CF_EVENTS)?;
        let timestamp = Utc::now();
        let key = timestamp.timestamp_nanos_opt().unwrap_or(0).to_be_bytes().to_vec();
        let value = serialize(&event)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        
        let db = Arc::clone(&self.db);
        let cf_clone = cf as *const ColumnFamily;
        spawn_blocking(move || unsafe {
            db.put_cf(&*cf_clone, key, value)
        })
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))?
        .map_err(|e| StorageError::BackendError(e.to_string()))?;
        
        Ok(())
    }
    
    async fn get_order_events(&self, order_id: &OrderId) -> Result<Vec<OrderEvent>, StorageError> {
        let cf = self.cf_handle(CF_EVENTS)?;
        let db = Arc::clone(&self.db);
        let cf_clone = cf as *const ColumnFamily;
        let order_id = order_id.clone();
        
        spawn_blocking(move || unsafe {
            let mut events = Vec::new();
            let iter = db.iterator_cf(&*cf_clone, IteratorMode::Start);
            
            for item in iter {
                if let Ok((_, value)) = item {
                    if let Ok(event) = deserialize::<OrderEvent>(&value) {
                        // Filter events for this order
                        match &event {
                            OrderEvent::Created { order_id: id, .. } |
                            OrderEvent::Updated { order_id: id, .. } |
                            OrderEvent::Filled { order_id: id, .. } |
                            OrderEvent::PartiallyFilled { order_id: id, .. } |
                            OrderEvent::Canceled { order_id: id, .. } |
                            OrderEvent::Rejected { order_id: id, .. } |
                            OrderEvent::Deleted { order_id: id, .. } => {
                                if id == &order_id {
                                    events.push(event);
                                }
                            }
                        }
                    }
                }
            }
            
            Ok(events)
        })
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))?
    }
    
    async fn get_events_by_time(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<OrderEvent>, StorageError> {
        let cf = self.cf_handle(CF_EVENTS)?;
        let start_key = start.timestamp_nanos_opt().unwrap_or(0).to_be_bytes().to_vec();
        let end_key = end.timestamp_nanos_opt().unwrap_or(0).to_be_bytes().to_vec();
        
        let db = Arc::clone(&self.db);
        let cf_clone = cf as *const ColumnFamily;
        
        spawn_blocking(move || unsafe {
            let mut events = Vec::new();
            let iter = db.iterator_cf(&*cf_clone, IteratorMode::From(&start_key, Direction::Forward));
            
            for item in iter {
                if let Ok((key, value)) = item {
                    if key.as_ref() > end_key.as_slice() {
                        break;
                    }
                    
                    if let Ok(event) = deserialize(&value) {
                        events.push(event);
                    }
                }
            }
            
            Ok(events)
        })
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))?
    }
    
    async fn replay_from(&self, checkpoint: DateTime<Utc>) -> Result<Vec<OrderEvent>, StorageError> {
        self.get_events_by_time(checkpoint, Utc::now()).await
    }
}

#[async_trait]
impl SnapshotStorage for PersistentStorage {
    async fn create_snapshot(&self) -> Result<String, StorageError> {
        let snapshot_id = Uuid::new_v4().to_string();
        let timestamp = Utc::now();
        
        let cf_snapshots = self.cf_handle(CF_SNAPSHOTS)?;
        let cf_orders = self.cf_handle(CF_ORDERS)?;
        
        // Collect all orders
        let db = Arc::clone(&self.db);
        let cf_orders_clone = cf_orders as *const ColumnFamily;
        let orders = spawn_blocking(move || unsafe {
            let mut orders = Vec::new();
            let iter = db.iterator_cf(&*cf_orders_clone, IteratorMode::Start);
            
            for item in iter {
                if let Ok((_, value)) = item {
                    if let Ok(order) = deserialize::<Order>(&value) {
                        orders.push(order);
                    }
                }
            }
            
            Ok::<Vec<Order>, StorageError>(orders)
        })
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))??;
        
        // Create snapshot info
        let info = SnapshotInfo {
            id: snapshot_id.clone(),
            created_at: timestamp,
            size_bytes: orders.len() as u64 * 1024, // Estimate
            order_count: orders.len() as u64,
            description: Some("Automatic snapshot".to_string()),
        };
        
        // Store snapshot
        let key = snapshot_id.as_bytes().to_vec();
        let value = serialize(&(info, orders))
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        
        let db = Arc::clone(&self.db);
        let cf_clone = cf_snapshots as *const ColumnFamily;
        spawn_blocking(move || unsafe {
            db.put_cf(&*cf_clone, key, value)
        })
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))?
        .map_err(|e| StorageError::BackendError(e.to_string()))?;
        
        self.stats.snapshots_created.fetch_add(1, Ordering::Relaxed);
        info!("Created snapshot: {}", snapshot_id);
        
        Ok(snapshot_id)
    }
    
    async fn restore_snapshot(&self, snapshot_id: &str) -> Result<(), StorageError> {
        let cf_snapshots = self.cf_handle(CF_SNAPSHOTS)?;
        let key = snapshot_id.as_bytes().to_vec();
        
        let db = Arc::clone(&self.db);
        let cf_clone = cf_snapshots as *const ColumnFamily;
        let snapshot_data = spawn_blocking(move || unsafe {
            db.get_cf(&*cf_clone, &key)
        })
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))?
        .map_err(|e| StorageError::BackendError(e.to_string()))?
        .ok_or_else(|| StorageError::BackendError(format!("Snapshot {} not found", snapshot_id)))?;
        
        let (_info, orders): (SnapshotInfo, Vec<Order>) = deserialize(&snapshot_data)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        
        // Clear existing data
        self.clear().await?;
        
        // Restore orders
        self.batch_insert(orders).await?;
        
        info!("Restored from snapshot: {}", snapshot_id);
        Ok(())
    }
    
    async fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>, StorageError> {
        let cf_snapshots = self.cf_handle(CF_SNAPSHOTS)?;
        
        let db = Arc::clone(&self.db);
        let cf_clone = cf_snapshots as *const ColumnFamily;
        
        spawn_blocking(move || unsafe {
            let mut snapshots = Vec::new();
            let iter = db.iterator_cf(&*cf_clone, IteratorMode::Start);
            
            for item in iter {
                if let Ok((_, value)) = item {
                    if let Ok((info, _)) = deserialize::<(SnapshotInfo, Vec<Order>)>(&value) {
                        snapshots.push(info);
                    }
                }
            }
            
            Ok(snapshots)
        })
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))?
    }
    
    async fn delete_snapshot(&self, snapshot_id: &str) -> Result<(), StorageError> {
        let cf_snapshots = self.cf_handle(CF_SNAPSHOTS)?;
        let key = snapshot_id.as_bytes().to_vec();
        
        let db = Arc::clone(&self.db);
        let cf_clone = cf_snapshots as *const ColumnFamily;
        spawn_blocking(move || unsafe {
            db.delete_cf(&*cf_clone, key)
        })
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))?
        .map_err(|e| StorageError::BackendError(e.to_string()))?;
        
        info!("Deleted snapshot: {}", snapshot_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::order::{Order, OrderSide, OrderType};
    use tempfile::TempDir;
    
    async fn create_test_storage() -> (PersistentStorage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        
        let storage = PersistentStorage::new(config).await.unwrap();
        (storage, temp_dir)
    }
    
    #[tokio::test]
    async fn test_persistent_storage_crud() {
        let (storage, _temp) = create_test_storage().await;
        
        // Create test order
        let order = Order::new(
            "TEST_CLIENT".into(),
            "AAPL".into(),
            100.0,
            OrderSide::Buy,
            OrderType::Market,
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
    async fn test_persistent_storage_indexes() {
        let (storage, _temp) = create_test_storage().await;
        
        // Insert test orders
        let client_id = ClientId("CLIENT_1".into());
        let symbol = Symbol("AAPL".into());
        
        for i in 0..5 {
            let order = Order::new(
                client_id.clone(),
                symbol.clone(),
                100.0 * (i + 1) as f64,
                OrderSide::Buy,
                OrderType::Market,
            );
            storage.insert_order(order).await.unwrap();
        }
        
        // Test client index
        let client_orders = storage.get_client_orders(&client_id).await.unwrap();
        assert_eq!(client_orders.len(), 5);
        
        // Test symbol index
        let symbol_orders = storage.get_symbol_orders(&symbol).await.unwrap();
        assert_eq!(symbol_orders.len(), 5);
    }
}
