// src/engine/lifecycle.rs - Order Lifecycle Management
//! Order state machine and lifecycle management
//!
//! This module manages the complete lifecycle of orders from creation to completion,
//! including state transitions, expiration, time-in-force handling, and event generation.
//!
//! ## Order State Machine
//!
//! ```text
//!                    ┌─────────┐
//!                    │   New   │
//!                    └────┬────┘
//!                         │ Submit
//!                         ▼
//!                  ┌─────────────┐
//!                  │  Validating │
//!                  └──────┬──────┘
//!                     Pass│ │Fail
//!              ┌──────────┘ └──────────┐
//!              ▼                       ▼
//!        ┌──────────┐            ┌──────────┐
//!        │  Active  │            │ Rejected │
//!        └────┬─────┘            └──────────┘
//!             │
//!      ┌──────┼──────┬──────────┬────────┐
//!      │      │      │          │        │
//!   Cancel  Match  Expire    Stop    Modify
//!      │      │      │      Triggered    │
//!      ▼      ▼      ▼          ▼        ▼
//! ┌────────┐ ┌─────────────┐ ┌──────┐ ┌──────────┐
//! │Canceled│ │PartiallyFill│ │Expired│ │ Pending  │
//! └────────┘ └──────┬──────┘ └──────┘ └──────────┘
//!                   │
//!              Full Fill
//!                   ▼
//!              ┌────────┐
//!              │ Filled │
//!              └────────┘
//! ```

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{
    sync::{broadcast, mpsc, Mutex},
    time::{interval, sleep},
};
use tracing::{debug, error, info, instrument, warn};

use crate::{
    core::{
        events::{OrderEvent, SystemEvent},
        order::{Order, OrderStatus, OrderType},
        types::{ClientId, OrderId, Symbol},
    },
    storage::OrderStorage,
};

/// Lifecycle configuration
#[derive(Debug, Clone, Deserialize)]
pub struct LifecycleConfig {
    /// Enable automatic order expiration
    pub enable_expiration: bool,
    /// Check interval for expired orders (seconds)
    pub expiration_check_interval: u64,
    /// Maximum pending state duration (seconds)
    pub max_pending_duration: u64,
    /// Enable state transition validation
    pub enforce_state_transitions: bool,
    /// Enable automatic cleanup of completed orders
    pub enable_auto_cleanup: bool,
    /// Time to keep completed orders before cleanup (seconds)
    pub completed_order_retention: u64,
    /// Maximum state transition history to keep
    pub max_history_entries: usize,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            enable_expiration: true,
            expiration_check_interval: 60,      // 1 minute
            max_pending_duration: 300,          // 5 minutes
            enforce_state_transitions: true,
            enable_auto_cleanup: true,
            completed_order_retention: 86400,   // 24 hours
            max_history_entries: 100,
        }
    }
}

/// Order lifecycle event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleEvent {
    pub order_id: OrderId,
    pub timestamp: DateTime<Utc>,
    pub event_type: LifecycleEventType,
    pub from_state: OrderStatus,
    pub to_state: OrderStatus,
    pub details: Option<String>,
}

/// Types of lifecycle events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LifecycleEventType {
    Created,
    Submitted,
    Validated,
    Activated,
    Modified,
    Filled,
    PartiallyFilled,
    Canceled,
    Expired,
    Rejected,
    Completed,
}

/// State transition rule
#[derive(Debug, Clone)]
pub struct StateTransition {
    pub from: OrderStatus,
    pub to: OrderStatus,
    pub allowed: bool,
    pub requires_validation: bool,
    pub emit_event: bool,
}

/// Order lifecycle state with metadata
#[derive(Debug, Clone)]
pub struct LifecycleState {
    pub order_id: OrderId,
    pub current_status: OrderStatus,
    pub previous_status: Option<OrderStatus>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state_entered_at: DateTime<Utc>,
    pub expire_time: Option<DateTime<Utc>>,
    pub history: VecDeque<LifecycleEvent>,
    pub metadata: HashMap<String, String>,
}

impl LifecycleState {
    fn new(order_id: OrderId, status: OrderStatus) -> Self {
        let now = Utc::now();
        Self {
            order_id,
            current_status: status,
            previous_status: None,
            created_at: now,
            updated_at: now,
            state_entered_at: now,
            expire_time: None,
            history: VecDeque::new(),
            metadata: HashMap::new(),
        }
    }

    fn add_event(&mut self, event: LifecycleEvent, max_entries: usize) {
        self.history.push_back(event);
        while self.history.len() > max_entries {
            self.history.pop_front();
        }
    }
}

/// Order lifecycle manager
pub struct LifecycleManager {
    /// Configuration
    config: Arc<LifecycleConfig>,
    /// Order states
    states: Arc<DashMap<OrderId, LifecycleState>>,
    /// State transition rules
    transition_rules: Arc<RwLock<HashMap<(OrderStatus, OrderStatus), StateTransition>>>,
    /// Storage backend
    storage: Option<Arc<dyn OrderStorage>>,
    /// Event broadcast channel
    event_tx: broadcast::Sender<LifecycleEvent>,
    /// Command channel
    command_tx: mpsc::Sender<LifecycleCommand>,
    command_rx: Arc<Mutex<mpsc::Receiver<LifecycleCommand>>>,
    /// Statistics
    stats: Arc<LifecycleStats>,
    /// Running flag
    is_running: Arc<RwLock<bool>>,
}

/// Lifecycle management commands
#[derive(Debug)]
pub enum LifecycleCommand {
    TransitionOrder {
        order_id: OrderId,
        new_status: OrderStatus,
        reason: Option<String>,
    },
    ExpireOrder {
        order_id: OrderId,
    },
    CleanupCompleted,
    Stop,
}

/// Lifecycle statistics
#[derive(Debug, Default)]
pub struct LifecycleStats {
    pub total_orders: AtomicU64,
    pub active_orders: AtomicU64,
    pub completed_orders: AtomicU64,
    pub expired_orders: AtomicU64,
    pub canceled_orders: AtomicU64,
    pub rejected_orders: AtomicU64,
    pub state_transitions: AtomicU64,
    pub invalid_transitions: AtomicU64,
}

impl LifecycleManager {
    /// Create new lifecycle manager
    pub fn new(config: LifecycleConfig) -> Self {
        let (event_tx, _) = broadcast::channel(10000);
        let (command_tx, command_rx) = mpsc::channel(1000);
        
        let mut manager = Self {
            config: Arc::new(config),
            states: Arc::new(DashMap::new()),
            transition_rules: Arc::new(RwLock::new(HashMap::new())),
            storage: None,
            event_tx,
            command_tx,
            command_rx: Arc::new(Mutex::new(command_rx)),
            stats: Arc::new(LifecycleStats::default()),
            is_running: Arc::new(RwLock::new(false)),
        };
        
        manager.initialize_transition_rules();
        manager
    }

    /// Set storage backend
    pub fn with_storage(mut self, storage: Arc<dyn OrderStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Initialize state transition rules
    fn initialize_transition_rules(&mut self) {
        let mut rules = self.transition_rules.write();
        
        // Define valid state transitions
        let transitions = vec![
            // From New
            (OrderStatus::New, OrderStatus::Active, true, false),
            (OrderStatus::New, OrderStatus::Rejected, true, false),
            (OrderStatus::New, OrderStatus::Canceled, true, false),
            
            // From Active
            (OrderStatus::Active, OrderStatus::PartiallyFilled, true, false),
            (OrderStatus::Active, OrderStatus::Filled, true, false),
            (OrderStatus::Active, OrderStatus::Canceled, true, false),
            (OrderStatus::Active, OrderStatus::Expired, true, false),
            
            // From PartiallyFilled
            (OrderStatus::PartiallyFilled, OrderStatus::Filled, true, false),
            (OrderStatus::PartiallyFilled, OrderStatus::Canceled, true, false),
            (OrderStatus::PartiallyFilled, OrderStatus::Expired, true, false),
            
            // Terminal states - no transitions allowed
            (OrderStatus::Filled, OrderStatus::Filled, false, false),
            (OrderStatus::Canceled, OrderStatus::Canceled, false, false),
            (OrderStatus::Rejected, OrderStatus::Rejected, false, false),
            (OrderStatus::Expired, OrderStatus::Expired, false, false),
        ];
        
        for (from, to, allowed, requires_validation) in transitions {
            rules.insert(
                (from, to),
                StateTransition {
                    from,
                    to,
                    allowed,
                    requires_validation,
                    emit_event: true,
                },
            );
        }
    }

    /// Start lifecycle manager
    pub async fn start(&self) -> Result<()> {
        if *self.is_running.read() {
            return Ok(());
        }
        
        *self.is_running.write() = true;
        info!("Starting lifecycle manager");
        
        // Start background tasks
        self.spawn_command_handler();
        
        if self.config.enable_expiration {
            self.spawn_expiration_checker();
        }
        
        if self.config.enable_auto_cleanup {
            self.spawn_cleanup_task();
        }
        
        Ok(())
    }

    /// Stop lifecycle manager
    pub async fn stop(&self) -> Result<()> {
        *self.is_running.write() = false;
        let _ = self.command_tx.send(LifecycleCommand::Stop).await;
        info!("Lifecycle manager stopped");
        Ok(())
    }

    /// Register a new order
    #[instrument(skip(self, order))]
    pub async fn register_order(&self, order: &Order) -> Result<()> {
        let state = LifecycleState::new(order.id.clone(), order.status);
        
        // Set expiration based on time-in-force
        let mut state = state;
        state.expire_time = self.calculate_expire_time(order);
        
        self.states.insert(order.id.clone(), state);
        self.stats.total_orders.fetch_add(1, Ordering::Relaxed);
        
        if matches!(order.status, OrderStatus::New | OrderStatus::Active) {
            self.stats.active_orders.fetch_add(1, Ordering::Relaxed);
        }
        
        // Emit created event
        self.emit_event(LifecycleEvent {
            order_id: order.id.clone(),
            timestamp: Utc::now(),
            event_type: LifecycleEventType::Created,
            from_state: OrderStatus::New,
            to_state: order.status,
            details: None,
        });
        
        debug!("Registered order {} with status {:?}", order.id, order.status);
        Ok(())
    }

    /// Transition order to new state
    #[instrument(skip(self))]
    pub async fn transition_order(
        &self,
        order_id: &OrderId,
        new_status: OrderStatus,
        reason: Option<String>,
    ) -> Result<()> {
        let mut state = self.states.get_mut(order_id)
            .ok_or_else(|| anyhow!("Order {} not found", order_id))?;
        
        let current_status = state.current_status;
        
        // Check if transition is allowed
        if self.config.enforce_state_transitions {
            if !self.is_transition_allowed(current_status, new_status) {
                self.stats.invalid_transitions.fetch_add(1, Ordering::Relaxed);
                return Err(anyhow!(
                    "Invalid state transition from {:?} to {:?}",
                    current_status, new_status
                ));
            }
        }
        
        // Update state
        state.previous_status = Some(current_status);
        state.current_status = new_status;
        state.updated_at = Utc::now();
        state.state_entered_at = Utc::now();
        
        // Add to history
        let event = LifecycleEvent {
            order_id: order_id.clone(),
            timestamp: Utc::now(),
            event_type: self.get_event_type(new_status),
            from_state: current_status,
            to_state: new_status,
            details: reason.clone(),
        };
        
        state.add_event(event.clone(), self.config.max_history_entries);
        
        // Update statistics
        self.update_statistics(current_status, new_status);
        
        // Update storage if available
        if let Some(ref storage) = self.storage {
            if let Ok(mut order) = storage.get_order(order_id).await {
                if let Some(mut order) = order {
                    order.status = new_status;
                    order.updated_at = Utc::now();
                    let _ = storage.update_order(order).await;
                }
            }
        }
        
        // Emit event
        self.emit_event(event);
        
        info!("Order {} transitioned from {:?} to {:?}", order_id, current_status, new_status);
        Ok(())
    }

    /// Check if state transition is allowed
    fn is_transition_allowed(&self, from: OrderStatus, to: OrderStatus) -> bool {
        let rules = self.transition_rules.read();
        rules.get(&(from, to))
            .map(|rule| rule.allowed)
            .unwrap_or(false)
    }

    /// Get event type for status
    fn get_event_type(&self, status: OrderStatus) -> LifecycleEventType {
        match status {
            OrderStatus::New => LifecycleEventType::Created,
            OrderStatus::Active => LifecycleEventType::Activated,
            OrderStatus::PartiallyFilled => LifecycleEventType::PartiallyFilled,
            OrderStatus::Filled => LifecycleEventType::Filled,
            OrderStatus::Canceled => LifecycleEventType::Canceled,
            OrderStatus::Rejected => LifecycleEventType::Rejected,
            OrderStatus::Expired => LifecycleEventType::Expired,
        }
    }

    /// Calculate expiration time based on order parameters
    fn calculate_expire_time(&self, order: &Order) -> Option<DateTime<Utc>> {
        match order.time_in_force() {
            "DAY" => {
                // Expire at end of trading day (simplified: midnight UTC)
                let tomorrow = Utc::now().date_naive().succ_opt()
                    .and_then(|d| d.and_hms_opt(0, 0, 0))
                    .map(|dt| DateTime::from_naive_utc_and_offset(dt, Utc));
                tomorrow
            }
            "IOC" | "FOK" => {
                // Immediate expiration
                Some(Utc::now() + Duration::seconds(1))
            }
            "GTD" => {
                // Good-till-date - would need to check order.expire_time field
                None // Placeholder
            }
            _ => None, // GTC or unknown
        }
    }

    /// Update statistics based on state transition
    fn update_statistics(&self, from: OrderStatus, to: OrderStatus) {
        self.stats.state_transitions.fetch_add(1, Ordering::Relaxed);
        
        // Update active orders count
        let was_active = matches!(from, OrderStatus::New | OrderStatus::Active | OrderStatus::PartiallyFilled);
        let is_active = matches!(to, OrderStatus::New | OrderStatus::Active | OrderStatus::PartiallyFilled);
        
        if was_active && !is_active {
            self.stats.active_orders.fetch_sub(1, Ordering::Relaxed);
            self.stats.completed_orders.fetch_add(1, Ordering::Relaxed);
        } else if !was_active && is_active {
            self.stats.active_orders.fetch_add(1, Ordering::Relaxed);
        }
        
        // Update specific counters
        match to {
            OrderStatus::Expired => {
                self.stats.expired_orders.fetch_add(1, Ordering::Relaxed);
            }
            OrderStatus::Canceled => {
                self.stats.canceled_orders.fetch_add(1, Ordering::Relaxed);
            }
            OrderStatus::Rejected => {
                self.stats.rejected_orders.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Emit lifecycle event
    fn emit_event(&self, event: LifecycleEvent) {
        let _ = self.event_tx.send(event);
    }

    /// Subscribe to lifecycle events
    pub fn subscribe(&self) -> broadcast::Receiver<LifecycleEvent> {
        self.event_tx.subscribe()
    }

    /// Get order lifecycle state
    pub fn get_state(&self, order_id: &OrderId) -> Option<LifecycleState> {
        self.states.get(order_id).map(|s| s.clone())
    }

    /// Get all active order IDs
    pub fn get_active_orders(&self) -> Vec<OrderId> {
        self.states
            .iter()
            .filter(|entry| {
                matches!(
                    entry.value().current_status,
                    OrderStatus::New | OrderStatus::Active | OrderStatus::PartiallyFilled
                )
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Handle order fill
    pub async fn handle_fill(&self, order_id: &OrderId, quantity: f64, remaining: f64) -> Result<()> {
        let new_status = if remaining > 0.0 {
            OrderStatus::PartiallyFilled
        } else {
            OrderStatus::Filled
        };
        
        self.transition_order(
            order_id,
            new_status,
            Some(format!("Filled {} units", quantity)),
        ).await
    }

    /// Handle order cancellation
    pub async fn handle_cancel(&self, order_id: &OrderId, reason: Option<String>) -> Result<()> {
        self.transition_order(
            order_id,
            OrderStatus::Canceled,
            reason.or_else(|| Some("User requested".to_string())),
        ).await
    }

    /// Handle order rejection
    pub async fn handle_reject(&self, order_id: &OrderId, reason: String) -> Result<()> {
        self.transition_order(
            order_id,
            OrderStatus::Rejected,
            Some(reason),
        ).await
    }

    /// Check and expire orders
    async fn check_expired_orders(&self) -> Result<()> {
        let now = Utc::now();
        let expired_orders: Vec<OrderId> = self.states
            .iter()
            .filter(|entry| {
                let state = entry.value();
                // Only expire active orders
                matches!(state.current_status, OrderStatus::Active | OrderStatus::PartiallyFilled) &&
                state.expire_time.map_or(false, |exp| exp <= now)
            })
            .map(|entry| entry.key().clone())
            .collect();
        
        for order_id in expired_orders {
            if let Err(e) = self.transition_order(
                &order_id,
                OrderStatus::Expired,
                Some("Time in force expired".to_string()),
            ).await {
                warn!("Failed to expire order {}: {}", order_id, e);
            }
        }
        
        Ok(())
    }

    /// Clean up completed orders
    async fn cleanup_completed_orders(&self) -> Result<()> {
        let cutoff = Utc::now() - Duration::seconds(self.config.completed_order_retention as i64);
        
        let to_remove: Vec<OrderId> = self.states
            .iter()
            .filter(|entry| {
                let state = entry.value();
                matches!(
                    state.current_status,
                    OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected | OrderStatus::Expired
                ) && state.updated_at < cutoff
            })
            .map(|entry| entry.key().clone())
            .collect();
        
        for order_id in to_remove {
            self.states.remove(&order_id);
            debug!("Cleaned up completed order {}", order_id);
        }
        
        Ok(())
    }

    /// Spawn command handler task
    fn spawn_command_handler(&self) {
        let manager = self.clone();
        
        tokio::spawn(async move {
            let mut command_rx = manager.command_rx.lock().await;
            
            while let Some(command) = command_rx.recv().await {
                match command {
                    LifecycleCommand::TransitionOrder { order_id, new_status, reason } => {
                        if let Err(e) = manager.transition_order(&order_id, new_status, reason).await {
                            error!("Failed to transition order {}: {}", order_id, e);
                        }
                    }
                    LifecycleCommand::ExpireOrder { order_id } => {
                        if let Err(e) = manager.transition_order(
                            &order_id,
                            OrderStatus::Expired,
                            Some("Manual expiration".to_string()),
                        ).await {
                            error!("Failed to expire order {}: {}", order_id, e);
                        }
                    }
                    LifecycleCommand::CleanupCompleted => {
                        if let Err(e) = manager.cleanup_completed_orders().await {
                            error!("Failed to cleanup orders: {}", e);
                        }
                    }
                    LifecycleCommand::Stop => {
                        break;
                    }
                }
            }
        });
    }

    /// Spawn expiration checker task
    fn spawn_expiration_checker(&self) {
        let manager = self.clone();
        let check_interval = self.config.expiration_check_interval;
        
        tokio::spawn(async move {
            let mut interval = interval(tokio::time::Duration::from_secs(check_interval));
            
            while *manager.is_running.read() {
                interval.tick().await;
                
                if let Err(e) = manager.check_expired_orders().await {
                    error!("Error checking expired orders: {}", e);
                }
            }
        });
    }

    /// Spawn cleanup task
    fn spawn_cleanup_task(&self) {
        let manager = self.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(tokio::time::Duration::from_secs(3600)); // Every hour
            
            while *manager.is_running.read() {
                interval.tick().await;
                
                if let Err(e) = manager.cleanup_completed_orders().await {
                    error!("Error cleaning up orders: {}", e);
                }
            }
        });
    }

    /// Get statistics
    pub fn get_stats(&self) -> LifecycleStats {
        LifecycleStats {
            total_orders: AtomicU64::new(self.stats.total_orders.load(Ordering::Relaxed)),
            active_orders: AtomicU64::new(self.stats.active_orders.load(Ordering::Relaxed)),
            completed_orders: AtomicU64::new(self.stats.completed_orders.load(Ordering::Relaxed)),
            expired_orders: AtomicU64::new(self.stats.expired_orders.load(Ordering::Relaxed)),
            canceled_orders: AtomicU64::new(self.stats.canceled_orders.load(Ordering::Relaxed)),
            rejected_orders: AtomicU64::new(self.stats.rejected_orders.load(Ordering::Relaxed)),
            state_transitions: AtomicU64::new(self.stats.state_transitions.load(Ordering::Relaxed)),
            invalid_transitions: AtomicU64::new(self.stats.invalid_transitions.load(Ordering::Relaxed)),
        }
    }

    /// Add custom state transition rule
    pub fn add_transition_rule(&self, from: OrderStatus, to: OrderStatus, allowed: bool) {
        let mut rules = self.transition_rules.write();
        rules.insert(
            (from, to),
            StateTransition {
                from,
                to,
                allowed,
                requires_validation: false,
                emit_event: true,
            },
        );
    }

    /// Set order metadata
    pub fn set_metadata(&self, order_id: &OrderId, key: String, value: String) {
        if let Some(mut state) = self.states.get_mut(order_id) {
            state.metadata.insert(key, value);
            state.updated_at = Utc::now();
        }
    }

    /// Get order metadata
    pub fn get_metadata(&self, order_id: &OrderId, key: &str) -> Option<String> {
        self.states.get(order_id)
            .and_then(|state| state.metadata.get(key).cloned())
    }
}

// Clone implementation for manager
impl Clone for LifecycleManager {
    fn clone(&self) -> Self {
        Self {
            config: Arc::clone(&self.config),
            states: Arc::clone(&self.states),
            transition_rules: Arc::clone(&self.transition_rules),
            storage: self.storage.clone(),
            event_tx: self.event_tx.clone(),
            command_tx: self.command_tx.clone(),
            command_rx: Arc::clone(&self.command_rx),
            stats: Arc::clone(&self.stats),
            is_running: Arc::clone(&self.is_running),
        }
    }
}

/// Helper functions for order lifecycle
pub mod helpers {
    use super::*;

    /// Check if order is in terminal state
    pub fn is_terminal_state(status: OrderStatus) -> bool {
        matches!(
            status,
            OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected | OrderStatus::Expired
        )
    }

    /// Check if order is active
    pub fn is_active_state(status: OrderStatus) -> bool {
        matches!(
            status,
            OrderStatus::New | OrderStatus::Active | OrderStatus::PartiallyFilled
        )
    }

    /// Get valid next states for current status
    pub fn get_valid_transitions(current: OrderStatus) -> Vec<OrderStatus> {
        match current {
            OrderStatus::New => vec![
                OrderStatus::Active,
                OrderStatus::Rejected,
                OrderStatus::Canceled,
            ],
            OrderStatus::Active => vec![
                OrderStatus::PartiallyFilled,
                OrderStatus::Filled,
                OrderStatus::Canceled,
                OrderStatus::Expired,
            ],
            OrderStatus::PartiallyFilled => vec![
                OrderStatus::Filled,
                OrderStatus::Canceled,
                OrderStatus::Expired,
            ],
            _ => vec![], // Terminal states have no transitions
        }
    }

    /// Calculate time in state
    pub fn time_in_state(state: &LifecycleState) -> Duration {
        let now = Utc::now();
        now.signed_duration_since(state.state_entered_at)
            .to_std()
            .unwrap_or(std::time::Duration::ZERO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::order::{OrderSide, OrderType};

    #[tokio::test]
    async fn test_lifecycle_manager_creation() {
        let config = LifecycleConfig::default();
        let manager = LifecycleManager::new(config);
        
        assert!(manager.start().await.is_ok());
        assert!(manager.stop().await.is_ok());
    }

    #[tokio::test]
    async fn test_order_registration() {
        let manager = LifecycleManager::new(LifecycleConfig::default());
        manager.start().await.unwrap();
        
        let order = Order::new(
            "CLIENT_1".into(),
            "AAPL".into(),
            100.0,
            OrderSide::Buy,
            OrderType::Market,
        );
        
        assert!(manager.register_order(&order).await.is_ok());
        
        let state = manager.get_state(&order.id);
        assert!(state.is_some());
        assert_eq!(state.unwrap().current_status, OrderStatus::New);
        
        manager.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_state_transition() {
        let manager = LifecycleManager::new(LifecycleConfig::default());
        manager.start().await.unwrap();
        
        let order = Order::new(
            "CLIENT_1".into(),
            "AAPL".into(),
            100.0,
            OrderSide::Buy,
            OrderType::Market,
        );
        
        manager.register_order(&order).await.unwrap();
        
        // Valid transition: New -> Active
        assert!(manager.transition_order(
            &order.id,
            OrderStatus::Active,
            None,
        ).await.is_ok());
        
        // Invalid transition: Active -> New
        assert!(manager.transition_order(
            &order.id,
            OrderStatus::New,
            None,
        ).await.is_err());
        
        // Valid transition: Active -> Filled
        assert!(manager.transition_order(
            &order.id,
            OrderStatus::Filled,
            Some("Test fill".to_string()),
        ).await.is_ok());
        
        let state = manager.get_state(&order.id).unwrap();
        assert_eq!(state.current_status, OrderStatus::Filled);
        assert_eq!(state.previous_status, Some(OrderStatus::Active));
        assert_eq!(state.history.len(), 3); // Created, Active, Filled
        
        manager.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_handle_fill() {
        let manager = LifecycleManager::new(LifecycleConfig::default());
        manager.start().await.unwrap();
        
        let order = Order::new(
            "CLIENT_1".into(),
            "AAPL".into(),
            100.0,
            OrderSide::Buy,
            OrderType::Market,
        );
        
        manager.register_order(&order).await.unwrap();
        manager.transition_order(&order.id, OrderStatus::Active, None).await.unwrap();
        
        // Partial fill
        assert!(manager.handle_fill(&order.id, 50.0, 50.0).await.is_ok());
        let state = manager.get_state(&order.id).unwrap();
        assert_eq!(state.current_status, OrderStatus::PartiallyFilled);
        
        // Complete fill
        assert!(manager.handle_fill(&order.id, 50.0, 0.0).await.is_ok());
        let state = manager.get_state(&order.id).unwrap();
        assert_eq!(state.current_status, OrderStatus::Filled);
        
        manager.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_metadata() {
        let manager = LifecycleManager::new(LifecycleConfig::default());
        
        let order = Order::new(
            "CLIENT_1".into(),
            "AAPL".into(),
            100.0,
            OrderSide::Buy,
            OrderType::Market,
        );
        
        manager.register_order(&order).await.unwrap();
        
        // Set metadata
        manager.set_metadata(&order.id, "venue".to_string(), "NYSE".to_string());
        manager.set_metadata(&order.id, "algo".to_string(), "VWAP".to_string());
        
        // Get metadata
        assert_eq!(manager.get_metadata(&order.id, "venue"), Some("NYSE".to_string()));
        assert_eq!(manager.get_metadata(&order.id, "algo"), Some("VWAP".to_string()));
        assert_eq!(manager.get_metadata(&order.id, "missing"), None);
    }

    #[test]
    fn test_helper_functions() {
        use helpers::*;
        
        assert!(is_terminal_state(OrderStatus::Filled));
        assert!(is_terminal_state(OrderStatus::Canceled));
        assert!(!is_terminal_state(OrderStatus::Active));
        
        assert!(is_active_state(OrderStatus::New));
        assert!(is_active_state(OrderStatus::Active));
        assert!(!is_active_state(OrderStatus::Filled));
        
        let valid = get_valid_transitions(OrderStatus::Active);
        assert!(valid.contains(&OrderStatus::Filled));
        assert!(valid.contains(&OrderStatus::Canceled));
        assert!(!valid.contains(&OrderStatus::New));
    }
}
