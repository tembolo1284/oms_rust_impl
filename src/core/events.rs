// src/core/events.rs - Event System
//! Event system for order lifecycle and system events
//!
//! This module defines events that occur throughout the system lifecycle,
//! enabling event-driven architecture and audit trails.

use crate::core::types::{AccountId, ExecutionId, OrderId, Price, Quantity, Symbol, Timestamp, Venue};
use crate::{Event, OmsError};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Order-related event types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderEventType {
    /// Order was created
    OrderCreated,
    /// Order was submitted to venue
    OrderSubmitted,
    /// Order was acknowledged by venue
    OrderAcknowledged,
    /// Order received a fill
    OrderFilled,
    /// Order was partially filled
    OrderPartiallyFilled,
    /// Order was completely filled
    OrderCompletelyFilled,
    /// Order was canceled
    OrderCanceled,
    /// Cancel request was sent
    CancelRequested,
    /// Cancel was rejected
    CancelRejected,
    /// Order was replaced/modified
    OrderReplaced,
    /// Replace request was sent
    ReplaceRequested,
    /// Replace was rejected
    ReplaceRejected,
    /// Order was rejected
    OrderRejected,
    /// Order expired
    OrderExpired,
    /// Order was suspended
    OrderSuspended,
    /// Order was resumed
    OrderResumed,
    /// Risk check failed
    RiskCheckFailed,
    /// Risk check passed
    RiskCheckPassed,
}

/// Order event containing order state changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderEvent {
    /// Event ID
    pub id: uuid::Uuid,
    /// Event type
    pub event_type: OrderEventType,
    /// Order ID
    pub order_id: OrderId,
    /// Client order ID
    pub client_order_id: String,
    /// Account ID
    pub account_id: AccountId,
    /// Symbol
    pub symbol: Symbol,
    /// Event timestamp
    pub timestamp: Timestamp,
    /// Previous order status
    pub previous_status: Option<crate::core::order::OrderStatus>,
    /// New order status
    pub new_status: crate::core::order::OrderStatus,
    /// Fill details (if applicable)
    pub fill_details: Option<FillDetails>,
    /// Venue where event occurred
    pub venue: Option<Venue>,
    /// Error message (for rejections)
    pub error_message: Option<String>,
    /// Error code (for rejections)
    pub error_code: Option<String>,
    /// Additional event data
    pub metadata: HashMap<String, String>,
    /// Sequence number for ordering
    pub sequence_number: u64,
}

/// Fill details for execution events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillDetails {
    /// Execution ID
    pub execution_id: ExecutionId,
    /// Fill price
    pub price: Price,
    /// Fill quantity
    pub quantity: Quantity,
    /// Cumulative filled quantity
    pub cumulative_quantity: Quantity,
    /// Average fill price
    pub average_price: Price,
    /// Commission charged
    pub commission: Option<Price>,
    /// Commission currency
    pub commission_currency: Option<String>,
    /// Contra party
    pub contra_party: Option<String>,
    /// Trade ID
    pub trade_id: Option<String>,
    /// Settlement date
    pub settlement_date: Option<chrono::DateTime<chrono::Utc>>,
}

impl OrderEvent {
    /// Create a new order created event
    pub fn order_created(
        order_id: OrderId,
        client_order_id: String,
        account_id: AccountId,
        symbol: Symbol,
        sequence_number: u64,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            event_type: OrderEventType::OrderCreated,
            order_id,
            client_order_id,
            account_id,
            symbol,
            timestamp: Utc::now(),
            previous_status: None,
            new_status: crate::core::order::OrderStatus::New,
            fill_details: None,
            venue: None,
            error_message: None,
            error_code: None,
            metadata: HashMap::new(),
            sequence_number,
        }
    }
    
    /// Create a new order fill event
    pub fn order_filled(
        order_id: OrderId,
        client_order_id: String,
        account_id: AccountId,
        symbol: Symbol,
        previous_status: crate::core::order::OrderStatus,
        new_status: crate::core::order::OrderStatus,
        fill_details: FillDetails,
        venue: Option<Venue>,
        sequence_number: u64,
    ) -> Self {
        let event_type = match new_status {
            crate::core::order::OrderStatus::Filled => OrderEventType::OrderCompletelyFilled,
            crate::core::order::OrderStatus::PartiallyFilled => OrderEventType::OrderPartiallyFilled,
            _ => OrderEventType::OrderFilled,
        };
        
        Self {
            id: uuid::Uuid::new_v4(),
            event_type,
            order_id,
            client_order_id,
            account_id,
            symbol,
            timestamp: Utc::now(),
            previous_status: Some(previous_status),
            new_status,
            fill_details: Some(fill_details),
            venue,
            error_message: None,
            error_code: None,
            metadata: HashMap::new(),
            sequence_number,
        }
    }
    
    /// Create a new order rejected event
    pub fn order_rejected(
        order_id: OrderId,
        client_order_id: String,
        account_id: AccountId,
        symbol: Symbol,
        previous_status: Option<crate::core::order::OrderStatus>,
        error_message: String,
        error_code: String,
        venue: Option<Venue>,
        sequence_number: u64,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            event_type: OrderEventType::OrderRejected,
            order_id,
            client_order_id,
            account_id,
            symbol,
            timestamp: Utc::now(),
            previous_status,
            new_status: crate::core::order::OrderStatus::Rejected,
            fill_details: None,
            venue,
            error_message: Some(error_message),
            error_code: Some(error_code),
            metadata: HashMap::new(),
            sequence_number,
        }
    }
    
    /// Create a new order canceled event
    pub fn order_canceled(
        order_id: OrderId,
        client_order_id: String,
        account_id: AccountId,
        symbol: Symbol,
        previous_status: crate::core::order::OrderStatus,
        venue: Option<Venue>,
        sequence_number: u64,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            event_type: OrderEventType::OrderCanceled,
            order_id,
            client_order_id,
            account_id,
            symbol,
            timestamp: Utc::now(),
            previous_status: Some(previous_status),
            new_status: crate::core::order::OrderStatus::Canceled,
            fill_details: None,
            venue,
            error_message: None,
            error_code: None,
            metadata: HashMap::new(),
            sequence_number,
        }
    }
    
    /// Add metadata to the event
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
    
    /// Check if this is a terminal event (order reached final state)
    pub fn is_terminal(&self) -> bool {
        self.new_status.is_final()
    }
    
    /// Check if this is a fill event
    pub fn is_fill(&self) -> bool {
        matches!(
            self.event_type,
            OrderEventType::OrderFilled
                | OrderEventType::OrderPartiallyFilled
                | OrderEventType::OrderCompletelyFilled
        )
    }
    
    /// Check if this is an error/rejection event
    pub fn is_error(&self) -> bool {
        matches!(
            self.event_type,
            OrderEventType::OrderRejected
                | OrderEventType::CancelRejected
                | OrderEventType::ReplaceRejected
                | OrderEventType::RiskCheckFailed
        )
    }
}

impl Event for OrderEvent {
    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
    
    fn event_type(&self) -> &'static str {
        match self.event_type {
            OrderEventType::OrderCreated => "order_created",
            OrderEventType::OrderSubmitted => "order_submitted",
            OrderEventType::OrderAcknowledged => "order_acknowledged",
            OrderEventType::OrderFilled => "order_filled",
            OrderEventType::OrderPartiallyFilled => "order_partially_filled",
            OrderEventType::OrderCompletelyFilled => "order_completely_filled",
            OrderEventType::OrderCanceled => "order_canceled",
            OrderEventType::CancelRequested => "cancel_requested",
            OrderEventType::CancelRejected => "cancel_rejected",
            OrderEventType::OrderReplaced => "order_replaced",
            OrderEventType::ReplaceRequested => "replace_requested",
            OrderEventType::ReplaceRejected => "replace_rejected",
            OrderEventType::OrderRejected => "order_rejected",
            OrderEventType::OrderExpired => "order_expired",
            OrderEventType::OrderSuspended => "order_suspended",
            OrderEventType::OrderResumed => "order_resumed",
            OrderEventType::RiskCheckFailed => "risk_check_failed",
            OrderEventType::RiskCheckPassed => "risk_check_passed",
        }
    }
    
    fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

/// System event types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SystemEventType {
    /// System started
    SystemStarted,
    /// System shutting down
    SystemShuttingDown,
    /// System shutdown complete
    SystemShutdown,
    /// Connection established
    ConnectionEstablished,
    /// Connection lost
    ConnectionLost,
    /// Market data connected
    MarketDataConnected,
    /// Market data disconnected
    MarketDataDisconnected,
    /// Venue connected
    VenueConnected,
    /// Venue disconnected
    VenueDisconnected,
    /// Configuration updated
    ConfigurationUpdated,
    /// Error occurred
    ErrorOccurred,
    /// Performance warning
    PerformanceWarning,
    /// Health check
    HealthCheck,
}

/// System event for infrastructure and operational events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemEvent {
    /// Event ID
    pub id: uuid::Uuid,
    /// Event type
    pub event_type: SystemEventType,
    /// Event timestamp
    pub timestamp: Timestamp,
    /// Component that generated the event
    pub component: String,
    /// Event message
    pub message: String,
    /// Event severity level
    pub severity: EventSeverity,
    /// Additional event data
    pub metadata: HashMap<String, String>,
}

/// Event severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSeverity {
    /// Debug information
    Debug,
    /// Informational message
    Info,
    /// Warning message
    Warning,
    /// Error message
    Error,
    /// Critical error
    Critical,
}

impl SystemEvent {
    /// Create a new system event
    pub fn new(
        event_type: SystemEventType,
        component: String,
        message: String,
        severity: EventSeverity,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            event_type,
            timestamp: Utc::now(),
            component,
            message,
            severity,
            metadata: HashMap::new(),
        }
    }
    
    /// Add metadata to the event
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

impl Event for SystemEvent {
    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
    
    fn event_type(&self) -> &'static str {
        match self.event_type {
            SystemEventType::SystemStarted => "system_started",
            SystemEventType::SystemShuttingDown => "system_shutting_down",
            SystemEventType::SystemShutdown => "system_shutdown",
            SystemEventType::ConnectionEstablished => "connection_established",
            SystemEventType::ConnectionLost => "connection_lost",
            SystemEventType::MarketDataConnected => "market_data_connected",
            SystemEventType::MarketDataDisconnected => "market_data_disconnected",
            SystemEventType::VenueConnected => "venue_connected",
            SystemEventType::VenueDisconnected => "venue_disconnected",
            SystemEventType::ConfigurationUpdated => "configuration_updated",
            SystemEventType::ErrorOccurred => "error_occurred",
            SystemEventType::PerformanceWarning => "performance_warning",
            SystemEventType::HealthCheck => "health_check",
        }
    }
    
    fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

/// Risk event types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskEventType {
    /// Position limit breached
    PositionLimitBreached,
    /// Order size limit exceeded
    OrderSizeLimitExceeded,
    /// Daily loss limit approached
    DailyLossLimitApproached,
    /// Daily loss limit breached
    DailyLossLimitBreached,
    /// Concentration limit breached
    ConcentrationLimitBreached,
    /// Leverage limit exceeded
    LeverageLimitExceeded,
    /// Fat finger check triggered
    FatFingerTriggered,
    /// Order rate limit exceeded
    OrderRateLimitExceeded,
    /// Margin call issued
    MarginCallIssued,
    /// Account suspended
    AccountSuspended,
    /// Risk override applied
    RiskOverrideApplied,
}

/// Risk event for risk management notifications
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskEvent {
    /// Event ID
    pub id: uuid::Uuid,
    /// Event type
    pub event_type: RiskEventType,
    /// Event timestamp
    pub timestamp: Timestamp,
    /// Account affected
    pub account_id: AccountId,
    /// Symbol affected (if applicable)
    pub symbol: Option<Symbol>,
    /// Order ID related to the risk event (if applicable)
    pub order_id: Option<OrderId>,
    /// Risk check that failed
    pub risk_check: String,
    /// Current value that triggered the event
    pub current_value: f64,
    /// Limit that was breached
    pub limit_value: f64,
    /// Event message
    pub message: String,
    /// Event severity
    pub severity: EventSeverity,
    /// Additional event data
    pub metadata: HashMap<String, String>,
}

impl Event for RiskEvent {
    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
    
    fn event_type(&self) -> &'static str {
        match self.event_type {
            RiskEventType::PositionLimitBreached => "position_limit_breached",
            RiskEventType::OrderSizeLimitExceeded => "order_size_limit_exceeded",
            RiskEventType::DailyLossLimitApproached => "daily_loss_limit_approached",
            RiskEventType::DailyLossLimitBreached => "daily_loss_limit_breached",
            RiskEventType::ConcentrationLimitBreached => "concentration_limit_breached",
            RiskEventType::LeverageLimitExceeded => "leverage_limit_exceeded",
            RiskEventType::FatFingerTriggered => "fat_finger_triggered",
            RiskEventType::OrderRateLimitExceeded => "order_rate_limit_exceeded",
            RiskEventType::MarginCallIssued => "margin_call_issued",
            RiskEventType::AccountSuspended => "account_suspended",
            RiskEventType::RiskOverrideApplied => "risk_override_applied",
        }
    }
    
    fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::order::OrderStatus;
    
    #[test]
    fn test_order_event_creation() {
        let event = OrderEvent::order_created(
            uuid::Uuid::new_v4(),
            "TEST_001".to_string(),
            "TEST_ACCOUNT".to_string(),
            "AAPL".to_string(),
            1,
        );
        
        assert_eq!(event.event_type, OrderEventType::OrderCreated);
        assert_eq!(event.new_status, OrderStatus::New);
        assert!(!event.is_terminal());
        assert!(!event.is_fill());
        assert!(!event.is_error());
    }
    
    #[test]
    fn test_order_fill_event() {
        let fill_details = FillDetails {
            execution_id: uuid::Uuid::new_v4(),
            price: 150.0,
            quantity: 100.0,
            cumulative_quantity: 100.0,
            average_price: 150.0,
            commission: Some(1.0),
            commission_currency: Some("USD".to_string()),
            contra_party: None,
            trade_id: Some("T123".to_string()),
            settlement_date: None,
        };
        
        let event = OrderEvent::order_filled(
            uuid::Uuid::new_v4(),
            "TEST_001".to_string(),
            "TEST_ACCOUNT".to_string(),
            "AAPL".to_string(),
            OrderStatus::Active,
            OrderStatus::Filled,
            fill_details,
            Some("NASDAQ".to_string()),
            2,
        );
        
        assert_eq!(event.event_type, OrderEventType::OrderCompletelyFilled);
        assert_eq!(event.new_status, OrderStatus::Filled);
        assert!(event.is_terminal());
        assert!(event.is_fill());
        assert!(!event.is_error());
        assert!(event.fill_details.is_some());
    }
    
    #[test]
    fn test_system_event_creation() {
        let event = SystemEvent::new(
            SystemEventType::SystemStarted,
            "main".to_string(),
            "System starting up".to_string(),
            EventSeverity::Info,
        ).with_metadata("version".to_string(), "1.0.0".to_string());
        
        assert_eq!(event.event_type, SystemEventType::SystemStarted);
        assert_eq!(event.severity, EventSeverity::Info);
        assert!(event.metadata.contains_key("version"));
    }
    
    #[test]
    fn test_event_severity_ordering() {
        assert!(EventSeverity::Debug < EventSeverity::Info);
        assert!(EventSeverity::Info < EventSeverity::Warning);
        assert!(EventSeverity::Warning < EventSeverity::Error);
        assert!(EventSeverity::Error < EventSeverity::Critical);
    }
}
