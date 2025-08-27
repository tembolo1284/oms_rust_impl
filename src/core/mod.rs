// src/core/mod.rs - Core Module Declaration
//! Core business domain models and types
//!
//! This module contains the fundamental types and business logic for the trading system.
//! It includes order management, type definitions, and event handling.

pub mod events;
pub mod order;
pub mod types;

// Re-export commonly used types for convenience
pub use events::{OrderEvent, OrderEventType};
pub use order::{
    Order, OrderBuilder, OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType,
    OrderUpdate, OrderUpdateType, TimeInForce,
};
pub use types::{
    Account, AccountId, AccountStatus, AccountType, ClientOrderId, Connection, ConnectionStatus,
    ConnectionType, Currency, Execution, ExecutionId, MarketTick, Money, OrderId, Position, Price,
    Quantity, RiskLimits, Session, SessionStatus, Symbol, Timestamp, Trade, TradeId, Venue,
};
