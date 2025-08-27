// src/main.rs - High-Performance Order Management System Entry Point
#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

use anyhow::{Context, Result};
use axum::{
    extract::{Path, Query, State, WebSocketUpgrade},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use color_eyre::eyre::WrapErr;
use config::{Config, ConfigError, Environment, File};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    signal,
    sync::{broadcast, mpsc, oneshot},
    time::{sleep, timeout, Instant},
};
use tower::{ServiceBuilder, ServiceExt};
use tower_http::{
    cors::CorsLayer,
    compression::CompressionLayer,
    trace::TraceLayer,
};
use tracing::{debug, error, info, instrument, span, warn, Instrument, Level};
use uuid::Uuid;

mod fix_handler;

// Use our FIX handler
use fix_handler::{FixHandler, FixMessage};

// Global allocator for better performance
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// Application configuration
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub trading: TradingConfig,
    pub fix: FixConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    pub request_timeout: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TradingConfig {
    pub max_order_size: f64,
    pub max_orders_per_second: u32,
    pub enable_risk_checks: bool,
    pub default_venue: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FixConfig {
    pub enabled: bool,
    pub port: u16,
    pub sender_comp_id: String,
    pub target_comp_id: String,
    pub heartbeat_interval: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String, // "json" or "pretty"
    pub file_path: Option<String>,
}

// Order domain models
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    PendingNew,
    PendingCancel,
    PendingReplace,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeInForce {
    Day,
    GoodTilCanceled,
    ImmediateOrCancel,
    FillOrKill,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: Uuid,
    pub client_order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: f64,
    pub price: Option<f64>,
    pub stop_price: Option<f64>,
    pub time_in_force: TimeInForce,
    pub status: OrderStatus,
    pub filled_quantity: f64,
    pub avg_price: Option<f64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub venue: Option<String>,
    pub account: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub client_order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: f64,
    pub price: Option<f64>,
    pub stop_price: Option<f64>,
    pub time_in_force: TimeInForce,
    pub account: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResponse {
    pub order: Order,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderUpdate {
    pub order_id: Uuid,
    pub status: OrderStatus,
    pub filled_quantity: f64,
    pub avg_price: Option<f64>,
    pub message: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketData {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub last: Option<f64>,
    pub volume: f64,
    pub timestamp: DateTime<Utc>,
}

// Application state
#[derive(Clone)]
pub struct AppState {
    pub orders: Arc<DashMap<Uuid, Order>>,
    pub order_counter: Arc<AtomicU64>,
    pub config: Arc<AppConfig>,
    pub fix_handler: Arc<RwLock<Option<FixHandler>>>,
    pub order_updates_tx: broadcast::Sender<OrderUpdate>,
    pub market_data: Arc<DashMap<String, MarketData>>,
    pub metrics: Arc<Metrics>,
}

#[derive(Debug)]
pub struct Metrics {
    pub orders_created: AtomicU64,
    pub orders_filled: AtomicU64,
    pub orders_canceled: AtomicU64,
    pub orders_rejected: AtomicU64,
    pub total_volume: parking_lot::RwLock<f64>,
    pub uptime_start: Instant,
}

impl AppState {
    pub fn new(config: AppConfig) -> Self {
        let (order_updates_tx, _) = broadcast::channel(10000);
        
        Self {
            orders: Arc::new(DashMap::new()),
            order_counter: Arc::new(AtomicU64::new(1)),
            config: Arc::new(config),
            fix_handler: Arc::new(RwLock::new(None)),
            order_updates_tx,
            market_data: Arc::new(DashMap::new()),
            metrics: Arc::new(Metrics {
                orders_created: AtomicU64::new(0),
                orders_filled: AtomicU64::new(0),
                orders_canceled: AtomicU64::new(0),
                orders_rejected: AtomicU64::new(0),
                total_volume: parking_lot::RwLock::new(0.0),
                uptime_start: Instant::now(),
            }),
        }
    }
}

// REST API handlers
#[instrument(skip(state))]
async fn create_order(
    State(state): State<AppState>,
    Json(request): Json<OrderRequest>,
) -> Result<Json<OrderResponse>, (StatusCode, String)> {
    info!("Creating new order: {}", request.client_order_id);
    
    // Basic validation
    if request.quantity <= 0.0 {
        warn!("Invalid quantity: {}", request.quantity);
        return Err((StatusCode::BAD_REQUEST, "Quantity must be positive".into()));
    }
    
    if request.quantity > state.config.trading.max_order_size {
        warn!("Order size {} exceeds maximum {}", request.quantity, state.config.trading.max_order_size);
        return Err((StatusCode::BAD_REQUEST, "Order size exceeds maximum".into()));
    }
    
    // Generate order ID
    let order_id = Uuid::new_v4();
    let now = Utc::now();
    
    let order = Order {
        id: order_id,
        client_order_id: request.client_order_id,
        symbol: request.symbol,
        side: request.side,
        order_type: request.order_type,
        quantity: request.quantity,
        price: request.price,
        stop_price: request.stop_price,
        time_in_force: request.time_in_force,
        status: OrderStatus::New,
        filled_quantity: 0.0,
        avg_price: None,
        created_at: now,
        updated_at: now,
        venue: Some(state.config.trading.default_venue.clone()),
        account: request.account,
    };
    
    // Store order
    state.orders.insert(order_id, order.clone());
    state.metrics.orders_created.fetch_add(1, Ordering::Relaxed);
    
    // Send to FIX handler if enabled
    if state.config.fix.enabled {
        if let Some(fix_handler) = state.fix_handler.read().as_ref() {
            let fix_message = FixMessage {
                msg_type: "D".to_string(), // New Order Single
                order_id,
                symbol: order.symbol.clone(),
                side: match order.side {
                    OrderSide::Buy => "1".to_string(),
                    OrderSide::Sell => "2".to_string(),
                },
                quantity: order.quantity,
                price: order.price,
                order_type: match order.order_type {
                    OrderType::Market => "1".to_string(),
                    OrderType::Limit => "2".to_string(),
                    OrderType::Stop => "3".to_string(),
                    OrderType::StopLimit => "4".to_string(),
                },
            };
            
            if let Err(e) = fix_handler.send_message(fix_message).await {
                error!("Failed to send FIX message: {}", e);
            }
        }
    }
    
    // Simulate order processing (replace with real routing logic)
    let state_clone = state.clone();
    let order_clone = order.clone();
    tokio::spawn(async move {
        simulate_order_processing(state_clone, order_clone).await;
    });
    
    info!("Order created successfully: {} ({})", order_id, order.client_order_id);
    
    Ok(Json(OrderResponse {
        order,
        message: "Order created successfully".into(),
    }))
}

#[instrument(skip(state))]
async fn get_order(
    State(state): State<AppState>,
    Path(order_id): Path<Uuid>,
) -> Result<Json<Order>, (StatusCode, String)> {
    match state.orders.get(&order_id) {
        Some(order) => Ok(Json(order.clone())),
        None => Err((StatusCode::NOT_FOUND, "Order not found".into())),
    }
}

#[instrument(skip(state))]
async fn cancel_order(
    State(state): State<AppState>,
    Path(order_id): Path<Uuid>,
) -> Result<Json<OrderResponse>, (StatusCode, String)> {
    info!("Canceling order: {}", order_id);
    
    match state.orders.get_mut(&order_id) {
        Some(mut order_ref) => {
            let order = order_ref.value_mut();
            
            if matches!(order.status, OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected) {
                return Err((StatusCode::BAD_REQUEST, "Cannot cancel order in current state".into()));
            }
            
            order.status = OrderStatus::Canceled;
            order.updated_at = Utc::now();
            
            state.metrics.orders_canceled.fetch_add(1, Ordering::Relaxed);
            
            // Send order update
            let update = OrderUpdate {
                order_id: order.id,
                status: order.status.clone(),
                filled_quantity: order.filled_quantity,
                avg_price: order.avg_price,
                message: Some("Order canceled".into()),
                updated_at: order.updated_at,
            };
            
            let _ = state.order_updates_tx.send(update);
            
            Ok(Json(OrderResponse {
                order: order.clone(),
                message: "Order canceled successfully".into(),
            }))
        }
        None => Err((StatusCode::NOT_FOUND, "Order not found".into())),
    }
}

#[instrument(skip(state))]
async fn list_orders(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<Vec<Order>> {
    let status_filter = params.get("status");
    let symbol_filter = params.get("symbol");
    let account_filter = params.get("account");
    
    let orders: Vec<Order> = state
        .orders
        .iter()
        .filter_map(|entry| {
            let order = entry.value();
            
            // Apply filters
            if let Some(status) = status_filter {
                let matches = match status.as_str() {
                    "new" => matches!(order.status, OrderStatus::New),
                    "filled" => matches!(order.status, OrderStatus::Filled),
                    "canceled" => matches!(order.status, OrderStatus::Canceled),
                    "rejected" => matches!(order.status, OrderStatus::Rejected),
                    "partial" => matches!(order.status, OrderStatus::PartiallyFilled),
                    _ => true,
                };
                if !matches { return None; }
            }
            
            if let Some(symbol) = symbol_filter {
                if order.symbol != *symbol { return None; }
            }
            
            if let Some(account) = account_filter {
                if order.account != *account { return None; }
            }
            
            Some(order.clone())
        })
        .collect();
    
    Json(orders)
}

#[instrument(skip(state))]
async fn get_metrics(State(state): State<AppState>) -> Json<serde_json::Value> {
    let uptime = state.metrics.uptime_start.elapsed().as_secs();
    let total_orders = state.orders.len();
    
    Json(serde_json::json!({
        "uptime_seconds": uptime,
        "total_orders": total_orders,
        "orders_created": state.metrics.orders_created.load(Ordering::Relaxed),
        "orders_filled": state.metrics.orders_filled.load(Ordering::Relaxed),
        "orders_canceled": state.metrics.orders_canceled.load(Ordering::Relaxed),
        "orders_rejected": state.metrics.orders_rejected.load(Ordering::Relaxed),
        "total_volume": *state.metrics.total_volume.read(),
        "active_symbols": state.market_data.len(),
    }))
}

async fn health_check() -> &'static str {
    "OK"
}

async fn websocket_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(|socket| handle_websocket(socket, state))
}

async fn handle_websocket(
    socket: axum::extract::ws::WebSocket, 
    state: AppState
) {
    use axum::extract::ws::{Message, WebSocket};
    use futures_util::{SinkExt, StreamExt};
    
    let (mut sender, mut receiver) = socket.split();
    let mut order_updates_rx = state.order_updates_tx.subscribe();
    
    // Send order updates to client
    let send_task = tokio::spawn(async move {
        while let Ok(update) = order_updates_rx.recv().await {
            let message = serde_json::to_string(&update).unwrap_or_default();
            if sender.send(Message::Text(message)).await.is_err() {
                break;
            }
        }
    });
    
    // Handle incoming messages from client
    let receive_task = tokio::spawn(async move {
        while let Some(Ok(message)) = receiver.next().await {
            if let Message::Text(text) = message {
                debug!("Received WebSocket message: {}", text);
                // Handle client messages (ping, subscription changes, etc.)
            }
        }
    });
    
    // Wait for either task to finish
    tokio::select! {
        _ = send_task => {},
        _ = receive_task => {},
    }
}

// Simulate order processing (replace with real execution logic)
#[instrument(skip(state))]
async fn simulate_order_processing(state: AppState, mut order: Order) {
    // Simulate processing delay
    sleep(Duration::from_millis(100)).await;
    
    // Update order status to partially filled
    order.status = OrderStatus::PartiallyFilled;
    order.filled_quantity = order.quantity * 0.5;
    order.avg_price = order.price;
    order.updated_at = Utc::now();
    
    state.orders.insert(order.id, order.clone());
    
    let update = OrderUpdate {
        order_id: order.id,
        status: order.status.clone(),
        filled_quantity: order.filled_quantity,
        avg_price: order.avg_price,
        message: Some("Partial fill".into()),
        updated_at: order.updated_at,
    };
    
    let _ = state.order_updates_tx.send(update);
    
    // Simulate another fill
    sleep(Duration::from_millis(200)).await;
    
    order.status = OrderStatus::Filled;
    order.filled_quantity = order.quantity;
    order.updated_at = Utc::now();
    
    state.orders.insert(order.id, order.clone());
    state.metrics.orders_filled.fetch_add(1, Ordering::Relaxed);
    
    // Update total volume
    {
        let mut total_volume = state.metrics.total_volume.write();
        *total_volume += order.quantity * order.avg_price.unwrap_or(0.0);
    }
    
    let update = OrderUpdate {
        order_id: order.id,
        status: order.status,
        filled_quantity: order.filled_quantity,
        avg_price: order.avg_price,
        message: Some("Order filled".into()),
        updated_at: order.updated_at,
    };
    
    let _ = state.order_updates_tx.send(update);
    info!("Order {} fully executed", order.id);
}

// Configuration loading
fn load_config() -> Result<AppConfig, ConfigError> {
    let config_dir = env::var("CONFIG_DIR").unwrap_or_else(|_| "config".to_string());
    
    let s = Config::builder()
        .add_source(File::with_name(&format!("{}/default", config_dir)).required(false))
        .add_source(File::with_name(&format!("{}/local", config_dir)).required(false))
        .add_source(Environment::with_prefix("OMS").separator("_"))
        .build()?;
    
    s.try_deserialize()
}

// Default configuration
impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 8080,
                max_connections: 10000,
                request_timeout: 30,
            },
            trading: TradingConfig {
                max_order_size: 1_000_000.0,
                max_orders_per_second: 1000,
                enable_risk_checks: true,
                default_venue: "EXCHANGE".to_string(),
            },
            fix: FixConfig {
                enabled: false,
                port: 9878,
                sender_comp_id: "TRADING_OMS".to_string(),
                target_comp_id: "EXCHANGE".to_string(),
                heartbeat_interval: 30,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                format: "json".to_string(),
                file_path: None,
            },
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Install color_eyre for better error messages
    color_eyre::install().wrap_err("Failed to install color_eyre")?;
    
    // Load configuration
    let config = load_config().unwrap_or_else(|_| {
        warn!("Failed to load config, using defaults");
        AppConfig::default()
    });
    
    // Initialize tracing
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(
            config.logging.level
                .parse::<Level>()
                .unwrap_or(Level::INFO)
        )
        .with_target(false)
        .with_thread_ids(true);
    
    if config.logging.format == "json" {
        subscriber.json().init();
    } else {
        subscriber.pretty().init();
    }
    
    info!("Starting Trading OMS v{}", env!("CARGO_PKG_VERSION"));
    info!("Configuration loaded: {:?}", config);
    
    // Create application state
    let state = AppState::new(config.clone());
    
    // Initialize FIX handler if enabled
    if config.fix.enabled {
        info!("Initializing FIX protocol handler");
        match FixHandler::new(
            config.fix.sender_comp_id.clone(),
            config.fix.target_comp_id.clone(),
            config.fix.port,
        ).await {
            Ok(fix_handler) => {
                *state.fix_handler.write() = Some(fix_handler);
                info!("FIX handler initialized successfully");
            }
            Err(e) => {
                error!("Failed to initialize FIX handler: {}", e);
                return Err(e.into());
            }
        }
    }
    
    // Build the router
    let app = Router::new()
        // Order management endpoints
        .route("/orders", post(create_order))
        .route("/orders", get(list_orders))
        .route("/orders/:order_id", get(get_order))
        .route("/orders/:order_id/cancel", post(cancel_order))
        
        // System endpoints
        .route("/health", get(health_check))
        .route("/metrics", get(get_metrics))
        .route("/ws", get(websocket_handler))
        
        // Add state and middleware
        .with_state(state)
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CompressionLayer::new())
                .layer(CorsLayer::permissive())
                .timeout(Duration::from_secs(config.server.request_timeout))
        );
    
    // Start server
    let addr = SocketAddr::from(([127, 0, 0, 1], config.server.port));
    let listener = TcpListener::bind(addr)
        .await
        .context("Failed to bind to address")?;
    
    info!("Server listening on {}", addr);
    
    // Graceful shutdown
    let shutdown_signal = async {
        let _ = signal::ctrl_c().await;
        info!("Shutdown signal received, shutting down gracefully...");
    };
    
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await
        .context("Server error")?;
    
    info!("Server shutdown complete");
    Ok(())
}
