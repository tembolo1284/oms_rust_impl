// src/transport/rest.rs - REST API Transport Layer
//! RESTful API endpoints for the Order Management System
//!
//! This module provides HTTP/REST endpoints for order management, market data,
//! account information, and system monitoring.

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::{Path, Query, State, WebSocketUpgrade},
    http::{header, HeaderMap, StatusCode},
    middleware,
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post},
    Extension, Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use tower::{
    limit::RateLimitLayer,
    timeout::TimeoutLayer,
    ServiceBuilder,
};
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer},
};
use tracing::{debug, error, info, instrument, warn, Level};
use uuid::Uuid;

use crate::{
    core::{
        order::{Order, OrderSide, OrderStatus, OrderType},
        types::{ClientId, OrderId, Symbol},
    },
    engine::{
        matching::{OrderBook, MatchingEngine},
        validator::{OrderValidator, ValidationResult},
        lifecycle::{LifecycleManager, LifecycleEvent},
        OrderEngine, EngineStats,
    },
    storage::{OrderStorage, StorageManager},
};

/// API configuration
#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    pub request_timeout_secs: u64,
    pub rate_limit_per_second: u64,
    pub enable_cors: bool,
    pub enable_compression: bool,
    pub enable_metrics: bool,
    pub api_key_header: String,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            max_connections: 10000,
            request_timeout_secs: 30,
            rate_limit_per_second: 100,
            enable_cors: true,
            enable_compression: true,
            enable_metrics: true,
            api_key_header: "X-API-Key".to_string(),
        }
    }
}

/// API server state shared across handlers
#[derive(Clone)]
pub struct ApiState {
    pub engine: Arc<OrderEngine>,
    pub storage: Arc<StorageManager>,
    pub validator: Arc<OrderValidator>,
    pub lifecycle: Arc<LifecycleManager>,
    pub matching_engine: Arc<MatchingEngine>,
    pub config: ApiConfig,
    pub start_time: Instant,
    pub request_counter: Arc<RwLock<u64>>,
    pub auth_keys: Arc<RwLock<HashMap<String, ClientId>>>,
}

/// Standard API response wrapper
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub request_id: String,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            timestamp: Utc::now(),
            request_id: Uuid::new_v4().to_string(),
        }
    }

    pub fn error(error: String) -> ApiResponse<()> {
        ApiResponse {
            success: false,
            data: None,
            error: Some(error),
            timestamp: Utc::now(),
            request_id: Uuid::new_v4().to_string(),
        }
    }
}

/// Order submission request
#[derive(Debug, Deserialize, Serialize)]
pub struct OrderRequest {
    pub client_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: f64,
    pub time_in_force: Option<String>,
}

/// Order modification request
#[derive(Debug, Deserialize)]
pub struct OrderModifyRequest {
    pub quantity: Option<f64>,
    pub price: Option<f64>,
}

/// Order query parameters
#[derive(Debug, Deserialize)]
pub struct OrderQueryParams {
    pub client_id: Option<String>,
    pub symbol: Option<String>,
    pub status: Option<OrderStatus>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Health check response
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub uptime_seconds: u64,
    pub version: String,
    pub timestamp: DateTime<Utc>,
}

/// Metrics response
#[derive(Debug, Serialize)]
pub struct MetricsResponse {
    pub engine_stats: EngineStats,
    pub storage_metrics: StorageMetrics,
    pub api_metrics: ApiMetrics,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Serialize, Default)]
pub struct StorageMetrics {
    pub total_orders: u64,
    pub active_orders: u64,
    pub cache_hit_rate: f64,
}

#[derive(Debug, Serialize)]
pub struct ApiMetrics {
    pub total_requests: u64,
    pub requests_per_second: f64,
    pub average_latency_ms: f64,
    pub active_connections: u64,
}

/// Create REST API router
pub fn create_router(state: ApiState) -> Router {
    Router::new()
        // Order endpoints
        .route("/api/v1/orders", post(submit_order))
        .route("/api/v1/orders", get(list_orders))
        .route("/api/v1/orders/:id", get(get_order))
        .route("/api/v1/orders/:id", patch(modify_order))
        .route("/api/v1/orders/:id", delete(cancel_order))
        
        // Market data endpoints
        .route("/api/v1/market/:symbol/book", get(get_order_book))
        .route("/api/v1/market/:symbol/trades", get(get_recent_trades))
        .route("/api/v1/market/symbols", get(list_symbols))
        
        // Account endpoints
        .route("/api/v1/accounts/:id/orders", get(get_account_orders))
        .route("/api/v1/accounts/:id/positions", get(get_account_positions))
        
        // System endpoints
        .route("/api/v1/health", get(health_check))
        .route("/api/v1/metrics", get(get_metrics))
        .route("/api/v1/config", get(get_config))
        
        // WebSocket endpoint
        .route("/api/v1/stream", get(websocket_handler))
        
        // Add middleware
        .layer(
            ServiceBuilder::new()
                // Add timeout
                .layer(TimeoutLayer::new(Duration::from_secs(
                    state.config.request_timeout_secs
                )))
                // Add rate limiting
                .layer(RateLimitLayer::new(
                    state.config.rate_limit_per_second,
                    Duration::from_secs(1),
                ))
                // Add CORS if enabled
                .layer(if state.config.enable_cors {
                    CorsLayer::new()
                        .allow_origin(Any)
                        .allow_methods(Any)
                        .allow_headers(Any)
                } else {
                    CorsLayer::new()
                })
                // Add compression if enabled
                .layer(if state.config.enable_compression {
                    CompressionLayer::new()
                } else {
                    CompressionLayer::new().no_gzip().no_deflate().no_br()
                })
                // Add tracing
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                        .on_response(DefaultOnResponse::new().level(Level::INFO))
                )
        )
        .with_state(state)
}

// ===== Order Endpoints =====

/// Submit a new order
#[instrument(skip(state, headers))]
async fn submit_order(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<OrderRequest>,
) -> Result<impl IntoResponse, AppError> {
    // Extract client ID from auth
    let client_id = extract_client_id(&state, &headers).await?;
    
    // Create order
    let order = Order::new(
        client_id,
        Symbol(request.symbol),
        request.quantity,
        request.side,
        request.order_type,
    );
    
    // Validate order
    let validation_result = state.validator.validate_order(&order).await;
    match validation_result {
        ValidationResult::Invalid { message, .. } => {
            return Err(AppError::BadRequest(message));
        }
        ValidationResult::Warning { message, .. } => {
            warn!("Order validation warning: {}", message);
        }
        _ => {}
    }
    
    // Submit to engine
    let order_id = state.engine.submit_order(order.clone()).await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    
    // Register with lifecycle manager
    state.lifecycle.register_order(&order).await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    
    // Return response
    Ok(Json(ApiResponse::success(json!({
        "order_id": order_id,
        "status": "submitted",
        "timestamp": Utc::now()
    }))))
}

/// Get order by ID
#[instrument(skip(state, headers))]
async fn get_order(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let order_id = OrderId(Uuid::parse_str(&id)
        .map_err(|_| AppError::BadRequest("Invalid order ID".to_string()))?);
    
    // Get order from storage
    let order = state.storage.get_order(&order_id).await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound("Order not found".to_string()))?;
    
    // Check authorization
    let client_id = extract_client_id(&state, &headers).await?;
    if order.client_id != client_id {
        return Err(AppError::Unauthorized("Access denied".to_string()));
    }
    
    Ok(Json(ApiResponse::success(order)))
}

/// List orders with filters
#[instrument(skip(state, headers))]
async fn list_orders(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<OrderQueryParams>,
) -> Result<impl IntoResponse, AppError> {
    let client_id = extract_client_id(&state, &headers).await?;
    
    // Get orders from storage
    let mut orders = if let Some(symbol) = params.symbol {
        state.storage.get_symbol_orders(&Symbol(symbol)).await
            .map_err(|e| AppError::Internal(e.to_string()))?
    } else {
        state.storage.get_client_orders(&client_id).await
            .map_err(|e| AppError::Internal(e.to_string()))?
    };
    
    // Apply filters
    if let Some(status) = params.status {
        orders.retain(|o| o.status == status);
    }
    
    if let Some(start) = params.start_time {
        orders.retain(|o| o.created_at >= start);
    }
    
    if let Some(end) = params.end_time {
        orders.retain(|o| o.created_at <= end);
    }
    
    // Apply pagination
    let total = orders.len();
    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(1000);
    
    orders = orders.into_iter()
        .skip(offset)
        .take(limit)
        .collect();
    
    Ok(Json(ApiResponse::success(json!({
        "orders": orders,
        "total": total,
        "offset": offset,
        "limit": limit
    }))))
}

/// Modify an existing order
#[instrument(skip(state, headers))]
async fn modify_order(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<OrderModifyRequest>,
) -> Result<impl IntoResponse, AppError> {
    let order_id = OrderId(Uuid::parse_str(&id)
        .map_err(|_| AppError::BadRequest("Invalid order ID".to_string()))?);
    
    // Get existing order
    let mut order = state.storage.get_order(&order_id).await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound("Order not found".to_string()))?;
    
    // Check authorization
    let client_id = extract_client_id(&state, &headers).await?;
    if order.client_id != client_id {
        return Err(AppError::Unauthorized("Access denied".to_string()));
    }
    
    // Check if order can be modified
    if !matches!(order.status, OrderStatus::New | OrderStatus::Active) {
        return Err(AppError::BadRequest("Order cannot be modified in current state".to_string()));
    }
    
    // Apply modifications
    if let Some(qty) = request.quantity {
        order.quantity = qty;
    }
    
    if let Some(price) = request.price {
        match &mut order.order_type {
            OrderType::Limit { price: p } => *p = price,
            OrderType::StopLimit { limit_price, .. } => *limit_price = price,
            _ => return Err(AppError::BadRequest("Cannot modify price for this order type".to_string())),
        }
    }
    
    // Update order
    order.updated_at = Utc::now();
    state.storage.update_order(order.clone()).await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    
    Ok(Json(ApiResponse::success(json!({
        "order_id": order_id,
        "status": "modified",
        "timestamp": Utc::now()
    }))))
}

/// Cancel an order
#[instrument(skip(state, headers))]
async fn cancel_order(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let order_id = OrderId(Uuid::parse_str(&id)
        .map_err(|_| AppError::BadRequest("Invalid order ID".to_string()))?);
    
    // Get existing order
    let order = state.storage.get_order(&order_id).await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound("Order not found".to_string()))?;
    
    // Check authorization
    let client_id = extract_client_id(&state, &headers).await?;
    if order.client_id != client_id {
        return Err(AppError::Unauthorized("Access denied".to_string()));
    }
    
    // Cancel order
    state.engine.cancel_order(&order_id).await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    
    // Update lifecycle
    state.lifecycle.handle_cancel(&order_id, Some("User requested".to_string())).await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    
    Ok(Json(ApiResponse::success(json!({
        "order_id": order_id,
        "status": "canceled",
        "timestamp": Utc::now()
    }))))
}

// ===== Market Data Endpoints =====

/// Get order book for a symbol
#[instrument(skip(state))]
async fn get_order_book(
    State(state): State<ApiState>,
    Path(symbol): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let symbol = Symbol(symbol);
    
    // Get order book from matching engine
    let book = state.matching_engine.get_book(&symbol)
        .ok_or_else(|| AppError::NotFound("Order book not found".to_string()))?;
    
    Ok(Json(ApiResponse::success(book)))
}

/// Get recent trades for a symbol
#[instrument(skip(state))]
async fn get_recent_trades(
    State(state): State<ApiState>,
    Path(symbol): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    // This would need to be implemented with a trade history storage
    // For now, return empty list
    Ok(Json(ApiResponse::success(json!({
        "symbol": symbol,
        "trades": []
    }))))
}

/// List available symbols
#[instrument(skip(state))]
async fn list_symbols(
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, AppError> {
    // Get all active order books
    let books = state.matching_engine.get_all_books();
    let symbols: Vec<String> = books.iter()
        .map(|b| b.symbol.0.clone())
        .collect();
    
    Ok(Json(ApiResponse::success(symbols)))
}

// ===== Account Endpoints =====

/// Get orders for an account
#[instrument(skip(state, headers))]
async fn get_account_orders(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let client_id = ClientId(account_id);
    
    // Check authorization
    let auth_client_id = extract_client_id(&state, &headers).await?;
    if client_id != auth_client_id {
        return Err(AppError::Unauthorized("Access denied".to_string()));
    }
    
    // Get orders
    let orders = state.storage.get_client_orders(&client_id).await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    
    Ok(Json(ApiResponse::success(orders)))
}

/// Get positions for an account
#[instrument(skip(state, headers))]
async fn get_account_positions(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    // This would need integration with position tracking
    // For now, return empty positions
    Ok(Json(ApiResponse::success(json!({
        "account_id": account_id,
        "positions": []
    }))))
}

// ===== System Endpoints =====

/// Health check endpoint
#[instrument(skip(state))]
async fn health_check(
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, AppError> {
    let uptime = state.start_time.elapsed().as_secs();
    
    Ok(Json(ApiResponse::success(HealthResponse {
        status: "healthy".to_string(),
        uptime_seconds: uptime,
        version: env!("CARGO_PKG_VERSION").to_string(),
        timestamp: Utc::now(),
    })))
}

/// Get system metrics
#[instrument(skip(state))]
async fn get_metrics(
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, AppError> {
    let engine_stats = state.engine.get_stats().await;
    
    let storage_metrics = state.storage.get_metrics().await
        .map(|m| StorageMetrics {
            total_orders: m.total_orders,
            active_orders: m.active_orders,
            cache_hit_rate: if m.cache_hits + m.cache_misses > 0 {
                m.cache_hits as f64 / (m.cache_hits + m.cache_misses) as f64
            } else {
                0.0
            },
        })
        .unwrap_or_default();
    
    let request_count = *state.request_counter.read().await;
    let uptime = state.start_time.elapsed().as_secs_f64();
    
    let api_metrics = ApiMetrics {
        total_requests: request_count,
        requests_per_second: if uptime > 0.0 { request_count as f64 / uptime } else { 0.0 },
        average_latency_ms: 0.0, // Would need request tracking
        active_connections: 0, // Would need connection tracking
    };
    
    Ok(Json(ApiResponse::success(MetricsResponse {
        engine_stats,
        storage_metrics,
        api_metrics,
        timestamp: Utc::now(),
    })))
}

/// Get system configuration (non-sensitive)
#[instrument(skip(state))]
async fn get_config(
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, AppError> {
    Ok(Json(ApiResponse::success(json!({
        "max_connections": state.config.max_connections,
        "request_timeout_secs": state.config.request_timeout_secs,
        "rate_limit_per_second": state.config.rate_limit_per_second,
        "enable_compression": state.config.enable_compression,
    }))))
}

// ===== WebSocket Handler =====

/// WebSocket upgrade handler for real-time updates
async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    // Extract client ID for auth
    let client_id = extract_client_id(&state, &headers).await?;
    
    Ok(ws.on_upgrade(move |socket| handle_socket(socket, state, client_id)))
}

/// Handle WebSocket connection
async fn handle_socket(
    socket: axum::extract::ws::WebSocket,
    state: ApiState,
    client_id: ClientId,
) {
    // This would implement WebSocket message handling
    // Subscribe to order updates, market data, etc.
    info!("WebSocket connected for client: {}", client_id);
    
    // For now, just close
    drop(socket);
}

// ===== Helper Functions =====

/// Extract client ID from headers
async fn extract_client_id(state: &ApiState, headers: &HeaderMap) -> Result<ClientId, AppError> {
    // Simple API key auth for demo
    let api_key = headers
        .get(&state.config.api_key_header)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| AppError::Unauthorized("Missing API key".to_string()))?;
    
    // Look up client ID from API key
    let auth_keys = state.auth_keys.read().await;
    auth_keys.get(api_key)
        .cloned()
        .ok_or_else(|| AppError::Unauthorized("Invalid API key".to_string()))
}

// ===== Error Handling =====

/// Application error type
#[derive(Debug)]
enum AppError {
    BadRequest(String),
    Unauthorized(String),
    NotFound(String),
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg),
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            AppError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };
        
        let body = Json(ApiResponse::<()>::error(error_message));
        
        (status, body).into_response()
    }
}

/// Start the REST API server
pub async fn start_server(
    config: ApiConfig,
    engine: Arc<OrderEngine>,
    storage: Arc<StorageManager>,
    validator: Arc<OrderValidator>,
    lifecycle: Arc<LifecycleManager>,
    matching_engine: Arc<MatchingEngine>,
) -> Result<()> {
    // Create state
    let state = ApiState {
        engine,
        storage,
        validator,
        lifecycle,
        matching_engine,
        config: config.clone(),
        start_time: Instant::now(),
        request_counter: Arc::new(RwLock::new(0)),
        auth_keys: Arc::new(RwLock::new(HashMap::new())),
    };
    
    // Add some test API keys
    state.auth_keys.write().await.insert(
        "test-api-key".to_string(),
        ClientId("TEST_CLIENT".to_string()),
    );
    
    // Create router
    let app = create_router(state);
    
    // Create socket address
    let addr = format!("{}:{}", config.host, config.port).parse::<SocketAddr>()?;
    
    info!("Starting REST API server on {}", addr);
    
    // Start server
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .await
        .context("Server error")?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;
    
    #[tokio::test]
    async fn test_health_endpoint() {
        // Create mock state
        let state = create_test_state().await;
        let app = create_router(state);
        
        // Send request
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        
        assert_eq!(response.status(), StatusCode::OK);
    }
    
    async fn create_test_state() -> ApiState {
        use crate::engine::EngineConfig;
        use crate::storage::StorageManagerConfig;
        
        let storage = Arc::new(
            StorageManager::new(StorageManagerConfig::default()).await.unwrap()
        );
        let engine = Arc::new(
            OrderEngine::new(EngineConfig::default(), storage.clone()).await.unwrap()
        );
        
        ApiState {
            engine: engine.clone(),
            storage: storage.clone(),
            validator: Arc::new(OrderValidator::new(Default::default())),
            lifecycle: Arc::new(LifecycleManager::new(Default::default())),
            matching_engine: Arc::new(
                MatchingEngine::new(Default::default()).unwrap().0
            ),
            config: ApiConfig::default(),
            start_time: Instant::now(),
            request_counter: Arc::new(RwLock::new(0)),
            auth_keys: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}
