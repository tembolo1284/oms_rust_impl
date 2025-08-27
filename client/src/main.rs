// client/src/main.rs - Trading OMS Test Client
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use color_eyre::eyre::WrapErr;
use console::{style, Term};
use fake::{Fake, Faker};
use futures_util::{SinkExt, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use rand::{thread_rng, Rng};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tabled::{Table, Tabled};
use tokio::{
    select,
    sync::{broadcast, mpsc},
    time::{interval, sleep, timeout},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

// Order types - mirrored from the main application
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

#[derive(Debug, Tabled)]
pub struct OrderDisplay {
    #[tabled(rename = "ID")]
    pub id: String,
    #[tabled(rename = "Client ID")]
    pub client_order_id: String,
    #[tabled(rename = "Symbol")]
    pub symbol: String,
    #[tabled(rename = "Side")]
    pub side: String,
    #[tabled(rename = "Type")]
    pub order_type: String,
    #[tabled(rename = "Quantity")]
    pub quantity: f64,
    #[tabled(rename = "Price")]
    pub price: String,
    #[tabled(rename = "Status")]
    pub status: String,
    #[tabled(rename = "Filled")]
    pub filled_quantity: f64,
    #[tabled(rename = "Created")]
    pub created_at: String,
}

impl From<Order> for OrderDisplay {
    fn from(order: Order) -> Self {
        Self {
            id: order.id.to_string()[..8].to_string(),
            client_order_id: order.client_order_id,
            symbol: order.symbol,
            side: format!("{:?}", order.side),
            order_type: format!("{:?}", order.order_type),
            quantity: order.quantity,
            price: order.price.map_or("N/A".to_string(), |p| format!("{:.2}", p)),
            status: format!("{:?}", order.status),
            filled_quantity: order.filled_quantity,
            created_at: order.created_at.format("%H:%M:%S").to_string(),
        }
    }
}

#[derive(Parser)]
#[command(name = "trading-oms-client")]
#[command(about = "Test client for Trading OMS")]
#[command(version)]
pub struct Args {
    /// OMS server URL
    #[arg(short, long, default_value = "http://localhost:8080")]
    pub url: String,
    
    /// Enable verbose logging
    #[arg(short, long)]
    pub verbose: bool,
    
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Create a single order
    Order {
        /// Symbol to trade
        #[arg(short, long, default_value = "AAPL")]
        symbol: String,
        
        /// Order side (buy/sell)
        #[arg(short = 's', long, default_value = "buy")]
        side: String,
        
        /// Order quantity
        #[arg(short, long, default_value = "100")]
        quantity: f64,
        
        /// Order price (optional for market orders)
        #[arg(short, long)]
        price: Option<f64>,
        
        /// Order type (market/limit)
        #[arg(short = 't', long, default_value = "market")]
        order_type: String,
        
        /// Account name
        #[arg(short, long, default_value = "TEST_ACCOUNT")]
        account: String,
    },
    
    /// List orders
    List {
        /// Filter by status
        #[arg(long)]
        status: Option<String>,
        
        /// Filter by symbol
        #[arg(long)]
        symbol: Option<String>,
        
        /// Filter by account
        #[arg(long)]
        account: Option<String>,
    },
    
    /// Get order details
    Get {
        /// Order ID
        order_id: String,
    },
    
    /// Cancel an order
    Cancel {
        /// Order ID to cancel
        order_id: String,
    },
    
    /// Run load test
    LoadTest {
        /// Number of concurrent clients
        #[arg(short, long, default_value = "10")]
        clients: usize,
        
        /// Orders per client
        #[arg(short = 'o', long, default_value = "100")]
        orders_per_client: usize,
        
        /// Delay between orders (ms)
        #[arg(short, long, default_value = "100")]
        delay_ms: u64,
        
        /// Use random symbols
        #[arg(long)]
        random_symbols: bool,
    },
    
    /// Stream order updates via WebSocket
    Stream {
        /// Duration to stream (seconds)
        #[arg(short, long, default_value = "60")]
        duration: u64,
        
        /// Show all updates or just summaries
        #[arg(long)]
        detailed: bool,
    },
    
    /// Get system metrics
    Metrics,
    
    /// Run interactive mode
    Interactive,
}

#[derive(Clone)]
pub struct OmsClient {
    client: Client,
    base_url: String,
    term: Arc<Term>,
}

impl OmsClient {
    pub fn new(base_url: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");
            
        Self {
            client,
            base_url,
            term: Arc::new(Term::stdout()),
        }
    }
    
    #[instrument(skip(self))]
    pub async fn create_order(&self, request: OrderRequest) -> Result<OrderResponse> {
        debug!("Creating order: {}", request.client_order_id);
        
        let response = self
            .client
            .post(&format!("{}/orders", self.base_url))
            .json(&request)
            .send()
            .await
            .context("Failed to send order request")?;
            
        self.handle_response::<OrderResponse>(response).await
    }
    
    #[instrument(skip(self))]
    pub async fn get_order(&self, order_id: Uuid) -> Result<Order> {
        debug!("Getting order: {}", order_id);
        
        let response = self
            .client
            .get(&format!("{}/orders/{}", self.base_url, order_id))
            .send()
            .await
            .context("Failed to send get order request")?;
            
        self.handle_response::<Order>(response).await
    }
    
    #[instrument(skip(self))]
    pub async fn cancel_order(&self, order_id: Uuid) -> Result<OrderResponse> {
        debug!("Canceling order: {}", order_id);
        
        let response = self
            .client
            .post(&format!("{}/orders/{}/cancel", self.base_url, order_id))
            .send()
            .await
            .context("Failed to send cancel request")?;
            
        self.handle_response::<OrderResponse>(response).await
    }
    
    #[instrument(skip(self))]
    pub async fn list_orders(&self, filters: HashMap<String, String>) -> Result<Vec<Order>> {
        debug!("Listing orders with filters: {:?}", filters);
        
        let mut url = format!("{}/orders", self.base_url);
        if !filters.is_empty() {
            let query: Vec<String> = filters
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            url = format!("{}?{}", url, query.join("&"));
        }
        
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to send list orders request")?;
            
        self.handle_response::<Vec<Order>>(response).await
    }
    
    #[instrument(skip(self))]
    pub async fn get_metrics(&self) -> Result<serde_json::Value> {
        debug!("Getting system metrics");
        
        let response = self
            .client
            .get(&format!("{}/metrics", self.base_url))
            .send()
            .await
            .context("Failed to get metrics")?;
            
        self.handle_response::<serde_json::Value>(response).await
    }
    
    async fn handle_response<T>(&self, response: Response) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let status = response.status();
        let text = response.text().await.context("Failed to read response body")?;
        
        if status.is_success() {
            serde_json::from_str(&text).context("Failed to deserialize response")
        } else {
            Err(anyhow::anyhow!("HTTP error {}: {}", status, text))
        }
    }
    
    pub fn print_order_table(&self, orders: &[Order]) -> Result<()> {
        if orders.is_empty() {
            self.term.write_line("No orders found.")?;
            return Ok(());
        }
        
        let display_orders: Vec<OrderDisplay> = orders.iter().cloned().map(Into::into).collect();
        let table = Table::new(display_orders).to_string();
        self.term.write_line(&table)?;
        Ok(())
    }
    
    pub fn print_metrics(&self, metrics: &serde_json::Value) -> Result<()> {
        self.term.write_line(&style("=== System Metrics ===").bold().to_string())?;
        
        if let Some(uptime) = metrics.get("uptime_seconds").and_then(|v| v.as_u64()) {
            self.term.write_line(&format!("Uptime: {} seconds", uptime))?;
        }
        
        if let Some(total) = metrics.get("total_orders").and_then(|v| v.as_u64()) {
            self.term.write_line(&format!("Total Orders: {}", total))?;
        }
        
        if let Some(created) = metrics.get("orders_created").and_then(|v| v.as_u64()) {
            self.term.write_line(&format!("Orders Created: {}", created))?;
        }
        
        if let Some(filled) = metrics.get("orders_filled").and_then(|v| v.as_u64()) {
            self.term.write_line(&format!("Orders Filled: {}", filled))?;
        }
        
        if let Some(canceled) = metrics.get("orders_canceled").and_then(|v| v.as_u64()) {
            self.term.write_line(&format!("Orders Canceled: {}", canceled))?;
        }
        
        if let Some(volume) = metrics.get("total_volume").and_then(|v| v.as_f64()) {
            self.term.write_line(&format!("Total Volume: {:.2}", volume))?;
        }
        
        Ok(())
    }
}

pub struct LoadTestStats {
    pub total_orders: AtomicU64,
    pub successful_orders: AtomicU64,
    pub failed_orders: AtomicU64,
    pub start_time: Instant,
}

impl LoadTestStats {
    pub fn new() -> Self {
        Self {
            total_orders: AtomicU64::new(0),
            successful_orders: AtomicU64::new(0),
            failed_orders: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }
    
    pub fn record_success(&self) {
        self.successful_orders.fetch_add(1, Ordering::Relaxed);
        self.total_orders.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_failure(&self) {
        self.failed_orders.fetch_add(1, Ordering::Relaxed);
        self.total_orders.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn print_summary(&self, term: &Term) -> Result<()> {
        let duration = self.start_time.elapsed();
        let total = self.total_orders.load(Ordering::Relaxed);
        let successful = self.successful_orders.load(Ordering::Relaxed);
        let failed = self.failed_orders.load(Ordering::Relaxed);
        
        term.write_line(&style("\n=== Load Test Results ===").bold().to_string())?;
        term.write_line(&format!("Duration: {:.2}s", duration.as_secs_f64()))?;
        term.write_line(&format!("Total Orders: {}", total))?;
        term.write_line(&format!("Successful: {} ({:.1}%)", successful, (successful as f64 / total as f64) * 100.0))?;
        term.write_line(&format!("Failed: {} ({:.1}%)", failed, (failed as f64 / total as f64) * 100.0))?;
        
        if duration.as_secs() > 0 {
            let ops_per_sec = total as f64 / duration.as_secs_f64();
            term.write_line(&format!("Orders/sec: {:.1}", ops_per_sec))?;
        }
        
        Ok(())
    }
}

fn generate_random_order(account: &str) -> OrderRequest {
    let mut rng = thread_rng();
    
    let symbols = vec!["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "META", "NFLX", "NVDA"];
    let symbol = symbols[rng.gen_range(0..symbols.len())].to_string();
    
    let side = if rng.gen_bool(0.5) { OrderSide::Buy } else { OrderSide::Sell };
    let quantity = rng.gen_range(1.0..1000.0);
    let price = if rng.gen_bool(0.8) { Some(rng.gen_range(50.0..500.0)) } else { None };
    let order_type = if price.is_some() { OrderType::Limit } else { OrderType::Market };
    
    OrderRequest {
        client_order_id: format!("TEST_{}", Uuid::new_v4()),
        symbol,
        side,
        order_type,
        quantity,
        price,
        stop_price: None,
        time_in_force: TimeInForce::Day,
        account: account.to_string(),
    }
}

async fn run_load_test(
    client: Arc<OmsClient>,
    clients: usize,
    orders_per_client: usize,
    delay_ms: u64,
    random_symbols: bool,
) -> Result<()> {
    let stats = Arc::new(LoadTestStats::new());
    let term = Term::stdout();
    
    term.write_line(&style("Starting load test...").bold().to_string())?;
    term.write_line(&format!("Clients: {}, Orders per client: {}, Delay: {}ms", clients, orders_per_client, delay_ms))?;
    
    let total_orders = clients * orders_per_client;
    let progress = ProgressBar::new(total_orders as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
    );
    
    let mut handles = Vec::new();
    
    // Spawn client tasks
    for client_id in 0..clients {
        let client_clone = client.clone();
        let stats_clone = stats.clone();
        let progress_clone = progress.clone();
        
        let handle = tokio::spawn(async move {
            for order_idx in 0..orders_per_client {
                let account = format!("LOADTEST_CLIENT_{}", client_id);
                let order = if random_symbols {
                    generate_random_order(&account)
                } else {
                    OrderRequest {
                        client_order_id: format!("LOAD_{}_{}", client_id, order_idx),
                        symbol: "AAPL".to_string(),
                        side: if order_idx % 2 == 0 { OrderSide::Buy } else { OrderSide::Sell },
                        order_type: OrderType::Limit,
                        quantity: 100.0,
                        price: Some(150.0 + (order_idx as f64)),
                        stop_price: None,
                        time_in_force: TimeInForce::Day,
                        account,
                    }
                };
                
                match client_clone.create_order(order).await {
                    Ok(_) => stats_clone.record_success(),
                    Err(e) => {
                        debug!("Order failed: {}", e);
                        stats_clone.record_failure();
                    }
                }
                
                progress_clone.inc(1);
                
                if delay_ms > 0 {
                    sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all clients to complete
    for handle in handles {
        handle.await.context("Client task failed")?;
    }
    
    progress.finish_with_message("Load test completed");
    stats.print_summary(&term)?;
    
    Ok(())
}

async fn stream_order_updates(base_url: &str, duration: u64, detailed: bool) -> Result<()> {
    let ws_url = base_url.replace("http", "ws") + "/ws";
    let term = Term::stdout();
    
    term.write_line(&format!("Connecting to WebSocket at {}", ws_url))?;
    
    let (ws_stream, _) = connect_async(&ws_url)
        .await
        .context("Failed to connect to WebSocket")?;
    
    let (mut _ws_sender, mut ws_receiver) = ws_stream.split();
    
    term.write_line("Connected! Streaming order updates...")?;
    
    let mut update_count = 0u64;
    let timeout_duration = Duration::from_secs(duration);
    
    loop {
        match timeout(timeout_duration, ws_receiver.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                update_count += 1;
                
                if detailed {
                    if let Ok(update) = serde_json::from_str::<OrderUpdate>(&text) {
                        term.write_line(&format!(
                            "[{}] Order {} -> {:?} (Filled: {:.2})",
                            update.updated_at.format("%H:%M:%S"),
                            &update.order_id.to_string()[..8],
                            update.status,
                            update.filled_quantity
                        ))?;
                    } else {
                        term.write_line(&format!("Raw message: {}", text))?;
                    }
                } else if update_count % 10 == 0 {
                    term.write_line(&format!("Received {} updates...", update_count))?;
                }
            }
            Ok(Some(Ok(Message::Close(_)))) => {
                term.write_line("WebSocket connection closed by server")?;
                break;
            }
            Ok(Some(Err(e))) => {
                error!("WebSocket error: {}", e);
                break;
            }
            Ok(None) => {
                term.write_line("WebSocket stream ended")?;
                break;
            }
            Err(_) => {
                term.write_line("Streaming timeout reached")?;
                break;
            }
        }
    }
    
    term.write_line(&format!("Total updates received: {}", update_count))?;
    Ok(())
}

async fn run_interactive_mode(client: Arc<OmsClient>) -> Result<()> {
    let term = Term::stdout();
    
    term.write_line(&style("=== Interactive Trading OMS Client ===").bold().to_string())?;
    term.write_line("Available commands:")?;
    term.write_line("  order <symbol> <side> <qty> [price] - Create order")?;
    term.write_line("  list [status] - List orders")?;
    term.write_line("  cancel <order_id> - Cancel order")?;
    term.write_line("  metrics - Show system metrics")?;
    term.write_line("  help - Show this help")?;
    term.write_line("  quit - Exit interactive mode")?;
    term.write_line("")?;
    
    loop {
        term.write_str("oms> ")?;
        let line = term.read_line()?.trim().to_string();
        
        if line.is_empty() {
            continue;
        }
        
        let parts: Vec<&str> = line.split_whitespace().collect();
        
        match parts.first() {
            Some(&"quit") | Some(&"exit") => {
                term.write_line("Goodbye!")?;
                break;
            }
            Some(&"help") => {
                term.write_line("Available commands:")?;
                term.write_line("  order <symbol> <side> <qty> [price] - Create order")?;
                term.write_line("  list [status] - List orders")?;
                term.write_line("  cancel <order_id> - Cancel order")?;
                term.write_line("  metrics - Show system metrics")?;
                term.write_line("  help - Show this help")?;
                term.write_line("  quit - Exit interactive mode")?;
            }
            Some(&"order") => {
                if parts.len() < 4 {
                    term.write_line("Usage: order <symbol> <side> <qty> [price]")?;
                    continue;
                }
                
                let symbol = parts[1].to_string();
                let side = match parts[2].to_lowercase().as_str() {
                    "buy" | "b" => OrderSide::Buy,
                    "sell" | "s" => OrderSide::Sell,
                    _ => {
                        term.write_line("Invalid side. Use 'buy' or 'sell'")?;
                        continue;
                    }
                };
                
                let quantity: f64 = match parts[3].parse() {
                    Ok(q) => q,
                    Err(_) => {
                        term.write_line("Invalid quantity")?;
                        continue;
                    }
                };
                
                let price = if parts.len() > 4 {
                    match parts[4].parse() {
                        Ok(p) => Some(p),
                        Err(_) => {
                            term.write_line("Invalid price")?;
                            continue;
                        }
                    }
                } else {
                    None
                };
                
                let order = OrderRequest {
                    client_order_id: format!("INTERACTIVE_{}", Uuid::new_v4()),
                    symbol,
                    side,
                    order_type: if price.is_some() { OrderType::Limit } else { OrderType::Market },
                    quantity,
                    price,
                    stop_price: None,
                    time_in_force: TimeInForce::Day,
                    account: "INTERACTIVE".to_string(),
                };
                
                match client.create_order(order).await {
                    Ok(response) => {
                        term.write_line(&format!("Order created: {} ({})", response.order.id, response.message))?;
                    }
                    Err(e) => {
                        term.write_line(&format!("Error creating order: {}", e))?;
                    }
                }
            }
            Some(&"list") => {
                let mut filters = HashMap::new();
                if parts.len() > 1 {
                    filters.insert("status".to_string(), parts[1].to_string());
                }
                
                match client.list_orders(filters).await {
                    Ok(orders) => {
                        client.print_order_table(&orders)?;
                    }
                    Err(e) => {
                        term.write_line(&format!("Error listing orders: {}", e))?;
                    }
                }
            }
            Some(&"cancel") => {
                if parts.len() < 2 {
                    term.write_line("Usage: cancel <order_id>")?;
                    continue;
                }
                
                let order_id: Uuid = match parts[1].parse() {
                    Ok(id) => id,
                    Err(_) => {
                        term.write_line("Invalid order ID")?;
                        continue;
                    }
                };
                
                match client.cancel_order(order_id).await {
                    Ok(response) => {
                        term.write_line(&format!("Order canceled: {}", response.message))?;
                    }
                    Err(e) => {
                        term.write_line(&format!("Error canceling order: {}", e))?;
                    }
                }
            }
            Some(&"metrics") => {
                match client.get_metrics().await {
                    Ok(metrics) => {
                        client.print_metrics(&metrics)?;
                    }
                    Err(e) => {
                        term.write_line(&format!("Error getting metrics: {}", e))?;
                    }
                }
            }
            Some(cmd) => {
                term.write_line(&format!("Unknown command: {}. Type 'help' for available commands.", cmd))?;
            }
            None => {}
        }
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install().wrap_err("Failed to install color_eyre")?;
    
    let args = Args::parse();
    
    // Initialize tracing
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(if args.verbose { tracing::Level::DEBUG } else { tracing::Level::INFO })
        .with_target(false)
        .pretty()
        .init();
    
    let client = Arc::new(OmsClient::new(args.url.clone()));
    
    // Test connection
    match client.get_metrics().await {
        Ok(_) => info!("Successfully connected to OMS at {}", args.url),
        Err(e) => {
            error!("Failed to connect to OMS: {}", e);
            return Err(e);
        }
    }
    
    match args.command {
        Commands::Order { symbol, side, quantity, price, order_type, account } => {
            let side = match side.to_lowercase().as_str() {
                "buy" | "b" => OrderSide::Buy,
                "sell" | "s" => OrderSide::Sell,
                _ => return Err(anyhow::anyhow!("Invalid side: {}. Use 'buy' or 'sell'", side)),
            };
            
            let order_type = match order_type.to_lowercase().as_str() {
                "market" | "m" => OrderType::Market,
                "limit" | "l" => OrderType::Limit,
                _ => return Err(anyhow::anyhow!("Invalid order type: {}. Use 'market' or 'limit'", order_type)),
            };
            
            let request = OrderRequest {
                client_order_id: format!("CLI_{}", Uuid::new_v4()),
                symbol,
                side,
                order_type,
                quantity,
                price,
                stop_price: None,
                time_in_force: TimeInForce::Day,
                account,
            };
            
            let response = client.create_order(request).await?;
            println!("Order created successfully:");
            println!("  ID: {}", response.order.id);
            println!("  Status: {:?}", response.order.status);
            println!("  Message: {}", response.message);
        }
        
        Commands::List { status, symbol, account } => {
            let mut filters = HashMap::new();
            if let Some(s) = status { filters.insert("status".to_string(), s); }
            if let Some(s) = symbol { filters.insert("symbol".to_string(), s); }
            if let Some(a) = account { filters.insert("account".to_string(), a); }
            
            let orders = client.list_orders(filters).await?;
            client.print_order_table(&orders)?;
        }
        
        Commands::Get { order_id } => {
            let uuid: Uuid = order_id.parse().context("Invalid order ID")?;
            let order = client.get_order(uuid).await?;
            let display = OrderDisplay::from(order);
            let table = Table::new(vec![display]).to_string();
            println!("{}", table);
        }
        
        Commands::Cancel { order_id } => {
            let uuid: Uuid = order_id.parse().context("Invalid order ID")?;
            let response = client.cancel_order(uuid).await?;
            println!("Order canceled: {}", response.message);
        }
        
        Commands::LoadTest { clients, orders_per_client, delay_ms, random_symbols } => {
            run_load_test(client, clients, orders_per_client, delay_ms, random_symbols).await?;
        }
        
        Commands::Stream { duration, detailed } => {
            stream_order_updates(&args.url, duration, detailed).await?;
        }
        
        Commands::Metrics => {
            let metrics = client.get_metrics().await?;
            client.print_metrics(&metrics)?;
        }
        
        Commands::Interactive => {
            run_interactive_mode(client).await?;
        }
    }
    
    Ok(())
}
