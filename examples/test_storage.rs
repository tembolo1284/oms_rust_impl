// examples/test_storage.rs
use anyhow::Result;
use trading_oms::{
    core::{
        order::{Order, OrderSide, OrderType},
        types::OrderStatus,
    },
    storage::{
        StorageManager, StorageManagerConfig,
        OrderStorage,
    },
};
use tracing::{info, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("Starting OMS Storage Test");

    // Create storage manager
    let config = StorageManagerConfig {
        enable_persistence: false, // Start with memory only
        enable_caching: true,
        ..Default::default()
    };
    
    let storage = StorageManager::new(config).await?;
    
    // Create test orders
    let orders = vec![
        Order::new(
            "CLIENT_A".into(),
            "AAPL".into(),
            100.0,
            OrderSide::Buy,
            OrderType::Limit { price: 150.0 },
        ),
        Order::new(
            "CLIENT_B".into(),
            "GOOGL".into(),
            50.0,
            OrderSide::Sell,
            OrderType::Market,
        ),
        Order::new(
            "CLIENT_A".into(),
            "MSFT".into(),
            75.0,
            OrderSide::Buy,
            OrderType::Limit { price: 350.0 },
        ),
    ];
    
    // Insert orders
    info!("Inserting {} test orders", orders.len());
    for order in &orders {
        storage.insert_order(order.clone()).await?;
        info!("Inserted order: {} for {}", order.id, order.symbol);
    }
    
    // Retrieve and display
    info!("\n--- Testing Retrieval ---");
    for order in &orders {
        if let Some(retrieved) = storage.get_order(&order.id).await? {
            info!("Retrieved: {} - {} {} shares of {} @ {:?}", 
                retrieved.id,
                if retrieved.side == OrderSide::Buy { "BUY" } else { "SELL" },
                retrieved.quantity,
                retrieved.symbol,
                retrieved.order_type
            );
        }
    }
    
    // Test queries
    info!("\n--- Testing Queries ---");
    let client_orders = storage.get_client_orders(&"CLIENT_A".into()).await?;
    info!("CLIENT_A has {} orders", client_orders.len());
    
    let active_orders = storage.get_active_orders().await?;
    info!("Active orders: {}", active_orders.len());
    
    // Get metrics
    let metrics = storage.get_metrics().await?;
    info!("\n--- Storage Metrics ---");
    info!("Total orders: {}", metrics.total_orders);
    info!("Cache hits: {}", metrics.cache_hits);
    info!("Avg read latency: {}ns", metrics.avg_read_latency_ns);
    
    Ok(())
}
