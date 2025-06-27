use std::sync::Arc;
use anyhow::Result;
use rust_kv_store::KVStore;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    info!("Starting Rust KV Store server...");
    
    let prefix = std::env::var("DATA_DIR_PREFIX").unwrap_or_else(|_| "./data".to_string());

    // Create a new unique data directory under prefix
    let data_dir = format!("{}/{}", prefix, uuid::Uuid::new_v4());
    std::fs::create_dir_all(&data_dir)?;

    // Create the KV store (RocksDB)
    let _store = Arc::new(KVStore::new(&data_dir)?);
    
    // For now, just print that the store is created
    // TODO: Add HTTP server implementation
    info!("KV Store created successfully at {}", data_dir);
    info!("Store is ready for use");
    
    // Keep the process running
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");
    
    Ok(())
} 