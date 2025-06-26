use std::sync::Arc;
use anyhow::Result;
use rust_kv_store::{KVStore, server};
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    info!("Starting Rust KV Store server...");
    
    // Create the KV store
    let store = Arc::new(KVStore::new());
    
    // Create the server
    let app = server::create_server(store).await?;
    
    // Start the server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    info!("Server listening on http://127.0.0.1:3000");
    
    axum::serve(listener, app).await?;
    
    Ok(())
} 