use std::sync::Arc;
use tonic::transport::Server;

use rust_kv_store::{KVStore, grpc_server::create_grpc_server, grpc_client::KvStoreClient};
use rust_kv_store::grpc_server::kvstore::{Value, DataType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Create RocksDB-based KV store
    let db_path = "./data/rocksdb_kvstore";
    let store = Arc::new(KVStore::new(db_path)?);
    
    println!("Using RocksDB storage at: {}", db_path);

    // Start gRPC server on IPv6
    let grpc_addr = "[::1]:50052".parse()?;
    let grpc_service = create_grpc_server(store.clone());
    
    println!("Starting gRPC server on {}", grpc_addr);
    let grpc_server = Server::builder()
        .add_service(grpc_service)
        .serve(grpc_addr);

    // Run gRPC server in background
    tokio::spawn(grpc_server);

    // Wait a moment for server to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Test the gRPC client
    println!("Testing RocksDB gRPC client...");
    let mut client = KvStoreClient::connect("http://[::1]:50052".to_string()).await?;

    // Health check
    let health = client.health().await?;
    println!("Health check: {}", health);

    // Create a test value
    let test_value = Value {
        shape: vec![2, 2],
        dtype: DataType::Fp64 as i32,
        size_check: 16,
        key_check: 12345,
        data: vec![vec![1, 2, 3, 4, 5, 6, 7, 8]],
    };
    let test_key = 12345u64;

    // Store value
    println!("Storing value with key: {}", test_key);
    client.put(test_key, test_value.clone()).await?;

    // List keys
    let keys = client.list().await?;
    println!("Available keys: {:?}", keys);

    // Get value
    let retrieved = client.get(test_key).await?;
    if let Some(value) = retrieved {
        println!("Retrieved value: shape={:?}, dtype={}", value.shape, value.dtype);
    }

    // Show database stats
    let db_size = store.get_db_size()?;
    println!("Database size: {} bytes", db_size);

    let count = store.len()?;
    println!("Number of entries: {}", count);

    // Compact the database
    store.compact()?;
    println!("Database compacted");

    // Delete value
    client.delete(test_key).await?;
    println!("Value deleted");

    // List keys again
    let keys_after_delete = client.list().await?;
    println!("Keys after delete: {:?}", keys_after_delete);

    println!("RocksDB example completed successfully!");
    println!("Data persists in: {}", db_path);
    
    Ok(())
} 