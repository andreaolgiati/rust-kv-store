use rust_kv_store::{KVStore, grpc_server, grpc_client};
use std::sync::Arc;
use rand::Rng;
use sha2::Digest;
use grpc_server::kvstore::DataType;
use std::collections::HashSet;
use tonic::transport::Server;
use std::net::SocketAddr;
use std::str::FromStr;

#[tokio::test]
async fn test_grpc_operations() {
    // Create a temporary store
    let temp_dir = std::env::temp_dir().join(format!("kvstore_grpc_test_{}", uuid::Uuid::new_v4()));
    let store = Arc::new(KVStore::new(&temp_dir).unwrap());
    
    // Create gRPC server
    let grpc_service = grpc_server::create_grpc_server(store.clone());
    let addr = SocketAddr::from_str("[::1]:50051").unwrap();
    
    // Start server in background
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(grpc_service)
            .serve(addr)
            .await
    });
    
    // Wait a bit for server to start
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // Create client
    let mut client = grpc_client::KvStoreClient::connect("http://[::1]:50051".to_string()).await.unwrap();
    
    // Test health check
    let health_status = client.health().await.unwrap();
    assert_eq!(health_status, "healthy");
    
    // Test basic operations
    let mut rng = rand::thread_rng();
    let mut keys_and_hashes = Vec::new();
    let mut keys_so_far = HashSet::new();
    
    // Test PUT operations
    for _i in 0..10 {
        let mut key = rng.gen_range(0..1000000);
        while keys_so_far.contains(&key) {
            key = rng.gen_range(0..1000000);
        }
        keys_so_far.insert(key);
        
        let shape = vec![rng.gen_range(1..10), rng.gen_range(1..10)];
        let dtype = DataType::Fp64;
        let size_check = shape.iter().product::<u64>() * 8;
        let key_check = key;
        let data = vec![vec![rng.gen_range(0..255) as u8; 8]];
        let data_hash: [u8; 32] = sha2::Sha256::digest(&data[0]).into();
        
        let value = grpc_server::kvstore::Value {
            shape,
            dtype: dtype as i32,
            size_check,
            key_check,
            data: data.clone(),
        };
        
        // Test PUT
        client.put(key, value).await.unwrap();
        keys_and_hashes.push((key, data_hash, data));
        
        println!("gRPC PUT: key={}, hash={:?}", key, hex::encode(data_hash));
    }
    
    // Test LIST
    let keys = client.list().await.unwrap();
    assert_eq!(keys.len(), 10);
    for (key, _, _) in &keys_and_hashes {
        assert!(keys.contains(key));
    }
    
    // Test GET operations
    for (key, expected_hash, expected_data) in &keys_and_hashes {
        let retrieved_value = client.get(*key).await.unwrap();
        assert!(retrieved_value.is_some());
        
        let value = retrieved_value.unwrap();
        let actual_hash: [u8; 32] = sha2::Sha256::digest(&value.data[0]).into();
        
        assert_eq!(*expected_hash, actual_hash, 
            "Hash mismatch for key {}: expected {:?}, got {:?}", 
            key, hex::encode(expected_hash), hex::encode(actual_hash));
        
        assert_eq!(value.data, *expected_data);
        assert_eq!(value.key_check, *key);
    }
    
    // Test DELETE operations
    for (key, _, _) in &keys_and_hashes {
        client.delete(*key).await.unwrap();
        
        // Verify deletion
        let retrieved_value = client.get(*key).await.unwrap();
        assert!(retrieved_value.is_none());
    }
    
    // Verify all keys are deleted
    let remaining_keys = client.list().await.unwrap();
    assert_eq!(remaining_keys.len(), 0);
    
    // Clean up
    store.clear().unwrap();
    server_handle.abort();
} 