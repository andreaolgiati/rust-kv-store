use std::path::Path;
use std::sync::Arc;
use anyhow::Result;
use rocksdb::{DB, Options, WriteBatch};
use prost::Message;

pub mod grpc_server;
pub mod grpc_client;

// Include the generated protobuf types
use grpc_server::kvstore::Value;

#[derive(Debug, Clone)]
pub struct RocksDBStore {
    db: Arc<DB>,
}

impl RocksDBStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_max_open_files(10000);
        opts.set_use_fsync(true);
        opts.set_bytes_per_sync(1024 * 1024); // 1MB
        
        let db = DB::open(&opts, path)?;
        Ok(Self {
            db: Arc::new(db),
        })
    }

    pub fn put(&self, key: u64, value: Value) -> Result<Option<Value>> {
        let key_bytes = key.to_be_bytes();
        let value_bytes = value.encode_to_vec();
        
        // Check if key exists first
        let existing = self.db.get(key_bytes)?;
        let old_value = if let Some(existing_bytes) = existing {
            Some(Value::decode(existing_bytes.as_slice())?)
        } else {
            None
        };
        
        // Insert new value
        self.db.put(key_bytes, value_bytes)?;
        
        Ok(old_value)
    }

    pub fn get(&self, key: &u64) -> Result<Option<Value>> {
        let key_bytes = key.to_be_bytes();
        let value_bytes = self.db.get(key_bytes)?;
        
        if let Some(bytes) = value_bytes {
            let value = Value::decode(bytes.as_slice())?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    pub fn delete(&self, key: &u64) -> Result<Option<Value>> {
        let key_bytes = key.to_be_bytes();
        
        // Get the value before deleting
        let value_bytes = self.db.get(key_bytes)?;
        let value = if let Some(bytes) = value_bytes {
            Some(Value::decode(bytes.as_slice())?)
        } else {
            None
        };
        
        // Delete the key
        self.db.delete(key_bytes)?;
        
        Ok(value)
    }

    pub fn contains_key(&self, key: &u64) -> Result<bool> {
        let key_bytes = key.to_be_bytes();
        Ok(self.db.get(key_bytes)?.is_some())
    }

    pub fn len(&self) -> Result<usize> {
        let mut count = 0;
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for _ in iter {
            count += 1;
        }
        Ok(count)
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    pub fn keys(&self) -> Result<Vec<u64>> {
        let mut keys = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        
        for result in iter {
            let (key_bytes, _) = result?;
            if key_bytes.len() == 8 { // u64 is 8 bytes
                let key = u64::from_be_bytes(key_bytes.as_ref().try_into()?);
                keys.push(key);
            }
        }
        
        Ok(keys)
    }

    pub fn clear(&self) -> Result<()> {
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        let mut batch = WriteBatch::default();
        
        for result in iter {
            let (key_bytes, _) = result?;
            batch.delete(key_bytes);
        }
        
        self.db.write(batch)?;
        Ok(())
    }

    pub fn compact(&self) -> Result<()> {
        self.db.compact_range(None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }

    pub fn get_db_size(&self) -> Result<u64> {
        let mut size = 0;
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        
        for result in iter {
            let (key_bytes, value_bytes) = result?;
            size += key_bytes.len() as u64 + value_bytes.len() as u64;
        }
        
        Ok(size)
    }
}

impl Drop for RocksDBStore {
    fn drop(&mut self) {
        // RocksDB will be automatically closed when the Arc is dropped
    }
}

#[derive(Debug, Clone)]
pub struct KVStore {
    store: Arc<RocksDBStore>,
}

impl KVStore {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let store = RocksDBStore::new(path)?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    pub fn put(&self, key: u64, value: Value) -> Result<Option<Value>> {
        self.store.put(key, value)
    }

    pub fn get(&self, key: &u64) -> Result<Option<Value>> {
        self.store.get(key)
    }

    pub fn delete(&self, key: &u64) -> Result<Option<Value>> {
        self.store.delete(key)
    }

    pub fn contains_key(&self, key: &u64) -> Result<bool> {
        self.store.contains_key(key)
    }

    pub fn len(&self) -> Result<usize> {
        self.store.len()
    }

    pub fn is_empty(&self) -> Result<bool> {
        self.store.is_empty()
    }

    pub fn keys(&self) -> Result<Vec<u64>> {
        self.store.keys()
    }

    pub fn clear(&self) -> Result<()> {
        self.store.clear()
    }

    pub fn compact(&self) -> Result<()> {
        self.store.compact()
    }

    pub fn get_db_size(&self) -> Result<u64> {
        self.store.get_db_size()
    }
}

impl Default for KVStore {
    fn default() -> Self {
        // Create a default RocksDB store in a temporary directory
        let temp_dir = std::env::temp_dir().join("kvstore_default");
        Self::new(temp_dir).expect("Failed to create default KVStore")
    }
}

#[test]
fn test_kv_store_operations() {
    use rand::Rng;
    use sha2::{Digest};
    use grpc_server::kvstore::DataType;
    use std::collections::HashSet;
    let temp_dir = std::env::temp_dir().join(format!("kvstore_test_{}", uuid::Uuid::new_v4()));
    let store = KVStore::new(&temp_dir).unwrap();
    let mut rng = rand::thread_rng();
    let mut keys_and_hashes = Vec::new();
    let mut keys_so_far = HashSet::new();
    for i in 0..100000 {
        let mut key = rng.gen_range(0..1000000);
        while keys_so_far.contains(&key) {
            key = rng.gen_range(0..1000000);
        }
        keys_so_far.insert(key);
        let shape = vec![rng.gen_range(1..10), rng.gen_range(1..10)];
        let dtype = DataType::Fp64; 
        let size_check = shape.iter().product::<u64>() * 8; // FP64 is 8 bytes
        let key_check = key;
        let data = vec![vec![rng.gen_range(0..1000000) as u8; 8]]; // Create proper data structure
        let data_hash: [u8; 32] = sha2::Sha256::digest(&data[0]).into();
       
        
        let value = Value {
            shape,
            dtype: dtype as i32,
            size_check,
            key_check,
            data,
        };
        
        store.put(key, value).unwrap();
        keys_and_hashes.push((key, data_hash));
        let data_hash_str = hex::encode(data_hash);
        println!("iter: {}, key: {}, size_check: {}, key_check: {}, data_hash: {:?}", i, key, size_check, key_check, data_hash_str);
    }
    for (key, data_hash) in keys_and_hashes {
        let value = store.get(&key).unwrap();
        assert!(value.is_some());
        let value = value.unwrap();
        let got_data_hash: [u8; 32] = sha2::Sha256::digest(&value.data[0]).into();
        assert_eq!(data_hash, got_data_hash, "key: {}, data_hash: {:?}, got_data_hash: {:?}", key, data_hash, got_data_hash);
    }
    store.clear().unwrap();
    assert!(store.is_empty().unwrap());
}

