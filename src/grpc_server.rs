use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::{KVStore};

// Include the generated protobuf code
pub mod kvstore {
    tonic::include_proto!("kvstore");
}

use kvstore::kv_store_service_server::{KvStoreService, KvStoreServiceServer};
use kvstore::{
    CreateStoreRequest, CreateStoreResponse,
    DeleteRequest, DeleteResponse, GetRequest, GetResponse,
    HealthRequest, HealthResponse, ListRequest, ListResponse,
    PutRequest, PutResponse,
};

pub struct KvStoreGrpcService {
    store: Arc<KVStore>,
}

impl KvStoreGrpcService {
    pub fn new(store: Arc<KVStore>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl KvStoreService for KvStoreGrpcService {
    async fn create_store(
        &self,
        request: Request<CreateStoreRequest>,
    ) -> Result<Response<CreateStoreResponse>, Status> {
        let req = request.into_inner();
        
        // For now, just return success since the store is already created
        // In a real implementation, you might create a new store instance
        Ok(Response::new(CreateStoreResponse {
            success: true,
            message: format!("Store '{}' created successfully", req.name),
        }))
    }

    async fn put(
        &self,
        request: Request<PutRequest>,
    ) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        
        let value = match req.value {
            Some(v) => v,
            None => return Err(Status::invalid_argument("Value is required")),
        };

        let existing = self.store.put(req.key, value.clone())
            .map_err(|_| Status::internal("Storage error"))?;
        
        let message = if existing.is_some() {
            "Value updated successfully"
        } else {
            "Value stored successfully"
        };

        Ok(Response::new(PutResponse {
            key: req.key,
            success: true,
            message: message.to_string(),
        }))
    }

    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        
        let value = self.store.get(&req.key)
            .map_err(|_| Status::internal("Storage error"))?;
        
        let (success, message) = if value.is_some() {
            (true, "Value retrieved successfully")
        } else {
            (false, "Value not found")
        };

        Ok(Response::new(GetResponse {
            key: req.key,
            value,
            success,
            message: message.to_string(),
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        
        let deleted = self.store.delete(&req.key)
            .map_err(|_| Status::internal("Storage error"))?;
        
        let (success, message) = if deleted.is_some() {
            (true, "Value deleted successfully")
        } else {
            (false, "Value not found")
        };

        Ok(Response::new(DeleteResponse {
            key: req.key,
            success,
            message: message.to_string(),
        }))
    }

    async fn list(
        &self,
        _request: Request<ListRequest>,
    ) -> Result<Response<ListResponse>, Status> {
        let keys = self.store.keys()
            .map_err(|_| Status::internal("Storage error"))?;
        
        let count = keys.len() as u32;

        Ok(Response::new(ListResponse {
            keys,
            count,
            success: true,
        }))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "healthy".to_string(),
            service: "rust-kv-store".to_string(),
        }))
    }
}

pub fn create_grpc_server(store: Arc<KVStore>) -> KvStoreServiceServer<KvStoreGrpcService> {
    KvStoreServiceServer::new(KvStoreGrpcService::new(store))
} 