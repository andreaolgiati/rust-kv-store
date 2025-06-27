use tonic::transport::Channel;
use crate::grpc_server::kvstore::kv_store_service_client::KvStoreServiceClient;
use crate::grpc_server::kvstore::{PutRequest, GetRequest, DeleteRequest, ListRequest, HealthRequest};

pub struct KvStoreClient {
    client: KvStoreServiceClient<Channel>,
}

impl KvStoreClient {
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let client = KvStoreServiceClient::connect(addr).await?;
        Ok(Self { client })
    }

    pub async fn put(&mut self, key: u64, value: crate::grpc_server::kvstore::Value) -> Result<(), tonic::Status> {
        let request = tonic::Request::new(PutRequest { key, value: Some(value) });
        let _response = self.client.put(request).await?;
        Ok(())
    }

    pub async fn get(&mut self, key: u64) -> Result<Option<crate::grpc_server::kvstore::Value>, tonic::Status> {
        let request = tonic::Request::new(GetRequest { key });
        let response = self.client.get(request).await?;
        Ok(response.into_inner().value)
    }

    pub async fn delete(&mut self, key: u64) -> Result<(), tonic::Status> {
        let request = tonic::Request::new(DeleteRequest { key });
        let _response = self.client.delete(request).await?;
        Ok(())
    }

    pub async fn list(&mut self) -> Result<Vec<u64>, tonic::Status> {
        let request = tonic::Request::new(ListRequest {});
        let response = self.client.list(request).await?;
        Ok(response.into_inner().keys)
    }

    pub async fn health(&mut self) -> Result<String, tonic::Status> {
        let request = tonic::Request::new(HealthRequest {});
        let response = self.client.health(request).await?;
        Ok(response.into_inner().status)
    }
} 