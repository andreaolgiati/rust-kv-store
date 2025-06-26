use std::sync::Arc;
use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
    routing::{get, post, delete},
    Router,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use uuid::Uuid;

use crate::{KVStore, Matrix};

#[derive(Debug, Serialize, Deserialize)]
pub struct StoreRequest {
    pub matrix: Matrix,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoreResponse {
    pub key: String,
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetResponse {
    pub key: String,
    pub matrix: Option<Matrix>,
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListResponse {
    pub keys: Vec<String>,
    pub count: usize,
    pub success: bool,
}

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<KVStore>,
}

pub async fn create_server(store: Arc<KVStore>) -> Result<Router> {
    let state = AppState { store };

    let cors = CorsLayer::permissive();

    let app = Router::new()
        .route("/store/:key", post(store_matrix))
        .route("/store/:key", get(get_matrix))
        .route("/store/:key", delete(delete_matrix))
        .route("/store", get(list_keys))
        .route("/health", get(health_check))
        .layer(cors)
        .with_state(state);

    Ok(app)
}

async fn store_matrix(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(request): Json<StoreRequest>,
) -> impl IntoResponse {
    let key_uuid = match Uuid::parse_str(&key) {
        Ok(uuid) => uuid,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid UUID").into_response(),
    };

    let existing = state.store.insert(key_uuid, request.matrix);
    
    let message = if existing.is_some() {
        "Matrix updated successfully"
    } else {
        "Matrix stored successfully"
    };

    (StatusCode::OK, Json(StoreResponse {
        key,
        success: true,
        message: message.to_string(),
    })).into_response()
}

async fn get_matrix(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let key_uuid = match Uuid::parse_str(&key) {
        Ok(uuid) => uuid,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid UUID").into_response(),
    };

    let matrix = state.store.get(&key_uuid);
    
    let (success, message) = if matrix.is_some() {
        (true, "Matrix retrieved successfully")
    } else {
        (false, "Matrix not found")
    };

    (StatusCode::OK, Json(GetResponse {
        key,
        matrix,
        success,
        message: message.to_string(),
    })).into_response()
}

async fn delete_matrix(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let key_uuid = match Uuid::parse_str(&key) {
        Ok(uuid) => uuid,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid UUID").into_response(),
    };

    let deleted = state.store.remove(&key_uuid);
    
    let (success, message) = if deleted.is_some() {
        (true, "Matrix deleted successfully")
    } else {
        (false, "Matrix not found")
    };

    (StatusCode::OK, Json(StoreResponse {
        key,
        success,
        message: message.to_string(),
    })).into_response()
}

async fn list_keys(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let keys: Vec<String> = state.store.keys()
        .iter()
        .map(|k| k.to_string())
        .collect();
    let count = keys.len();
    (StatusCode::OK, Json(ListResponse {
        keys,
        count,
        success: true,
    })).into_response()
}

async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({
        "status": "healthy",
        "service": "rust-kv-store"
    }))).into_response()
} 