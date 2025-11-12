use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;
use tracing::error;

#[derive(Error, Debug)]
pub enum ConfluxError {
    #[error("Room not found: {0}")]
    RoomNotFound(String),

    #[error("Failed to send message to room: {0}")]
    RoomSendError(String),

    #[error("WebSocket message error: {0}")]
    WebSocketError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Unexpected internal error: {0}")]
    Internal(String),

    #[error("Authentication error: {0}")]
    AuthError(String),
}

impl IntoResponse for ConfluxError {
    fn into_response(self) -> Response {
        error!("{:?}", self); 

        let (status, msg) = match &self {
            ConfluxError::RoomNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            ConfluxError::RoomSendError(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            ConfluxError::WebSocketError(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            ConfluxError::SerializationError(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            ConfluxError::AuthError(_) => (StatusCode::UNAUTHORIZED, self.to_string()),
            ConfluxError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error".to_string()),
        };
        let body = Json(json!({
            "error": msg,
            "code": status.as_u16(),
        }));

        (status, body).into_response()
    }
}

pub type Result<T> = std::result::Result<T, ConfluxError>;
