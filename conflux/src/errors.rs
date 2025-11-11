use thiserror::Error;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[derive(Debug, Error)]
pub enum ConfluxError {
    #[error("Invalid client message: {0}")]
    InvalidMessage(String),

    #[error("WebSocket send error: {0}")]
    WebSocketSend(String),

    #[error("Failed to decode CRDT update: {0}")]
    DecodeError(String),

    #[error("Internal server error: {0}")]
    Internal(String),
}

impl IntoResponse for ConfluxError {
    fn into_response(self) -> Response {
        let status = match self {
            ConfluxError::InvalidMessage(_) => StatusCode::BAD_REQUEST,
            ConfluxError::WebSocketSend(_) => StatusCode::BAD_GATEWAY,
            ConfluxError::DecodeError(_) => StatusCode::UNPROCESSABLE_ENTITY,
            ConfluxError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body = axum::Json(serde_json::json!({
            "error": self.to_string()
        }));

        (status, body).into_response()
    }
}

#[macro_export]
macro_rules! err {
    ($variant:ident, $msg:expr) => {
        $crate::errors::ConfluxError::$variant($msg.to_string())
    };
}
