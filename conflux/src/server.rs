use crate::room::{OutboundMessage, RoomCommand};
use crate::room_manager::RoomManager;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn, error};
use uuid::Uuid;
use crate::errors::{ConfluxError, Result};


#[derive(Clone)]
pub struct AppState {
    pub room_manager: Arc<RoomManager>,
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/ws/:document_id", get(ws_handler))
        .with_state(state)
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMessage {
    Update { data: String },                 // base64 Yjs update
    Awareness { data: serde_json::Value },   // arbitrary awareness JSON
    SyncRequest,                             // ask for current diff
    Chat { message: String },                // demo chat
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Path(document_id): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = handle_socket(socket, document_id, state).await {
            error!("WebSocket session failed: {:?}", e);
        }
    })
}

async fn handle_socket(mut socket: WebSocket, document_id: String, state: AppState) -> Result<()> {
    let client_id = Uuid::new_v4().to_string();
    info!("🟢 Client {} connecting to {}", client_id, document_id);

    let room_handle = state.room_manager.get_or_create_room(&document_id).await;

    let (tx, mut rx) = mpsc::channel::<OutboundMessage>(32);

    let _ = room_handle
        .command_tx
        .send(RoomCommand::Join {
            client_id: client_id.clone(),
            tx: tx.clone(),
        })
        .await.map_err(|e| ConfluxError::RoomSendError(e.to_string()))?;


    loop {
        tokio::select! {
            maybe_msg = socket.recv() => {
                match maybe_msg {
                    Some(Ok(Message::Binary(bin))) => {
                        let _ = room_handle.command_tx.send(RoomCommand::ApplyUpdate {
                            client_id: client_id.clone(),
                            update: Bytes::from(bin),
                        }).await.map_err(|e| ConfluxError::RoomSendError(e.to_string()))?;
                    },
                    Some(Ok(Message::Text(text))) => {
                        info!(" Client {} sent text: {}", client_id, text);
                            if text.trim().is_empty() {
                                continue; }

                        match serde_json::from_str::<ClientMessage>(&text) {
                            Ok(ClientMessage::Update { data }) => {
                                match base64::decode(&data) {
                                    Ok(decoded) => {
                                        let _ = room_handle.command_tx.send(RoomCommand::ApplyUpdate {
                                            client_id: client_id.clone(),
                                            update: Bytes::from(decoded),
                                        }).await;
                                    }
                                    Err(e) => {
                                        warn!("Failed to decode base64 update from {}: {:?}", client_id, e);
                                    }
                                }
                            },
                            Ok(ClientMessage::Awareness { data }) => {
                                let _ = room_handle.command_tx.send(RoomCommand::SetAwareness {
                                    client_id: client_id.clone(),
                                    state: data,
                                }).await.map_err(|e| ConfluxError::RoomSendError(e.to_string()))?;
                            },
                            Ok(ClientMessage::SyncRequest) => {
                                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                                let _ = room_handle.command_tx.send(RoomCommand::RequestSync {
                                    client_id: client_id.clone(),
                                    state_vector: vec![],
                                    reply_to: reply_tx,
                                }).await.map_err(|e| ConfluxError::RoomSendError(e.to_string()))?;

                                if let Ok(sync_data) = reply_rx.await {
                                    let response = serde_json::json!({
                                        "type": "sync_response",
                                        "data": base64::encode(sync_data),
                                    });
                                    if let Ok(json_str) = serde_json::to_string(&response) {
                                        if socket.send(Message::Text(json_str)).await.is_err() {
                                            break; // socket closed
                                        }
                                    }
                                }
                            },
                            Ok(ClientMessage::Chat { message }) => {
                                info!(" Chat from {}: {}", client_id, message);
                                let _ = room_handle.command_tx.send(RoomCommand::Chat {
                                    client_id: client_id.clone(),
                                    message,
                                }).await.map_err(|e| ConfluxError::RoomSendError(e.to_string()))?;
                            },
                            Err(e) => {
                                warn!("Invalid JSON from {}: {:?}", client_id, e);
                            }
                        }
                    },
                    Some(Ok(Message::Close(_))) => break,
                    Some(Ok(_)) => { /* ignore Ping/Pong etc. */ },
                    Some(Err(e)) => {
                        warn!("WebSocket error for {}: {:?}", client_id, e);
                        break;
                    },
                    None => break, // peer closed
                }
            }

            maybe_out = rx.recv() => {
                match maybe_out {
                    Some(out_msg) => {
                        match serde_json::to_string(&out_msg) {
                            Ok(json) => {
                                if socket.send(Message::Text(json)).await.is_err() {
                                    break; // socket closed
                                }
                            }
                            Err(e) => {
                                warn!("Serialize OutboundMessage failed for {}: {:?}", client_id, e);
                            }
                        }
                    }
                    None => {
                        // Room dropped our sender; nothing more to write
                        break;
                    }
                }
            }
        }
    }

    let _ = room_handle
        .command_tx
        .send(RoomCommand::Leave {
            client_id: client_id.clone(),
        })
        .await;

    info!("🔴 Client {} disconnected", client_id);
    Ok(())
}
