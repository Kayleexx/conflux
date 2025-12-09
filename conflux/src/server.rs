use crate::{
    auth::{Claims, generate_token, validate_token, validate_token_anonymous},
    errors::{ConfluxError, Result},
    room::{OutboundMessage, RoomCommand},
    room_manager::{RoomInfo, RoomManager},
};
use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::{get, post},
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub room_manager: Arc<RoomManager>,
    pub anonymous_mode: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMessage {
    Update { data: String },
    Awareness { data: serde_json::Value },
    SyncRequest,
    Chat { message: String },
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/login", post(login_handler))
        .route("/dashboard", get(dashboard_handler))
        .route("/ws/{document_id}", get(ws_route))
        .with_state(state)
}

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
}

async fn login_handler(Json(req): Json<LoginRequest>) -> Json<serde_json::Value> {
    let token = generate_token(&req.username);
    Json(serde_json::json!({ "token": token }))
}

async fn dashboard_handler(State(state): State<AppState>) -> Result<Json<Vec<RoomInfo>>> {
    let rooms = state.room_manager.list_rooms().await;
    Ok(Json(rooms))
}

async fn ws_route(
    ws: WebSocketUpgrade,
    Path(document_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let state_clone = state.clone();
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = handle_ws(socket, document_id, params, state_clone).await {
            error!("WebSocket error: {:?}", e);
        }
    })
}

async fn handle_ws(
    mut socket: WebSocket,
    document_id: String,
    params: HashMap<String, String>,
    state: AppState,
) -> Result<()> {
    let token = params
        .get("token")
        .ok_or_else(|| ConfluxError::AuthError("Missing token".into()))?
        .clone();

    let claims: Claims = if state.anonymous_mode {
        validate_token_anonymous(&token)?
    } else {
        validate_token(&token)?
    };
    let user_id = claims.sub;
    let session_id = claims.sid;
    let client_id = Uuid::new_v4().to_string();

    info!(
        "🟢 {} (session {}) connected to {}",
        user_id, session_id, document_id
    );

    let room_handle = state.room_manager.get_or_create_room(&document_id).await;
    let (tx, mut rx) = mpsc::channel::<OutboundMessage>(32);
    room_handle
        .command_tx
        .send(RoomCommand::Join {
            client_id: client_id.clone(),
            tx: tx.clone(),
        })
        .await
        .map_err(|e| ConfluxError::RoomSendError(e.to_string()))?;

    loop {
        tokio::select! {
            // incoming messages
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(msg)) => match msg {
                        Message::Binary(bin) => {
                            info!("[Room {}] Binary update from {}", document_id, client_id);
                            let _ = room_handle.command_tx.send(RoomCommand::ApplyUpdate {
                                client_id: client_id.clone(),
                                update: bin,
                            }).await;
                        }

                        Message::Text(raw_text) => {
                            let text = raw_text.trim();
                            if text.is_empty() {
                                continue;
                            }

                            if text.starts_with('{') {
                                match serde_json::from_str::<ClientMessage>(text) {
                                    Ok(ClientMessage::Update { data }) => {
                                        info!("[Room {}] Update from {}", document_id, client_id);
                                        match STANDARD.decode(&data) {
                                            Ok(decoded) => {
                                                let _ = room_handle.command_tx.send(RoomCommand::ApplyUpdate {
                                                    client_id: client_id.clone(),
                                                    update: Bytes::from(decoded),
                                                }).await;
                                            }
                                            Err(e) => warn!("Base64 decode failed: {:?}", e),
                                        }
                                    }

                                    Ok(ClientMessage::Awareness { data }) => {
                                        info!("[Room {}] Awareness from {}: {:?}", document_id, client_id, data);
                                        let _ = room_handle.command_tx.send(RoomCommand::SetAwareness {
                                            client_id: client_id.clone(),
                                            state: data,
                                        }).await;
                                    }

                                    Ok(ClientMessage::Chat { message }) => {
                                        info!("[Room {}] Chat from {}: {}", document_id, client_id, message);
                                        let _ = room_handle.command_tx.send(RoomCommand::Chat {
                                            client_id: client_id.clone(),
                                            message,
                                        }).await;
                                    }

                                    Ok(ClientMessage::SyncRequest) => {
                                        info!("[Room {}] Sync request from {}", document_id, client_id);
                                        let (reply_tx, reply_rx) = oneshot::channel();
                                        let _ = room_handle.command_tx.send(RoomCommand::RequestSync {
                                            client_id: client_id.clone(),
                                            state_vector: vec![],
                                            reply_to: reply_tx,
                                        }).await;

                                        if let Ok(sync_data) = reply_rx.await {
                                            let response = serde_json::json!({
                                                "type": "sync_response",
                                                "data": STANDARD.encode(sync_data),
                                            });
                                            if let Ok(json_str) = serde_json::to_string(&response) {
                                                let _ = socket.send(Message::Text(json_str.into())).await;
                                            }
                                        }
                                    }

                                    Err(e) => {
                                        warn!("Invalid JSON from {}: {:?}", client_id, e);
                                    }
                                }
                            } else {
                                info!("[Room {}] Chat (plain) from {}: {}", document_id, client_id, text);
                                room_handle.command_tx.send(RoomCommand::Chat {
                                    client_id: client_id.clone(),
                                    message: text.to_string(),
                                }).await.map_err(|e| ConfluxError::RoomSendError(e.to_string()))?;
                            }
                        }

                        Message::Ping(p) => { let _ = socket.send(Message::Pong(p)).await; }
                        Message::Pong(_) => {}
                        Message::Close(_) => break,
                    },

                    Some(Err(e)) => {
                        warn!("WebSocket error for {}: {:?}", client_id, e);
                        break;
                    }

                    None => break,
                }
            },
            outgoing = rx.recv() => {
                match outgoing {
                    Some(out_msg) => {
                        if let Ok(json) = serde_json::to_string(&out_msg)
                            && socket.send(Message::Text(json.into())).await.is_err()
                        {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }

    let _ = room_handle
        .command_tx
        .send(RoomCommand::Leave { client_id })
        .await;

    info!(
        "🔴 {} (session {}) disconnected from {}",
        user_id, session_id, document_id
    );
    Ok(())
}
