use std::{collections::HashMap, sync::Arc, time::Duration};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{StreamExt, SinkExt};
use tokio::sync::{mpsc, Mutex};
use tracing::info;
use uuid::Uuid;
use bytes::Bytes;
use crate::room::{spawn_room, RoomCommand, RoomHandle, OutboundMessage};

#[derive(Clone)]
pub struct AppState {
    pub rooms: Arc<Mutex<HashMap<String, RoomHandle>>>,
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/ws/:document_id", get(ws_handler))
        .with_state(state)
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Path(document_id): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, document_id, state))
}

async fn handle_socket(socket: WebSocket, document_id: String, state: AppState) {
    let client_id = Uuid::new_v4().to_string();
    info!("🟢 Client {} connecting to {}", client_id, document_id);
    
    let mut rooms = state.rooms.lock().await;
    let room_handle = rooms
        .entry(document_id.clone())
        .or_insert_with(|| spawn_room(document_id.clone(), Duration::from_secs(60)))
        .clone();
    drop(rooms);

    let (tx, mut rx) = mpsc::channel::<OutboundMessage>(32);

    room_handle
        .command_tx
        .send(RoomCommand::Join {
            client_id: client_id.clone(),
            tx: tx.clone(),
        })
        .await
        .unwrap();

    let (mut sender, mut receiver) = socket.split();

    let reader_tx = room_handle.command_tx.clone();
    let reader_cid = client_id.clone();
    let reader = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            match msg {
                Message::Binary(bin) => {
                    let update = Bytes::from(bin);
                    let _ = reader_tx
                        .send(RoomCommand::ApplyUpdate {
                            client_id: reader_cid.clone(),
                            update,
                        })
                        .await;
                }
                Message::Text(text) => {
                    info!("💬 Client {} sent text: {}", reader_cid, text);
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        let _ = reader_tx
            .send(RoomCommand::Leave {
                client_id: reader_cid,
            })
            .await;
    });

    let writer = tokio::spawn(async move {
        while let Some(out_msg) = rx.recv().await {
            if let Ok(json) = serde_json::to_string(&out_msg) {
                if sender.send(Message::Text(json)).await.is_err() {
                    break;
                }
            }
        }
    });

    // Wait for either side to end
    tokio::select! {
        _ = reader => (),
        _ = writer => (),
    }

    info!("🔴 Client {} disconnected", client_id);
}
