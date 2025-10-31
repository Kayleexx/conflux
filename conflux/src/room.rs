use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use tokio::sync::mpsc;
use yrs::updates::encoder::Encode;
use crate::crdt::CrdtEngine;
use yrs::sync::Awareness;
use tokio::task::{spawn_local, LocalSet};

pub struct Room {
    pub document_id: String,
    client_count: AtomicUsize, 
}

impl Room {
    pub fn client_count(&self) -> usize {
        self.client_count.load(Ordering::Relaxed)
    }
}

pub enum RoomCommand { 
    Join { 
        client_id: String, 
        tx: mpsc::Sender<OutboundMessage> 
    },
    Leave { 
        client_id: String 
    }, 
    ApplyUpdate { 
        client_id: String, 
        update: Vec<u8> 
    }, 
    RequestSync { 
        client_id: String, 
        state_vector: Vec<u8>, 
        reply_to: mpsc::Sender<Vec<u8>> 
    },
    SetAwareness { 
        client_id: String, 
        state: serde_json::Value 
    }
}

#[derive(Clone)]
pub enum OutboundMessage {
    Update { 
        document_id: String, 
        update: Vec<u8> 
    },
    Awareness { 
        document_id: String, 
        update: Vec<u8> 
    }, 
    System(String)
}


pub fn spawn_room(document_id: String) -> (mpsc::Sender<RoomCommand>, Arc<Room>) {
    let (command_tx, mut command_rx) = mpsc::channel::<RoomCommand>(32);

    let room = Arc::new(Room {
        document_id: document_id.clone(),
        client_count: AtomicUsize::new(0),
    });
    
    let room_clone = room.clone();

    spawn_local(async move {
        room_actor(document_id, command_rx, room_clone).await;
    });
    
    (command_tx, room)
}

async fn broadcast_to_clients(
    clients: &mut HashMap<String, mpsc::Sender<OutboundMessage>>,
    sender_id: &str,
    message: OutboundMessage,
) {

    let mut failed_clients = Vec::new();

    for (client_id, client_tx) in clients.iter() {
        if client_id == sender_id {
            continue;
        }

        if client_tx.send(message.clone()).await.is_err() {
            println!("Client {} disconnected (send failed)", client_id);
            failed_clients.push(client_id.clone());
        }
    }
    
    for client_id in failed_clients {
        clients.remove(&client_id);
    }
}
async fn room_actor(
    document_id: String,
    mut command_rx: mpsc::Receiver<RoomCommand>,
    room_meta: Arc<Room>,
) {
    let mut clients: HashMap<String, mpsc::Sender<OutboundMessage>> = HashMap::new();
    let crdt = CrdtEngine::new();
    let awareness = Awareness::new(crdt.doc().read().unwrap().clone());

    while let Some(command) = command_rx.recv().await {
        match command {
            RoomCommand::Join { client_id, tx } => {
                println!("Client {} joined", client_id);
                clients.insert(client_id, tx);
                room_meta.client_count.store(clients.len(), Ordering::Relaxed);
            }

            RoomCommand::Leave { client_id } => {
                println!("Client {} left", client_id);
                clients.remove(&client_id);
                room_meta.client_count.store(clients.len(), Ordering::Relaxed);
            }

            RoomCommand::ApplyUpdate { client_id, update } => {
                crdt.apply_update(&update);
                broadcast_to_clients(
                    &mut clients,
                    &client_id,
                    OutboundMessage::Update { document_id: document_id.clone(), update },
                )
                .await;
            }

            RoomCommand::RequestSync { client_id, state_vector, reply_to } => {
                let diff = crdt.encode_diff(&state_vector);
                let _ = reply_to.send(diff).await;
            }

            RoomCommand::SetAwareness { client_id, state } => {
                println!("Awareness update from {}: {:?}", client_id, state);
                awareness.set_local_state(state);
                let update = awareness.update().unwrap().encode_v1();

                broadcast_to_clients(
                    &mut clients,
                    &client_id,
                    OutboundMessage::Awareness {
                        document_id: document_id.clone(),
                        update,
                    },
                )
                .await;
            }
        }
    }

    println!("Room {} actor shutting down", document_id);
}

fn make_test_room() -> (mpsc::Sender<RoomCommand>, Arc<Room>, tokio::task::LocalSet) {
    let local = LocalSet::new();
    let (tx, room) = spawn_room("test_doc".to_string());
    (tx, room, local)
}


