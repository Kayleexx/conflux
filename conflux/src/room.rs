use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering, AtomicU64};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time::interval;
use tokio::sync::{mpsc, oneshot};
use yrs::updates::encoder::Encode;
use crate::crdt::CrdtEngine;
use yrs::sync::Awareness;
use tokio::task::{spawn_local, JoinHandle};
use bytes::Bytes;

pub struct Room {
    pub document_id: String,
    client_count: AtomicUsize, 
    updates_received: AtomicU64,
    awareness_events: AtomicU64,
    clients_removed_for_slow: AtomicU64,
}

impl Room {
    pub fn client_count(&self) -> usize {
        self.client_count.load(Ordering::Relaxed)
    }
    pub fn updates_received(&self) -> u64 {
        self.updates_received.load(Ordering::Relaxed)
    }
    pub fn awareness_events(&self) -> u64 {
        self.awareness_events.load(Ordering::Relaxed)
    }
    pub fn clients_removed_for_slow(&self) -> u64 {
        self.clients_removed_for_slow.load(Ordering::Relaxed)
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
        update: Bytes,
    }, 
    RequestSync { 
        client_id: String, 
        state_vector: Vec<u8>, 
        reply_to: oneshot::Sender<Bytes>, 
    },
    SetAwareness { 
        client_id: String, 
        state: serde_json::Value 
    },
    Shutdown,
}

#[derive(Clone)]
pub enum OutboundMessage {
    Update { 
        document_id: String, 
        update: Bytes
    },
    Awareness { 
        document_id: String, 
        update: Bytes
    }, 
    System(String)
}

pub struct RoomHandle {
    pub room: Arc<Room>,
    pub command_tx: mpsc::Sender<RoomCommand>,
    pub actor_handle: JoinHandle<()>,
    shutdown_tx: oneshot::Sender<()>,
}

impl RoomHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        let _ = self.actor_handle.await;
    }
}

pub fn spawn_room(
    document_id: String,
    idle_timeout: Duration,
) -> RoomHandle {
    let (command_tx, command_rx) = mpsc::channel::<RoomCommand>(32);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let room = Arc::new(Room {
        document_id: document_id.clone(),
        client_count: AtomicUsize::new(0),
        updates_received: AtomicU64::new(0),
        awareness_events: AtomicU64::new(0),
        clients_removed_for_slow: AtomicU64::new(0),
    });

    let room_clone = room.clone();

    let actor_handle = spawn_local(async move {
        room_actor(
            document_id,
            command_rx,
            room_clone,
            shutdown_rx,
            idle_timeout,
        )
        .await;
    });

    RoomHandle {
        room,
        command_tx,
        actor_handle,
        shutdown_tx,
    }
}

async fn room_actor(
    document_id: String,
    mut command_rx: mpsc::Receiver<RoomCommand>,
    room_meta: Arc<Room>,
    mut shutdown_rx: oneshot::Receiver<()>,
    idle_timeout: Duration,
) {
    let mut clients: HashMap<String, mpsc::Sender<OutboundMessage>> = HashMap::new();
    let mut awareness_ids: HashMap<String, u64> = HashMap::new();
    let mut next_awareness_id: u64 = 1;

    let crdt = CrdtEngine::new();
    let mut awareness = Awareness::new(crdt.doc().read().unwrap().clone());

    let mut last_activity = Instant::now();
    let mut idle_check = interval(std::cmp::min(idle_timeout, Duration::from_millis(100)));

    println!("[Room {}] Actor started", document_id);

    loop {
        tokio::select! {
            Some(command) = command_rx.recv() => {
                last_activity = Instant::now();
                handle_command(
                    command,
                    &mut clients,
                    &mut awareness_ids,
                    &mut next_awareness_id,
                    &crdt,
                    &mut awareness,
                    &room_meta,
                    &document_id,
                )
                .await;
            }

            _ = idle_check.tick() => {
                if clients.is_empty() && last_activity.elapsed() > idle_timeout {
                    println!("[Room {}] Idle timeout - shutting down", document_id);
                    break;
                }
            }

            _ = &mut shutdown_rx => {
                println!("[Room {}] Shutdown signal received", document_id);
                break;
            }

            else => {
                println!("[Room {}] Command channel closed", document_id);
                break;
            }
        }
    }

    println!("[Room {}] Actor shutting down gracefully", document_id);
}

async fn handle_command(
    command: RoomCommand,
    clients: &mut HashMap<String, mpsc::Sender<OutboundMessage>>,
    awareness_ids: &mut HashMap<String, u64>,
    next_awareness_id: &mut u64,
    crdt: &CrdtEngine,
    awareness: &mut Awareness,
    room_meta: &Arc<Room>,
    document_id: &str,
) {
    match command {
        RoomCommand::Join { client_id, tx } => {
            println!("[Room {}] Client {} joined", document_id, client_id);

            let awareness_id = *next_awareness_id;
            *next_awareness_id += 1;
            awareness_ids.insert(client_id.clone(), awareness_id);

            clients.insert(client_id, tx);
            room_meta.client_count.store(clients.len(), Ordering::Relaxed);
        }

        RoomCommand::Leave { client_id } => {
            println!("[Room {}] Client {} left", document_id, client_id);

            if let Some(awareness_id) = awareness_ids.remove(&client_id) {
                let _ = awareness.remove_state(awareness_id);
                
                if let Ok(update) = awareness.update() {
                    let update_bytes = Bytes::from(update.encode_v1());
                    broadcast_to_clients(
                        clients,
                        &client_id,
                        OutboundMessage::Awareness {
                            document_id: document_id.to_string(),
                            update: update_bytes,
                        },
                        room_meta,
                    )
                    .await;
                }
            }

            clients.remove(&client_id);
            room_meta.client_count.store(clients.len(), Ordering::Relaxed);
        }

        RoomCommand::ApplyUpdate { client_id, update } => {
            room_meta.updates_received.fetch_add(1, Ordering::Relaxed);

            crdt.apply_update(&update);
            
            broadcast_to_clients(
                clients,
                &client_id,
                OutboundMessage::Update {
                    document_id: document_id.to_string(),
                    update,
                },
                room_meta,
            )
            .await;
        }

        RoomCommand::RequestSync {
            client_id,
            state_vector,
            reply_to,
        } => {
            println!("[Room {}] Sync request from {}", document_id, client_id);

            let diff = crdt.encode_diff(&state_vector);
            let _ = reply_to.send(Bytes::from(diff));
        }

        RoomCommand::SetAwareness { client_id, state } => {
            room_meta.awareness_events.fetch_add(1, Ordering::Relaxed);

            let _awareness_id = match awareness_ids.get(&client_id) {
                Some(&id) => id,
                None => {
                    eprintln!(
                        "[Room {}] Awareness update from unknown client {}",
                        document_id, client_id
                    );
                    return;
                }
            };

            awareness.set_local_state(state);
            
            if let Ok(update) = awareness.update() {
                let update_bytes = Bytes::from(update.encode_v1());
                broadcast_to_clients(
                    clients,
                    &client_id,
                    OutboundMessage::Awareness {
                        document_id: document_id.to_string(),
                        update: update_bytes,
                    },
                    room_meta,
                )
                .await;
            }
        }

        RoomCommand::Shutdown => {
            println!("[Room {}] Explicit shutdown command", document_id);
        }
    }
}

async fn broadcast_to_clients(
    clients: &mut HashMap<String, mpsc::Sender<OutboundMessage>>,
    sender_id: &str,
    message: OutboundMessage,
    room_meta: &Arc<Room>,
) {
    let mut failed_clients = Vec::new();

    for (client_id, client_tx) in clients.iter() {
        if client_id == sender_id {
            continue;
        }

        match client_tx.try_send(message.clone()) {
            Ok(_) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                eprintln!("Client {} is too slow, removing", client_id);
                failed_clients.push(client_id.clone());
                room_meta.clients_removed_for_slow.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                println!("Client {} disconnected", client_id);
                failed_clients.push(client_id.clone());
            }
        }
    }

    for client_id in failed_clients {
        clients.remove(&client_id);
        room_meta.client_count.store(clients.len(), Ordering::Relaxed);
    }
}

