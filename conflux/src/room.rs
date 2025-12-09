use bytes::Bytes;
use std::{
    collections::HashMap,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tokio::{
    runtime::Builder,
    sync::{mpsc, oneshot},
    task::{JoinHandle, spawn_local},
    time::interval,
};
use yrs::{sync::Awareness, updates::encoder::Encode};

use crate::crdt::CrdtEngine;

type RoomTask = Box<dyn FnOnce() + Send + 'static>;

static ROOM_EXECUTOR: OnceLock<mpsc::Sender<RoomTask>> = OnceLock::new();

fn init_room_executor() -> mpsc::Sender<RoomTask> {
    if let Some(tx) = ROOM_EXECUTOR.get() {
        return tx.clone();
    }

    let (tx, mut rx) = mpsc::channel::<RoomTask>(128);

    thread::spawn(move || {
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to build local room runtime");

        let local = tokio::task::LocalSet::new();

        rt.block_on(local.run_until(async move {
            while let Some(task) = rx.recv().await {
                (task)();
            }
        }));
    });

    ROOM_EXECUTOR.set(tx.clone()).ok();
    tx
}

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
        tx: mpsc::Sender<OutboundMessage>,
    },
    Leave {
        client_id: String,
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
        state: serde_json::Value,
    },
    Chat {
        client_id: String,
        message: String,
    },
    Shutdown,
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum OutboundMessage {
    Update {
        document_id: String,
        update: Bytes,
    },
    Awareness {
        document_id: String,
        update: Bytes,
    },
    System(String),
    Chat {
        document_id: String,
        from: String,
        message: String,
    },
}

struct RoomContext<'a> {
    clients: &'a mut HashMap<String, mpsc::Sender<OutboundMessage>>,
    awareness_ids: &'a mut HashMap<String, u64>,
    next_awareness_id: &'a mut u64,
    crdt: &'a CrdtEngine,
    awareness: &'a mut Awareness,
    room_meta: &'a Arc<Room>,
    document_id: &'a str,
}

pub struct RoomHandle {
    pub room: Arc<Room>,
    pub command_tx: mpsc::Sender<RoomCommand>,
    pub actor_handle: JoinHandle<()>,
    shutdown_tx: oneshot::Sender<()>,
}

impl Clone for RoomHandle {
    fn clone(&self) -> Self {
        Self {
            room: Arc::clone(&self.room),
            command_tx: self.command_tx.clone(),
            actor_handle: tokio::spawn(async {}),
            shutdown_tx: {
                let (tx, _rx) = oneshot::channel();
                tx
            },
        }
    }
}

impl RoomHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        let _ = self.actor_handle.await;
    }
}

pub fn spawn_room(document_id: String, idle_timeout: Duration) -> RoomHandle {
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
    let exec = init_room_executor();

    // This channel allows the spawned local task to signal completion
    let (done_tx, done_rx) = oneshot::channel::<()>();
    let doc_id_clone = document_id.clone();

    exec.try_send(Box::new(move || {
        spawn_local(async move {
            room_actor(
                doc_id_clone,
                command_rx,
                room_clone,
                shutdown_rx,
                idle_timeout,
            )
            .await;
            let _ = done_tx.send(());
        });
    }))
    .expect("Failed to schedule room task");

    let actor_handle = tokio::spawn(async move {
        let _ = done_rx.await;
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
                    &mut RoomContext {
                        clients: &mut clients,
                        awareness_ids: &mut awareness_ids,
                        next_awareness_id: &mut next_awareness_id,
                        crdt: &crdt,
                        awareness: &mut awareness,
                        room_meta: &room_meta,
                        document_id: &document_id,
                    },
                ).await;
            }

            _ = idle_check.tick() => {
                if clients.is_empty() && last_activity.elapsed() > idle_timeout {
                    println!("[Room {}] Idle timeout — shutting down", document_id);
                    break;
                }
            }

            _ = &mut shutdown_rx => {
                println!("[Room {}] Shutdown signal received", document_id);
                break;
            }

            else => break,
        }
    }
    println!("[Room {}] Actor shutdown complete", document_id);
}

async fn broadcast_to_clients(
    clients: &mut HashMap<String, mpsc::Sender<OutboundMessage>>,
    sender_id: &str,
    message: OutboundMessage,
    room_meta: &Arc<Room>,
) {
    let mut failed = Vec::new();

    for (cid, tx) in clients.iter() {
        if cid == sender_id {
            continue;
        }

        match tx.try_send(message.clone()) {
            Ok(_) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                eprintln!("Client {} is too slow, removing", cid);
                failed.push(cid.clone());
                room_meta
                    .clients_removed_for_slow
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                println!("Client {} disconnected", cid);
                failed.push(cid.clone());
            }
        }
    }

    for cid in failed {
        clients.remove(&cid);
        room_meta
            .client_count
            .store(clients.len(), Ordering::Relaxed);
    }
}
async fn handle_command(command: RoomCommand, ctx: &mut RoomContext<'_>) {
    match command {
        RoomCommand::Join { client_id, tx } => {
            println!("[Room {}] Client {} joined", ctx.document_id, client_id);
            let awareness_id = *ctx.next_awareness_id;
            *ctx.next_awareness_id += 1;
            ctx.awareness_ids.insert(client_id.clone(), awareness_id);
            ctx.clients.insert(client_id, tx);
            ctx.room_meta
                .client_count
                .store(ctx.clients.len(), Ordering::Relaxed);
        }

        RoomCommand::Leave { client_id } => {
            println!("[Room {}] Client {} left", ctx.document_id, client_id);

            if let Some(awareness_id) = ctx.awareness_ids.remove(&client_id) {
                ctx.awareness.remove_state(awareness_id);

                if let Ok(update) = ctx.awareness.update() {
                    let update_bytes = Bytes::from(update.encode_v1());
                    broadcast_to_clients(
                        ctx.clients,
                        &client_id,
                        OutboundMessage::Awareness {
                            document_id: ctx.document_id.to_string(),
                            update: update_bytes,
                        },
                        ctx.room_meta,
                    )
                    .await;
                }
            }

            ctx.clients.remove(&client_id);
            ctx.room_meta
                .client_count
                .store(ctx.clients.len(), Ordering::Relaxed);
        }

        RoomCommand::ApplyUpdate { client_id, update } => {
            ctx.room_meta
                .updates_received
                .fetch_add(1, Ordering::Relaxed);
            ctx.crdt.apply_update(&update);

            broadcast_to_clients(
                ctx.clients,
                &client_id,
                OutboundMessage::Update {
                    document_id: ctx.document_id.to_string(),
                    update,
                },
                ctx.room_meta,
            )
            .await;
        }

        RoomCommand::RequestSync {
            client_id,
            state_vector,
            reply_to,
        } => {
            println!("[Room {}] Sync request from {}", ctx.document_id, client_id);
            let diff = ctx.crdt.encode_diff(&state_vector);
            let _ = reply_to.send(Bytes::from(diff));
        }

        RoomCommand::SetAwareness { client_id, state } => {
            ctx.room_meta
                .awareness_events
                .fetch_add(1, Ordering::Relaxed);

            if let Some(&_id) = ctx.awareness_ids.get(&client_id) {
                let _ = ctx.awareness.set_local_state(state.clone());
                if let Ok(update) = ctx.awareness.update() {
                    let update_bytes = Bytes::from(update.encode_v1());
                    broadcast_to_clients(
                        ctx.clients,
                        &client_id,
                        OutboundMessage::Awareness {
                            document_id: ctx.document_id.to_string(),
                            update: update_bytes,
                        },
                        ctx.room_meta,
                    )
                    .await;
                }
            }
        }

        RoomCommand::Chat { client_id, message } => {
            println!(
                "[Room {}] Chat from {}: {}",
                ctx.document_id, client_id, message
            );

            let from_id = client_id.clone();
            broadcast_to_clients(
                ctx.clients,
                &client_id,
                OutboundMessage::Chat {
                    document_id: ctx.document_id.to_string(),
                    from: from_id,
                    message,
                },
                ctx.room_meta,
            )
            .await;
        }

        RoomCommand::Shutdown => {
            println!("[Room {}] Explicit shutdown command", ctx.document_id);
        }
    }
}
