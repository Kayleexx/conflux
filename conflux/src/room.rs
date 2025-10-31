use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use tokio::sync::mpsc;
use crate::crdt::CrdtEngine;
use yrs::sync::Awareness;

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

    tokio::spawn(async move {
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
                println!("Received update from {}: {} bytes", client_id, update.len());
                
                crdt.apply_update(&update);
                
                broadcast_to_clients(
                    &mut clients,
                    &client_id,
                    OutboundMessage::Update {
                        document_id: document_id.clone(),
                        update,
                    }
                ).await;
            }
            
            RoomCommand::RequestSync { client_id, state_vector, reply_to } => {
                println!("Sync request from {}", client_id);
                let diff = crdt.encode_diff(&state_vector);
                let _ = reply_to.send(diff).await;
            }
            
            RoomCommand::SetAwareness { client_id, state } => {
                println!("Awareness update from {}: {:?}", client_id, state);
                
            }
        }
    }
    
    println!("Room {} actor shutting down", document_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_room_spawns() {
        let (cmd_tx, room) = spawn_room("test-doc".to_string());
        
        assert_eq!(room.document_id, "test-doc");
        assert_eq!(room.client_count(), 0);
        assert!(!cmd_tx.is_closed());
    }

    #[tokio::test]
    async fn test_join_and_leave() {
        let (cmd_tx, room) = spawn_room("test-doc".to_string());
        
        // Create two fake clients
        let (client_a_tx, _client_a_rx) = mpsc::channel(16);
        let (client_b_tx, _client_b_rx) = mpsc::channel(16);
        
        // Join both clients
        cmd_tx.send(RoomCommand::Join {
            client_id: "alice".to_string(),
            tx: client_a_tx,
        }).await.unwrap();
        
        cmd_tx.send(RoomCommand::Join {
            client_id: "bob".to_string(),
            tx: client_b_tx,
        }).await.unwrap();
        
        // Give actor time to process
        sleep(Duration::from_millis(10)).await;
        
        assert_eq!(room.client_count(), 2);
        
        // Leave one client
        cmd_tx.send(RoomCommand::Leave {
            client_id: "alice".to_string(),
        }).await.unwrap();
        
        sleep(Duration::from_millis(10)).await;
        
        assert_eq!(room.client_count(), 1);
    }

    #[tokio::test]
    async fn test_broadcast_updates() {
        let (cmd_tx, room) = spawn_room("test-doc".to_string());
        
        // Create two clients with channels to receive messages
        let (client_a_tx, mut client_a_rx) = mpsc::channel(16);
        let (client_b_tx, mut client_b_rx) = mpsc::channel(16);
        
        // Both clients join
        cmd_tx.send(RoomCommand::Join {
            client_id: "alice".to_string(),
            tx: client_a_tx,
        }).await.unwrap();
        
        cmd_tx.send(RoomCommand::Join {
            client_id: "bob".to_string(),
            tx: client_b_tx,
        }).await.unwrap();
        
        sleep(Duration::from_millis(10)).await;
        assert_eq!(room.client_count(), 2);
        
        // Alice sends an update
        let update_data = vec![1, 2, 3, 4, 5]; // Fake CRDT update bytes
        cmd_tx.send(RoomCommand::ApplyUpdate {
            client_id: "alice".to_string(),
            update: update_data.clone(),
        }).await.unwrap();
        
        // Bob should receive the update (but not Alice)
        let received = tokio::time::timeout(
            Duration::from_secs(1),
            client_b_rx.recv()
        ).await.expect("Timeout waiting for message").expect("Channel closed");
        
        match received {
            OutboundMessage::Update { document_id, update } => {
                assert_eq!(document_id, "test-doc");
                assert_eq!(update, update_data);
                println!("✅ Bob received the update!");
            }
            _ => panic!("Expected Update message"),
        }
        
        // Alice should NOT receive her own update
        let alice_received = tokio::time::timeout(
            Duration::from_millis(50),
            client_a_rx.recv()
        ).await;
        
        assert!(alice_received.is_err(), "Alice should not receive her own update");
        println!("✅ Alice correctly didn't receive echo");
    }
}