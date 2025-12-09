use crate::room::{RoomHandle, spawn_room};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

struct Entry {
    handle: Arc<RoomHandle>,
    last_used: DateTime<Utc>,
}

pub struct RoomManager {
    rooms: Arc<Mutex<HashMap<String, Entry>>>,
    idle_timeout: Duration,
}

#[derive(Serialize, Clone)]
pub struct RoomInfo {
    pub document_id: String,
    pub client_count: usize,
    pub last_used: String,
}

impl RoomManager {
    pub fn new(idle_timeout: Duration) -> Self {
        Self {
            rooms: Arc::new(Mutex::new(HashMap::new())),
            idle_timeout,
        }
    }

    pub async fn get_or_create_room(&self, document_id: &str) -> Arc<RoomHandle> {
        let mut guard = self.rooms.lock().await;
        let now = Utc::now();

        if let Some(entry) = guard.get_mut(document_id) {
            entry.last_used = now;
            return Arc::clone(&entry.handle);
        }

        let handle = spawn_room(document_id.to_string(), self.idle_timeout);
        println!("[RoomManager] Created new room '{}'", document_id);
        let handle = Arc::new(handle);
        guard.insert(
            document_id.to_string(),
            Entry {
                handle: Arc::clone(&handle),
                last_used: now,
            },
        );
        handle
    }

    pub async fn cleanup_idle_rooms(&self) {
        let mut guard = self.rooms.lock().await;
        let now = Utc::now();
        let idle = chrono::Duration::from_std(self.idle_timeout).unwrap();

        let to_remove: Vec<String> = guard
            .iter()
            .filter_map(|(k, v)| {
                if now - v.last_used > idle {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect();

        for k in to_remove {
            if let Some(entry) = guard.remove(&k) {
                let h = Arc::clone(&entry.handle);
                tokio::spawn(async {
                    let _ = Arc::try_unwrap(h).ok().map(|owned| owned.shutdown());
                });
            }
        }
    }

    pub async fn list_rooms(&self) -> Vec<RoomInfo> {
        let guard = self.rooms.lock().await;
        guard
            .iter()
            .map(|(id, entry)| RoomInfo {
                document_id: id.clone(),
                client_count: entry.handle.room.client_count(),
                last_used: entry.last_used.to_rfc3339(),
            })
            .collect()
    }
    pub async fn room_count(&self) -> usize {
        let guard = self.rooms.lock().await;
        guard.len()
    }
}
