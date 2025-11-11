use crate::room::{spawn_room, RoomHandle};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

struct Entry {
    handle: Arc<RoomHandle>,
    last_used: Instant,
}

pub struct RoomManager {
    rooms: Arc<Mutex<HashMap<String, Entry>>>,
    idle_timeout: Duration,
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
        let now = Instant::now();

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
        let now = Instant::now();
        let idle = self.idle_timeout;

        let to_remove: Vec<String> = guard
            .iter()
            .filter_map(|(k, v)| {
                if now.duration_since(v.last_used) > idle {
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
    pub async fn room_count(&self) -> usize {
        let guard = self.rooms.lock().await;
        guard.len()
    }
}
