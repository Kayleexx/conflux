use std::{sync::Arc, time::Duration};
use axum::{Router, serve};
use tokio::net::TcpListener;
use tracing::info;

use conflux::server::{create_router, AppState};
use conflux::room_manager::RoomManager;

#[tokio::main(flavor = "multi_thread")]
async fn main() {

    tracing_subscriber::fmt::init();
    
    let room_manager = Arc::new(RoomManager::new(Duration::from_secs(60)));
    {
        let mgr = Arc::clone(&room_manager);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            loop {
                ticker.tick().await;
                mgr.cleanup_idle_rooms().await;
            }
        });
    }

    let state = AppState {
        room_manager: Arc::clone(&room_manager),
    };

    let app: Router = create_router(state);
    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(addr).await.unwrap();

    info!(" Conflux server running at ws://{}", addr);

    serve(listener, app.into_make_service()).await.unwrap();
}
