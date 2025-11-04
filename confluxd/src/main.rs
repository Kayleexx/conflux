use std::{collections::HashMap, sync::Arc};
use axum::{Router, serve};
use tokio::{net::TcpListener, task::LocalSet};
use tracing::info;
use conflux::server::{create_router, AppState};
use conflux::room::RoomHandle;
use tokio::sync::Mutex;

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = LocalSet::new();

    rt.block_on(local.run_until(async {
        tracing_subscriber::fmt::init();

        let state = AppState {
            rooms: Arc::new(Mutex::new(HashMap::<String, RoomHandle>::new())),
        };

        let app: Router = create_router(state);

        let addr = "127.0.0.1:8080";
        let listener = TcpListener::bind(addr).await.unwrap();
        info!("Conflux server running at ws://{}", addr);

        serve(listener, app.into_make_service()).await.unwrap();
    }));
}
