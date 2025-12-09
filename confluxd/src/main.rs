use std::{sync::Arc, time::Duration};

use axum::{Router, serve};
use clap::Parser;
use conflux::room_manager::RoomManager;
use conflux::server::{AppState, create_router};
use tokio::net::TcpListener;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "confluxd", about = "Conflux collaboration server")]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value = "8080")]
    port: u16,

    /// Host address to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Enable anonymous authentication (no signature verification)
    #[arg(long, default_value = "false")]
    anonymous: bool,

    /// Room idle timeout in seconds
    #[arg(long, default_value = "60")]
    idle_timeout: u64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    if args.anonymous {
        warn!("Anonymous authentication enabled - JWTs will not be signature-verified");
    }

    let room_manager = Arc::new(RoomManager::new(Duration::from_secs(args.idle_timeout)));
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
        anonymous_mode: args.anonymous,
    };

    let app: Router = create_router(state);
    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).await?;

    info!("Conflux server running at ws://{}", addr);

    serve(listener, app.into_make_service()).await?;
    Ok(())
}
