# Conflux

Conflux is a modular, actor-based real-time collaboration engine written in Rust.
It provides room-based CRDT synchronization, awareness (presence), and text chat over WebSockets.

This project serves as a backend foundation for live collaborative applications like editors or shared whiteboards.

---

## Features

- Room-based collaboration with automatic lifecycle management
- Real-time synchronization via WebSockets
- CRDT document management using [Yrs (Yjs for Rust)](https://github.com/y-crdt/yrs)
- Awareness and presence updates (cursor, user state)
- Chat messaging within rooms
- Idle room cleanup via background task
- Modular architecture:
  - `room.rs` – actor loop for each room
  - `room_manager.rs` – manages and cleans up idle rooms
  - `server.rs` – Axum WebSocket server and message routing
- Designed for integration with browser-based Y.js clients

---

## Architecture


```
            ┌──────────────────────────────────────┐
            │              Client A                │
            │  WebSocket ↔ CRDT/Chat/Awareness     │
            └──────────────────────────────────────┘
                        ▲
                        │ ws://127.0.0.1:8080/ws/:document_id
                        ▼
            ┌──────────────────────────────────────┐
            │               Conflux                │
            │   Axum server + Room manager         │
            │   ├─ room_manager.rs                 │
            │   ├─ room.rs (actor loop)            │
            │   ├─ server.rs (WebSocket handler)   │
            └──────────────────────────────────────┘
                        ▲
                        │ Channels (mpsc)
                        ▼
            ┌──────────────────────────────────────┐
            │           CRDT Engine (Yrs)          │
            │      Handles updates and merges      │
            └──────────────────────────────────────┘
```

## Project Structure

```

conflux-workspace/
│
├── conflux/          # Core library (CRDT, rooms, networking)
│   ├── src/
│   │   ├── crdt.rs
│   │   ├── errors.rs
│   │   ├── room.rs
│   │   ├── room_manager.rs
│   │   ├── server.rs
│   │   └── lib.rs
│   └── Cargo.toml
│
├── confluxd/         # Binary server
│   ├── src/main.rs
│   └── Cargo.toml
│
└── frontend/         # (Optional) Web client using Y.js
└── index.html

````

---

## Running the Server

### 1. Start the backend

```bash
cargo run -p confluxd
````

Expected output:

```
INFO confluxd: Conflux server running at ws://127.0.0.1:8080
```

### 2. Connect via WebSocket

You can test with `websocat`:

```bash
websocat ws://127.0.0.1:8080/ws/testdoc
```

Example messages:

```json
{"type": "chat", "message": "Hello, world"}
{"type": "awareness", "data": {"cursor": 42}}
```

---

## Future Enhancements

* Prometheus metrics endpoint
* Distributed scaling with Redis or NATS

---

## License

MIT License
Copyright (c) 2025

