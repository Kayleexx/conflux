# Conflux

Conflux is a modular, actor-based real-time collaboration backend written in Rust.
It provides room-based CRDT synchronization, presence awareness, chat messaging, and JWT-authenticated WebSocket sessions.


---

## Features

* Room-based collaboration with automatic lifecycle management
* JWT authentication with per-session tracking
* Real-time CRDT synchronization using [Yrs (Yjs for Rust)](https://github.com/y-crdt/yrs)
* Awareness and presence state broadcasting (cursor, user state, etc.)
* Text chat messaging between clients
* Background cleanup for idle rooms
* Dashboard API to view room statistics
* Modular architecture with clear separation of components

---

## Architecture

```
        ┌──────────────────────────────────────┐
        │              Client A                │
        │  WebSocket ↔ CRDT / Chat / Awareness │
        └──────────────────────────────────────┘
                    ▲
                    │ ws://127.0.0.1:8080/ws/:document_id?token=<JWT>
                    ▼
        ┌──────────────────────────────────────┐
        │               Conflux                │
        │   Axum WebSocket Server + RoomMgr    │
        │   ├─ auth.rs         → JWT sessions  │
        │   ├─ room_manager.rs → cleanup logic │
        │   ├─ room.rs         → actor logic   │
        │   ├─ server.rs       → routing       │
        │   ├─ errors.rs       → unified errs  │
        │   ├─ crdt.rs         → Yrs backend   │
        │
        └──────────────────────────────────────┘
```

---

## Project Structure

```
conflux-workspace/
├── conflux/             # Core backend library
│   ├── src/
│   │   ├── auth.rs
│   │   ├── crdt.rs
│   │   ├── errors.rs
│   │   ├── room.rs
│   │   ├── room_manager.rs
│   │   ├── server.rs
│   │   └── lib.rs
│   └── Cargo.toml
│
├── confluxd/            # Binary executable
│   ├── src/main.rs
│   └── Cargo.toml
│
├── frontend/            # Optional Y.js client (future)
│   └── index.html
│
└── scripts/             # Utility and test scripts
    └── test_conflux.sh
```

---

## Endpoints

### `POST /login`

Authenticate a user and receive a JWT token.

**Request**

```json
{ "username": "kaylee" }
```

**Response**

```json
{ "token": "<JWT_TOKEN>" }
```

Each login issues a new token with a unique session ID (`sid`).

---

### `GET /dashboard`

Returns information about all active rooms and their current state.

**Response**

```json
[
  {
    "document_id": "testroom",
    "clients": 2,
    "updates": 14,
    "awareness_events": 5
  }
]
```

---

### `GET /ws/:document_id?token=<JWT>`

Connect to a collaborative room using a valid JWT token.

**Example**

```bash
websocat "ws://127.0.0.1:8080/ws/testroom?token=<JWT>"
```

**Send messages**

```json
{"type": "chat", "message": "Hello"}
{"type": "awareness", "data": {"cursor": 42}}
```

**Receive**

```json
{"Chat": {"document_id": "testroom", "from": "kaylee", "message": "Hello"}}
```

---

## Running the Server

```bash
cargo run -p confluxd
```

Expected output:

```
INFO confluxd: Conflux server running at ws://127.0.0.1:8080
```

---

## Security

* JWTs expire after 24 hours
* Each login generates a unique session ID (sid)
* Tokens can be revoked by rotating the SECRET_KEY
* Tokens are stateless (no database dependency)

---

## License

MIT License
Copyright (c) 2025
