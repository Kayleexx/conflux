# Conflux

Conflux is a modular, actor-based real-time collaboration backend written in Rust.  
It provides room-based CRDT synchronization, presence/awareness broadcasting, and text chat — all over WebSockets with JWT authentication.

It’s designed as the backend core for collaborative editors, shared boards, or multiplayer apps where multiple users edit or interact in real time.

---

## Features

- Room-based collaboration with automatic lifecycle management
- Real-time document synchronization using [Yrs (Yjs for Rust)](https://github.com/y-crdt/yrs)
- Awareness broadcasting (cursor, selection, etc.)
- Chat messages and text communication between clients
- JWT authentication and per-session tracking
- Dashboard API to list active rooms and their metrics
- Automatic cleanup for idle rooms
- Modular architecture split into `room`, `room_manager`, `auth`, and `server`

---

## Architecture Overview

```
    ┌────────────────────────────────────────┐
    │               Client A                 │
    │ WebSocket → send text / CRDT / cursor  │
    └────────────────────────────────────────┘
                 ▲
                 │ ws://127.0.0.1:8080/ws/:room?token=<JWT>
                 ▼
    ┌────────────────────────────────────────┐
    │               Conflux                  │
    │ Axum server + Room Manager + CRDT Core │
    │ ├── auth.rs        → JWT validation    │
    │ ├── room.rs        → per-room actor    │
    │ ├── room_manager.rs → cleanup, metrics │
    │ ├── server.rs      → WebSocket routing │
    │ └── crdt.rs        → Yrs document API  │
    └────────────────────────────────────────┘
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
└── README.md

````

---

## Endpoints

### `POST /login`

Authenticate and receive a JWT token.

**Request**
```json
{ "username": "kaylee" }
````

**Response**

```json
{ "token": "<JWT_TOKEN>" }
```

Each login creates a new session with a unique session ID (`sid`).

---

### `GET /dashboard`

Returns all active rooms and their current state.

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

Connect to a collaborative room via WebSocket.

**Example:**

```bash
websocat "ws://127.0.0.1:8080/ws/testroom?token=<JWT>"
```

---

## Sending Messages (Client → Server)

You can send three kinds of messages to the server:

### 1. Text / Chat

```json
{ "type": "chat", "message": "Hello everyone" }
```

→ Broadcasts to all clients in the same room:

```json
{ "Chat": { "document_id": "testroom", "from": "kaylee", "message": "Hello everyone" } }
```

---

### 2. Awareness (Presence)

```json
{ "type": "awareness", "data": { "cursor": 42 } }
```

→ Notifies all connected clients about your cursor or user state.

---

### 3. CRDT Updates

```json
{ "type": "update", "data": "<base64_encoded_update>" }
```

→ The CRDT engine merges the update into the shared document using Yrs.

---

### 4. Sync Request

```json
{ "type": "sync_request" }
```

→ Requests the latest document state from the server if the client missed updates.

---

## Running the Server

```bash
cargo run -p confluxd
```

Output:

```
INFO confluxd: Conflux server running at ws://127.0.0.1:8080
```

---

## Example Session

1. Start the server

   ```bash
   cargo run -p confluxd
   ```

2. Get a JWT

   ```bash
   curl -X POST http://127.0.0.1:8080/login \
     -H "Content-Type: application/json" \
     -d '{"username":"kaylee"}'
   ```

3. Connect with the token

   ```bash
   websocat "ws://127.0.0.1:8080/ws/testroom?token=<JWT>"
   ```

4. Send messages from the client:

   ```
   {"type": "chat", "message": "hi from client 1"}
   {"type": "awareness", "data": {"cursor": 101}}
   {"type": "sync_request"}
   ```

---

## Security

* JWT tokens expire after 24 hours
* Each login generates a unique session ID (`sid`)
* Tokens can be revoked by rotating the `SECRET_KEY`
* All state is ephemeral (no DB dependency)

---

## License

MIT License
Copyright (c) 2025 Kaylee



