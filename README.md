# 🗳️ Project 26: Classroom Live E-Voting & Polling System

A high-concurrency, fault-tolerant classroom live polling and e-voting system designed with **FastAPI**, **WebSockets**, and **Server-Sent Events (SSE)**. Built to easily handle synchronized bursts of 200+ concurrent student voters over lossy campus Wi-Fi networks without socket starvation, server crashes, or duplicate votes.

---

## ⚡ Quick Start

### 1. Install Dependencies
Make sure you have **Python 3.8+** installed:
```bash
pip install -r requirements.txt
```

### 2. Start the FastAPI Server
```bash
python server.py
```
*(By default, the server binds to `http://localhost:3000` on all network interfaces).*

### 3. Open the User Interfaces
Once the server is running, open the following URLs in your browser:

| Interface | URL | Description |
|---|---|---|
| **🏠 Portal Hub** | [http://localhost:3000](http://localhost:3000) | Landing page with links to all portals |
| **➕ Create Poll** | [http://localhost:3000/create.html](http://localhost:3000/create.html) | Instructor UI to create questions and switch active polls |
| **📱 Student Voter UI** | [http://localhost:3000/voter.html](http://localhost:3000/voter.html) | Mobile-responsive voter interface with instant WebSocket ACKs |
| **📽️ Classroom Projector** | [http://localhost:3000/projector.html](http://localhost:3000/projector.html) | Real-time live bar telemetry streamed via SSE |
| **📖 API Documentation** | [http://localhost:3000/docs](http://localhost:3000/docs) | Interactive Swagger/OpenAPI documentation |

---

## 🏛️ System Architecture

```
                 ┌─────────────────────────────────────────────────────────────┐
                 │                 Campus Wi-Fi (Lossy Channel)                │
                 └───────▲────────────────────────────▲────────────────────────┘
                         │                            │
   200+ Student Voters   │ WebSockets (Full Duplex)   │ SSE (text/event-stream)
  (A/B/C/D + UUID + Token)│ [Bidirectional ACKs]       │ [Telemetry Broadcast]
                         ▼                            ▼
                 ┌─────────────────────────────────────────────────────────────┐
                 │                 Python FastAPI Server (Uvicorn)             │
                 │ ─────────────────────────────────────────────────────────── │
                 │ 1. Idempotency Gate (Set: poll_id + voter_token, vote_id)    │
                 │    ├─ New: Tally++ & Instant ACK_ACCEPTED                   │
                 │    └─ Duplicate: Skip increment & ACK_DUPLICATE_IGNORED     │
                 │ 2. In-Memory FIFO Queue (deque) & Multi-Poll Store (dict)   │
                 │ 3. Background Micro-Batcher (Decoupled SSE Timer @ 100ms)   │
                 └─────────────────────────────────────────────────────────────┘
                                                      │
                                                      ▼
                                       ┌──────────────────────────────┐
                                       │ Classroom Projector Screen   │
                                       │ Real-Time Live Bar Telemetry │
                                       └──────────────────────────────┘
```

### 🔑 Key Network & System Design Highlights
- **100% In-Memory RAM Architecture:** Pure in-memory state using Python `set`, `dict`, and `collections.deque` for sub-millisecond execution with zero disk/database I/O bottlenecks.
- **Strictly-Once Semantics (Atomic Idempotency):** In-memory $O(1)$ set lookup on composite keys `(poll_id:voter_token)` and `vote_id`. Check-and-insert runs synchronously on the asyncio event loop without `await` points, guaranteeing atomic deduplication.
- **Asymmetric Protocol Separation:**
  - **Student Voters:** Full-duplex **WebSockets (`ws`)** with exponential backoff retry and immediate application-layer ACKs.
  - **Classroom Projector:** **Server-Sent Events (`text/event-stream`)** for lightweight, unidirectional telemetry.
- **100ms Micro-Batching & Back-Pressure:** Decouples high-volume incoming vote bursts from telemetry broadcasts using bounded client queues and a 100ms flush timer.
- **Dual-Stack Socket Support:** Binds an `AF_INET6` dual-stack socket with IPv4 mapping enabled to eliminate Windows `localhost` DNS resolution fallbacks.

---

## 📁 Repository Structure

```text
├── server.py                  # FastAPI + WebSockets + SSE + In-Memory Engine
├── requirements.txt           # Python dependencies (fastapi, uvicorn)
├── .gitignore                 # Standard Python gitignore rules
├── README.md                  # Project overview and run guide
└── public/
    ├── index.html             # Role selection landing hub
    ├── create.html            # Instructor portal: Create & activate classroom polls
    ├── voter.html             # Student voter UI (mobile-first, exponential backoff)
    └── projector.html         # Classroom projector dashboard with live bar animations
```

---

## 🧪 Testing & Evaluation Suite

### 1,000 and 10,000 simultaneous WebSocket connections

Start the server, then run either benchmark in another terminal:

```bash
pip install websockets
python tests/load_test_1000.py --output tests/benchmark_1000_results.json
python tests/load_test_10000.py --output tests/benchmark_10000_results.json
```

Each script creates and activates a new poll on the target server, holds all connections open until every client is ready, then releases all voters together. After an accepted vote, 10% of clients resend the identical vote. The test checks both acknowledgements and the server's final tally and exits nonzero on failure. Since it activates a new poll and generates substantial load, use a dedicated test server, not a live classroom. Override the server with `--server http://HOST:PORT`; use `--help` for timeout and connection ramp options. The 10,000-connection test requires enough open file descriptors and local network resources on both client and server.

The included result files record one run on an Apple M4 Mac (macOS 26.5.2, Python 3.14.2), with both client and server on the same machine. FastAPI 0.138.0, Uvicorn 0.49.0, and websockets 16.0 were used. Latency is the time from client send to acknowledgement, including client scheduling and both initial votes and retries. Vote-and-cleanup time also includes closing sockets. `queueLength` is cumulative across polls, and the active socket count is a snapshot while the server may still be processing disconnects.

| Concurrent connections | Accepted | Duplicate retries ignored | Errors | Mean ACK latency | p95 ACK latency |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 1,000 | 100 | 0 | 36.35 ms | 42.09 ms |
| 10,000 | 10,000 | 1,000 | 0 | 603.05 ms | 894.23 ms |

These are local single-run measurements, not a guarantee of capacity or latency over campus Wi-Fi. The retry test resends messages deliberately; it does not inject packet loss or test server crashes. No projector clients were connected during these runs.

---

## 📊 REST & WebSocket Endpoints

| Endpoint | Protocol | Purpose |
|---|---|---|
| `GET /` | HTTP | Landing hub |
| `GET /voter.html` | HTTP | Student voter UI |
| `GET /projector.html` | HTTP | Classroom projector display |
| `GET /create.html` | HTTP | Poll creation and activation UI |
| `GET /events` | HTTP (SSE) | Live telemetry event stream |
| `WS /` | WebSocket | Bidirectional vote casting & instant ACKs |
| `POST /api/polls` | HTTP (JSON) | Create a new poll with 2–6 custom choices |
| `GET /api/polls` | HTTP (JSON) | List all polls |
| `POST /api/polls/{id}/activate` | HTTP (JSON) | Switch active poll in real time |
| `GET /api/poll/active` | HTTP (JSON) | Fetch active poll details |
| `GET /api/metrics` | HTTP (JSON) | Live server metrics summary |
| `POST /api/reset` | HTTP (JSON) | Reset active poll state |
| `GET /docs` | HTTP (Swagger) | Interactive OpenAPI documentation |
