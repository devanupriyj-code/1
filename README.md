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
├── requirements.txt           # Python dependencies (fastapi, uvicorn, websockets, matplotlib, numpy)
├── .gitignore                 # Standard Python gitignore rules
├── public/
│   ├── index.html             # Role selection landing hub
│   ├── create.html            # Instructor portal: Create & activate classroom polls
│   ├── voter.html             # Student voter UI (mobile-first, exponential backoff)
│   └── projector.html         # Classroom projector dashboard with live bar animations
├── tests/
│   ├── load_test_200.py       # 200 concurrent bot benchmark harness (burst + 10% retries)
│   ├── plot_metrics.py        # Generates CDF and latency percentile breakdown plots
│   ├── benchmark_results.json # Raw benchmark telemetry data
│   └── benchmark_plots.png    # 300 DPI high-resolution evaluation figures
└── README.md                  # Project overview and run guide
```

---

## 🧪 Testing & Evaluation Suite

### 1. Run the 200-Student Burst Benchmark
In another terminal while `server.py` is running:
```bash
python tests/load_test_200.py
```
- Spawns **200 concurrent WebSocket bot connections** simultaneously.
- Synchronizes arrival using `asyncio.Barrier` and triggers a **fan-in burst within a 1-second window**.
- Simulates an intentional **10% unacknowledged client retry** scenario (20 bots re-transmit their vote payload).
- Verifies that the server registers **exactly 200 unique votes and 20 ignored duplicates**.

### 2. Generate Evaluation Plots
```bash
python tests/plot_metrics.py
```
Outputs `tests/benchmark_plots.png` containing:
1. Response Time Histogram with Mean and p95 overlays.
2. Latency Percentile Breakdown (Min, p50, Mean, p95, p99, Max).
3. Fault Tolerance & Idempotency Audit Summary.

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
| `POST /api/reset` | HTTP (JSON) | Reset active poll state for benchmark runs |
| `GET /docs` | HTTP (Swagger) | Interactive OpenAPI documentation |
