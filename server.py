"""
=========================================================================================
PROJECT 26: E-VOTING / LIVE POLLING SYSTEM FOR CLASSROOMS
High-Concurrency In-Memory Architecture (Python FastAPI + WebSockets + SSE)
Dynamic Multi-Poll Creation + Real-Time Live Voting
=========================================================================================

Core Computer Networks Highlights:
1. Synchronized Burst Tolerance:
   - 200+ concurrent student voters submit votes within a narrow window (<1s).
   - A single asyncio event loop (like Node's) multiplexes every socket; there is no
     disk / DB I/O on the hot path, so each vote is processed in microseconds.
2. Idempotency & Fault Tolerance:
   - Handles Wi-Fi packet drops, delayed ACKs and client retransmissions.
   - In-memory hash sets record (poll_id:voter_token) and vote_id (UUID) so every vote
     is tallied strictly once.
   - The check-and-insert runs with NO `await` in between, so it is atomic with respect
     to the event loop: two concurrent retries can never both pass the check.
3. Asymmetric Duplex Architecture:
   - Student voters:  full-duplex WebSocket at ws://<host>:3000/ with instant ACKs.
   - Projector:       unidirectional Server-Sent Events at /events (text/event-stream).
4. Micro-Batched Telemetry:
   - SSE frames are emitted by a background task every 100 ms instead of once per vote,
     so a burst of 200 votes costs ~10 projector frames rather than 200.
5. Back-pressure:
   - Each SSE client has a bounded queue. A slow projector drops its oldest frame
     instead of growing server memory without limit.

Run:   python server.py            (dual-stack IPv4+IPv6 on port 3000; PORT env var overrides)
  or:  uvicorn server:app --host 0.0.0.0 --port 3000   (IPv4 only; use 127.0.0.1, not
       "localhost", from Windows clients or each connect stalls ~2 s)
=========================================================================================
"""

import asyncio
import json
import os
import secrets
import socket
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, Optional, Set

import uvicorn
from fastapi import FastAPI, Request, WebSocket
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

# =========================================================================================
# CONFIGURATION
# =========================================================================================
PORT = int(os.environ.get("PORT", 3000))
HOST = os.environ.get("HOST", "0.0.0.0")  # 0.0.0.0 so phones on campus Wi-Fi can reach it
PUBLIC_DIR = Path(__file__).resolve().parent / "public"

BATCH_INTERVAL_S = 0.100       # SSE micro-batch cadence (100 ms)
SSE_HEARTBEAT_S = 15.0         # keep-alive comment so proxies / NATs don't drop idle SSE
SSE_CLIENT_QUEUE_MAX = 64      # per-projector back-pressure bound
ALLOWED_OPTION_KEYS = ("A", "B", "C", "D", "E", "F")


def now_ms() -> int:
    return int(time.time() * 1000)


def to_json(payload: Dict[str, Any]) -> str:
    # Compact separators = fewer bytes on the wire per frame
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


# =========================================================================================
# IN-MEMORY DATA STORES (Zero DB I/O for Sub-Millisecond Execution)
# =========================================================================================
@dataclass
class Poll:
    poll_id: str
    question: str
    options: Dict[str, str]
    tally: Dict[str, int]
    total_votes: int = 0
    duplicate_attempts: int = 0
    created_at: int = field(default_factory=now_ms)

    def snapshot(self) -> Dict[str, Any]:
        """Wire format shared with the browser clients (camelCase kept for the JS UIs)."""
        return {
            "poll_id": self.poll_id,
            "question": self.question,
            "options": dict(self.options),
            "tally": dict(self.tally),
            "totalVotes": self.total_votes,
            "duplicateAttempts": self.duplicate_attempts,
        }


class AppState:
    def __init__(self) -> None:
        # All polls: poll_id -> Poll
        self.polls: Dict[str, Poll] = {}
        self.active_poll_id: Optional[str] = None

        # Idempotency sets (O(1) membership checks)
        self.processed_voter_tokens: Set[str] = set()  # f"{poll_id}:{voter_token}"
        self.processed_vote_uuids: Set[str] = set()    # vote_id (UUID)

        # FIFO queue buffering accepted votes (audit log of arrival order)
        self.vote_queue: Deque[Dict[str, Any]] = deque()

        # Connected clients
        self.sse_clients: Set[asyncio.Queue] = set()   # one bounded queue per projector
        self.ws_clients: Set[WebSocket] = set()        # every connected voter socket

        # Set when the tally changes; cleared by the micro-batch broadcaster
        self.tally_dirty: bool = False

    @property
    def active_poll(self) -> Optional[Poll]:
        if self.active_poll_id and self.active_poll_id in self.polls:
            return self.polls[self.active_poll_id]
        return None

    def add_poll(self, poll: Poll, activate: bool = True) -> None:
        self.polls[poll.poll_id] = poll
        if activate:
            self.active_poll_id = poll.poll_id
            self.tally_dirty = True

    def new_poll_id(self) -> str:
        while True:
            candidate = f"poll-{secrets.token_hex(3)}"
            if candidate not in self.polls:
                return candidate


state = AppState()


# =========================================================================================
# BROADCAST HELPERS
# =========================================================================================
def sse_frame(payload: Dict[str, Any]) -> str:
    return f"data: {to_json(payload)}\n\n"


def broadcast_sse(payload: Dict[str, Any]) -> None:
    """Fan a frame out to every projector queue without blocking the event loop."""
    frame = sse_frame(payload)
    for queue in list(state.sse_clients):
        try:
            queue.put_nowait(frame)
        except asyncio.QueueFull:
            # Slow consumer: drop the oldest frame so the newest tally still gets through
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            queue.put_nowait(frame)


def broadcast_poll_snapshot(poll: Poll) -> None:
    broadcast_sse({"type": "POLL_SNAPSHOT", "timestamp": now_ms(), "poll": poll.snapshot()})


async def broadcast_ws(payload: Dict[str, Any]) -> None:
    """Send one frame to every connected voter concurrently; prune dead sockets."""
    text = to_json(payload)
    clients = list(state.ws_clients)
    results = await asyncio.gather(*(ws.send_text(text) for ws in clients), return_exceptions=True)
    for ws, result in zip(clients, results):
        if isinstance(result, Exception):
            state.ws_clients.discard(ws)


async def announce_poll_change(poll: Poll) -> None:
    broadcast_poll_snapshot(poll)
    await broadcast_ws({
        "type": "POLL_CHANGED",
        "poll_id": poll.poll_id,
        "question": poll.question,
        "options": poll.options,
        "serverTime": now_ms(),
    })


# =========================================================================================
# BACKGROUND WORKER: Micro-batched SSE Broadcast (Every 100ms)
# Decouples high-volume incoming vote processing from outgoing telemetry frames
# =========================================================================================
async def tally_broadcaster() -> None:
    while True:
        await asyncio.sleep(BATCH_INTERVAL_S)
        # Only broadcast if votes arrived since the last tick and someone is watching
        if not state.tally_dirty or not state.sse_clients:
            continue
        poll = state.active_poll
        if poll is None:
            state.tally_dirty = False
            continue
        broadcast_sse({
            "type": "TALLY_UPDATE",
            "timestamp": now_ms(),
            "poll_id": poll.poll_id,
            "tally": dict(poll.tally),
            "totalVotes": poll.total_votes,
            "duplicateAttempts": poll.duplicate_attempts,
        })
        state.tally_dirty = False


@asynccontextmanager
async def lifespan(_app: FastAPI):
    worker = asyncio.create_task(tally_broadcaster())
    # ASCII-only banner: the Windows console (cp1252) cannot print emoji
    print("=" * 64)
    print(f"Project 26 E-Voting System (FastAPI) on http://localhost:{PORT}")
    print(f"  WebSocket (voters):  ws://localhost:{PORT}/")
    print(f"  SSE (projector):     http://localhost:{PORT}/events")
    print(f"  Poll creator UI:     http://localhost:{PORT}/create.html")
    print(f"  Voter UI:            http://localhost:{PORT}/voter.html")
    print(f"  Projector UI:        http://localhost:{PORT}/projector.html")
    print(f"  API docs (Swagger):  http://localhost:{PORT}/docs")
    print("=" * 64, flush=True)
    try:
        yield
    finally:
        worker.cancel()


app = FastAPI(title="Project 26: Classroom E-Voting / Live Polling", lifespan=lifespan)


# Return {"error": "..."} (the shape create.html expects) instead of FastAPI's default 422
@app.exception_handler(RequestValidationError)
async def validation_error_handler(_request: Request, exc: RequestValidationError):
    messages = []
    for err in exc.errors():
        msg = str(err.get("msg", "Invalid request"))
        messages.append(msg.removeprefix("Value error, "))
    return JSONResponse(status_code=400, content={"error": "; ".join(messages) or "Invalid request"})


# =========================================================================================
# REST API ENDPOINTS: POLL MANAGEMENT & METRICS
# =========================================================================================
class PollCreate(BaseModel):
    question: str = Field(..., max_length=300)
    options: Dict[str, str]

    @field_validator("question")
    @classmethod
    def question_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Question is required")
        return value

    @field_validator("options")
    @classmethod
    def validate_options(cls, value: Dict[str, str]) -> Dict[str, str]:
        cleaned: Dict[str, str] = {}
        for key, label in value.items():
            key = key.strip().upper()
            label = label.strip()
            if not label:
                continue
            if key not in ALLOWED_OPTION_KEYS:
                raise ValueError(f"Option keys must be one of {', '.join(ALLOWED_OPTION_KEYS)}")
            if len(label) > 200:
                raise ValueError(f"Option {key} is longer than 200 characters")
            cleaned[key] = label
        if not 2 <= len(cleaned) <= 6:
            raise ValueError("Provide between 2 and 6 non-empty options")
        return cleaned


@app.post("/api/polls", status_code=201)
async def create_poll(body: PollCreate):
    poll = Poll(
        poll_id=state.new_poll_id(),
        question=body.question,
        options=body.options,
        tally={key: 0 for key in body.options},
    )
    state.add_poll(poll, activate=True)
    # Push the new question to every projector (SSE) and voter phone (WebSocket) right away
    await announce_poll_change(poll)
    return {"status": "POLL_CREATED", "poll": poll.snapshot()}


@app.get("/api/polls")
async def list_polls():
    return {
        "activePollId": state.active_poll_id,
        "polls": [
            {
                "poll_id": p.poll_id,
                "question": p.question,
                "options": p.options,
                "totalVotes": p.total_votes,
                "isActive": p.poll_id == state.active_poll_id,
            }
            for p in state.polls.values()
        ],
    }


@app.post("/api/polls/{poll_id}/activate")
async def activate_poll(poll_id: str):
    poll = state.polls.get(poll_id)
    if poll is None:
        return JSONResponse(status_code=404, content={"error": "Poll not found"})
    state.active_poll_id = poll_id
    state.tally_dirty = True
    await announce_poll_change(poll)
    return {"status": "POLL_ACTIVATED", "poll_id": poll_id}


@app.get("/api/poll/active")
async def get_active_poll():
    poll = state.active_poll
    if poll is None:
        return {"poll": None, "message": "No active poll. Create one at /create.html"}
    return poll.snapshot()


@app.get("/api/metrics")
async def metrics():
    poll = state.active_poll
    return {
        "poll_id": poll.poll_id if poll else None,
        "totalVotes": poll.total_votes if poll else 0,
        "duplicateAttempts": poll.duplicate_attempts if poll else 0,
        "tally": poll.tally if poll else {},
        "queueLength": len(state.vote_queue),
        "activeSseClients": len(state.sse_clients),
        "activeWsClients": len(state.ws_clients),
        "totalPolls": len(state.polls),
    }


@app.post("/api/reset")
async def reset():
    """Zero the active poll and clear idempotency state (for repeat benchmark runs)."""
    poll = state.active_poll
    if poll:
        for key in poll.tally:
            poll.tally[key] = 0
        poll.total_votes = 0
        poll.duplicate_attempts = 0
        broadcast_poll_snapshot(poll)
    state.processed_voter_tokens.clear()
    state.processed_vote_uuids.clear()
    state.vote_queue.clear()
    state.tally_dirty = True
    return {"status": "RESET_OK", "message": "Poll state cleared"}


# =========================================================================================
# SERVER-SENT EVENTS (SSE) ENDPOINT: /events (Projector Screen Interface)
# =========================================================================================
@app.get("/events")
async def events():
    async def stream():
        queue: asyncio.Queue = asyncio.Queue(maxsize=SSE_CLIENT_QUEUE_MAX)
        state.sse_clients.add(queue)
        try:
            yield ": sse-connected\n\n"
            # Initial snapshot so a freshly opened projector renders immediately
            poll = state.active_poll
            if poll:
                yield sse_frame({
                    "type": "POLL_SNAPSHOT",
                    "timestamp": now_ms(),
                    "poll": poll.snapshot(),
                })
            else:
                yield sse_frame({
                    "type": "NO_ACTIVE_POLL",
                    "timestamp": now_ms(),
                    "message": "No active poll currently running",
                })
            while True:
                try:
                    yield await asyncio.wait_for(queue.get(), timeout=SSE_HEARTBEAT_S)
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
        finally:
            # Runs when the browser disconnects (Starlette cancels the generator)
            state.sse_clients.discard(queue)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",  # disable buffering if placed behind nginx
            "Access-Control-Allow-Origin": "*",
        },
    )


# =========================================================================================
# WEBSOCKET SERVER (Student Voter Interface - Bidirectional Low-Latency ACKs)
# =========================================================================================
def ack_error(error: str, vote_id: Optional[str] = None) -> Dict[str, Any]:
    return {"type": "ACK_ERROR", "vote_id": vote_id, "error": error, "timestamp": now_ms()}


def process_cast_vote(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate + apply one vote and build the ACK frame.

    Deliberately synchronous (no `await`): the idempotency check and the set insert
    below happen in one uninterrupted step on the event loop, so duplicates arriving
    at the same instant cannot both be counted.
    """
    poll_id = payload.get("poll_id")
    voter_token = payload.get("voter_token")
    vote_id = payload.get("vote_id")
    choice = payload.get("choice")

    if not all(isinstance(v, str) and v for v in (poll_id, voter_token, vote_id, choice)):
        return ack_error("MISSING_MANDATORY_FIELDS", vote_id if isinstance(vote_id, str) else None)

    poll = state.polls.get(poll_id)
    if poll is None:
        return ack_error("UNKNOWN_POLL", vote_id)

    if choice not in poll.options:
        return ack_error("INVALID_CHOICE_OPTION", vote_id)

    voter_key = f"{poll_id}:{voter_token}"

    # -------------------------------------------------------------------------------
    # IDEMPOTENCY CHECK (O(1) set lookup)
    # -------------------------------------------------------------------------------
    if voter_key in state.processed_voter_tokens or vote_id in state.processed_vote_uuids:
        # DUPLICATE (packet retransmission / multi-click): ACK it, but don't count it
        poll.duplicate_attempts += 1
        state.tally_dirty = True
        return {
            "type": "VOTE_ACK",
            "status": "ACK_DUPLICATE_IGNORED",
            "vote_id": vote_id,
            "poll_id": poll_id,
            "voter_token": voter_token,
            "timestamp": now_ms(),
        }

    # -------------------------------------------------------------------------------
    # NEW VALID VOTE: mark processed -> enqueue -> tally -> ACK
    # -------------------------------------------------------------------------------
    state.processed_voter_tokens.add(voter_key)
    state.processed_vote_uuids.add(vote_id)

    state.vote_queue.append({
        "poll_id": poll_id,
        "voter_token": voter_token,
        "vote_id": vote_id,
        "choice": choice,
        "receivedAt": now_ms(),
    })

    poll.tally[choice] += 1
    poll.total_votes += 1
    state.tally_dirty = True

    return {
        "type": "VOTE_ACK",
        "status": "ACK_ACCEPTED",
        "vote_id": vote_id,
        "poll_id": poll_id,
        "voter_token": voter_token,
        "choice": choice,
        "timestamp": now_ms(),
    }


@app.websocket("/")
async def voter_socket(ws: WebSocket):
    await ws.accept()
    state.ws_clients.add(ws)
    client = f"{ws.client.host}:{ws.client.port}" if ws.client else "unknown"
    try:
        poll = state.active_poll
        if poll:
            await ws.send_text(to_json({
                "type": "CONNECTION_READY",
                "poll_id": poll.poll_id,
                "question": poll.question,
                "options": poll.options,
                "serverTime": now_ms(),
            }))
        else:
            await ws.send_text(to_json({
                "type": "NO_ACTIVE_POLL",
                "message": "No active poll currently running. Please wait for the instructor to create one.",
                "serverTime": now_ms(),
            }))

        while True:
            message = await ws.receive()
            if message["type"] == "websocket.disconnect":
                break

            raw = message.get("text")
            if raw is None and message.get("bytes") is not None:
                raw = message["bytes"].decode("utf-8", errors="replace")

            try:
                payload = json.loads(raw or "")
                if not isinstance(payload, dict):
                    raise ValueError("payload must be a JSON object")
            except ValueError:
                await ws.send_text(to_json(ack_error("INVALID_JSON_PAYLOAD")))
                continue

            if payload.get("type") == "CAST_VOTE":
                await ws.send_text(to_json(process_cast_vote(payload)))
    except Exception as exc:  # socket reset mid-send, etc.
        print(f"WebSocket client error [{client}]: {exc!r}")
    finally:
        state.ws_clients.discard(ws)


# =========================================================================================
# STATIC FRONTEND (index.html, create.html, voter.html, projector.html)
# Mounted LAST so the "/" WebSocket route and the /api routes above take priority.
# =========================================================================================
app.mount("/", StaticFiles(directory=PUBLIC_DIR, html=True), name="public")


def make_listen_socket() -> socket.socket:
    """
    Bind a single dual-stack socket (IPv6 + IPv4-mapped) when listening on all interfaces.

    Why: uvicorn's host="0.0.0.0" is IPv4-only. Windows resolves "localhost" to ::1 first,
    gets refused, and retries for ~2 s before falling back to 127.0.0.1, which would add
    2 s to every voter/projector connect. Node's server.listen() is dual-stack by default;
    this matches it.
    """
    try:
        if HOST in ("0.0.0.0", "::") and socket.has_dualstack_ipv6():
            return socket.create_server(("::", PORT), family=socket.AF_INET6,
                                        dualstack_ipv6=True, backlog=2048)
        return socket.create_server((HOST, PORT), backlog=2048)
    except OSError as err:
        if err.errno == 10048 or getattr(err, "winerror", None) == 10048:
            print("\n" + "!" * 70)
            print(f"[ERROR] Port {PORT} is already in use by another process!")
            print(f"To free port {PORT} on Windows PowerShell, run:")
            print(f'  Get-NetTCPConnection -LocalPort {PORT} | ForEach-Object {{ Stop-Process -Id $_.OwningProcess -Force }}')
            print(f"\nOr specify a different port using an environment variable:")
            print(f'  $env:PORT="3001"; python server.py')
            print("!" * 70 + "\n", flush=True)
            sys.exit(1)
        raise


if __name__ == "__main__":
    # access_log off: logging 200+ requests per burst would itself add latency
    config = uvicorn.Config(app, access_log=False, log_level="info")
    uvicorn.Server(config).run(sockets=[make_listen_socket()])
