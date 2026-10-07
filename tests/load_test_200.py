import asyncio
import json
import os
import time
import random
import uuid
import sys
import urllib.request
from pathlib import Path

import websockets

"""
=============================================================================================
PROJECT 26: HIGH-CONCURRENCY BOT HARNESS (200 STUDENTS BURST TEST)
=============================================================================================
Networks Benchmarking Objectives:
1. Concurrency: Spawn 200 persistent WebSocket bot connections simultaneously.
2. Synchronized Fan-In Burst: Bots synchronize arrival and fire votes within a 1-second burst window.
3. Fault Tolerance & Idempotency: Intentionally simulate a 10% packet loss / retry scenario
   where 20 bots intentionally re-transmit their identical vote payload to verify:
   - Server registers exactly 200 total accepted votes.
   - Server catches exactly 20 duplicate retransmissions without double-counting.
4. Telemetry: Compute latency distributions (Min, Max, Mean, p50, p95, p99).

Usage:
    python tests/load_test_200.py                                  # ws://localhost:3000
    $env:SERVER_URI="ws://localhost:3100"; python tests/load_test_200.py   (PowerShell)
=============================================================================================
"""

SERVER_URI = os.environ.get("SERVER_URI", "ws://localhost:3000").rstrip("/")
HTTP_BASE = SERVER_URI.replace("wss://", "https://").replace("ws://", "http://")
TOTAL_VOTERS = 200
BURST_WINDOW_SECONDS = 1.0
DUPLICATE_RETRY_RATIO = 0.10  # 10% of voters retransmit to simulate lost ACK / Wi-Fi packet drops
RESULTS_FILE = Path(__file__).resolve().parent / "benchmark_results.json"

# Shared metrics collection
latencies_ms = []
ack_statuses = {"ACK_ACCEPTED": 0, "ACK_DUPLICATE_IGNORED": 0, "ERROR": 0}
retransmitted_count = 0


def ensure_active_poll():
    """Ensure an active poll exists on the server. If none exists, creates a benchmark poll via POST /api/polls."""
    req = urllib.request.Request(f"{HTTP_BASE}/api/poll/active")
    with urllib.request.urlopen(req, timeout=5) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        if data.get("poll_id"):
            return data["poll_id"]

    # Create a benchmark poll if server has no active poll
    poll_payload = json.dumps({
        "question": "Benchmark Poll: Layer-4 Protocol Reliability Test",
        "options": {
            "A": "UDP (User Datagram Protocol)",
            "B": "TCP (Transmission Control Protocol)",
            "C": "ICMP (Internet Control Message Protocol)",
            "D": "IGMP (Internet Group Management Protocol)"
        }
    }).encode("utf-8")
    create_req = urllib.request.Request(
        f"{HTTP_BASE}/api/polls",
        data=poll_payload,
        method="POST",
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(create_req, timeout=5) as resp:
        res = json.loads(resp.read().decode("utf-8"))
        return res["poll"]["poll_id"]


def reset_server_state():
    """Zero the active poll so repeated runs start clean (bot tokens are reused each run)."""
    ensure_active_poll()
    req = urllib.request.Request(f"{HTTP_BASE}/api/reset", method="POST", data=b"")
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))



async def simulate_student_voter(student_index: int, barrier: asyncio.Barrier):
    global retransmitted_count

    voter_token = f"student_bot_{student_index:04d}"
    vote_id = str(uuid.uuid4())

    try:
        async with websockets.connect(SERVER_URI, ping_interval=None) as websocket:
            # 1. Await initial CONNECTION_READY frame from server (carries the active poll)
            greeting_raw = await websocket.recv()
            greeting = json.loads(greeting_raw)
            assert greeting.get("type") == "CONNECTION_READY"
            poll_id = greeting["poll_id"]
            choice = random.choice(list(greeting["options"].keys()))

            # 2. Wait at barrier so all 200 bots establish sockets and burst together
            await barrier.wait()

            # 3. Add random jitter within the 1-second burst window
            await asyncio.sleep(random.uniform(0.0, BURST_WINDOW_SECONDS))

            payload = {
                "type": "CAST_VOTE",
                "poll_id": poll_id,
                "voter_token": voter_token,
                "vote_id": vote_id,
                "choice": choice
            }

            # 4. Transmit Initial Vote Frame & record RTT
            start_time = time.perf_counter()
            await websocket.send(json.dumps(payload))

            ack_raw = await websocket.recv()
            rtt_ms = (time.perf_counter() - start_time) * 1000.0
            latencies_ms.append(rtt_ms)

            ack = json.loads(ack_raw)
            status = ack.get("status")

            if status in ack_statuses:
                ack_statuses[status] += 1
            else:
                ack_statuses["ERROR"] += 1

            # 5. Fault Simulation: If selected in the 10% retry cohort, simulate Wi-Fi unacknowledged retry
            # Client re-sends the exact same vote_id and voter_token after a simulated network delay
            if student_index < int(TOTAL_VOTERS * DUPLICATE_RETRY_RATIO):
                retransmitted_count += 1
                await asyncio.sleep(random.uniform(0.05, 0.15))  # 50-150ms retransmit delay

                # Retransmit duplicate frame
                dup_start = time.perf_counter()
                await websocket.send(json.dumps(payload))
                dup_ack_raw = await websocket.recv()
                dup_rtt_ms = (time.perf_counter() - dup_start) * 1000.0
                latencies_ms.append(dup_rtt_ms)

                dup_ack = json.loads(dup_ack_raw)
                dup_status = dup_ack.get("status")

                if dup_status in ack_statuses:
                    ack_statuses[dup_status] += 1
                else:
                    ack_statuses["ERROR"] += 1

    except Exception as e:
        ack_statuses["ERROR"] += 1
        # A bot that dies before reaching the barrier would otherwise make the rest wait forever
        barrier.abort()
        print(f"[Bot {student_index}] Error encountered: {e!r}", file=sys.stderr)


def calculate_percentile(data, percentile):
    if not data:
        return 0.0
    k = (len(data) - 1) * (percentile / 100.0)
    f = int(k)
    c = min(f + 1, len(data) - 1)
    d0 = data[f] * (c - k)
    d1 = data[c] * (k - f)
    return d0 + d1


async def main():
    print("=" * 70)
    print("[*] PROJECT 26: 200 CONCURRENT STUDENT BOT LOAD TEST")
    print(f"Target: {SERVER_URI} | Burst Window: {BURST_WINDOW_SECONDS}s")
    print(f"Total Bots: {TOTAL_VOTERS} | Simulated Retry Ratio: {int(DUPLICATE_RETRY_RATIO * 100)}%")
    print("=" * 70)

    # Bot voter tokens are identical every run, so clear the previous run's idempotency state
    try:
        reset_server_state()
        print("[+] Server poll state reset via POST /api/reset")
    except Exception as exc:
        print(f"[!] Could not reset server state ({exc!r}); counts may include a previous run")

    # Barrier ensures all 200 connections are established before triggering the synchronized burst
    barrier = asyncio.Barrier(TOTAL_VOTERS)

    test_start = time.perf_counter()
    tasks = [simulate_student_voter(i, barrier) for i in range(TOTAL_VOTERS)]
    await asyncio.gather(*tasks)
    total_duration = time.perf_counter() - test_start

    # Sort latencies for percentiles calculation
    latencies_ms.sort()

    min_lat = min(latencies_ms) if latencies_ms else 0
    max_lat = max(latencies_ms) if latencies_ms else 0
    mean_lat = sum(latencies_ms) / len(latencies_ms) if latencies_ms else 0
    p50_lat = calculate_percentile(latencies_ms, 50)
    p95_lat = calculate_percentile(latencies_ms, 95)
    p99_lat = calculate_percentile(latencies_ms, 99)

    print("\n" + "=" * 70)
    print("[*] BENCHMARK RESULTS & METRICS SUMMARY")
    print("=" * 70)
    print(f"Total Execution Time:        {total_duration:.3f} s")
    print(f"Total Transmissions Handled: {len(latencies_ms)} frames")
    print(f"Unique Accepted Votes:       {ack_statuses['ACK_ACCEPTED']} (Expected: {TOTAL_VOTERS})")
    print(f"Duplicate Votes Ignored:     {ack_statuses['ACK_DUPLICATE_IGNORED']} (Expected: {retransmitted_count})")
    print(f"Socket / Protocol Errors:    {ack_statuses['ERROR']} (Expected: 0)")
    print("-" * 70)
    print(f"Min Response Latency:        {min_lat:.2f} ms")
    print(f"Mean Response Latency:       {mean_lat:.2f} ms")
    print(f"Median (p50) Latency:        {p50_lat:.2f} ms")
    print(f"95th Percentile (p95):       {p95_lat:.2f} ms")
    print(f"99th Percentile (p99):       {p99_lat:.2f} ms")
    print(f"Max Response Latency:        {max_lat:.2f} ms")
    print("=" * 70)

    # Verification checks
    is_idempotency_valid = (
        ack_statuses["ACK_ACCEPTED"] == TOTAL_VOTERS and
        ack_statuses["ACK_DUPLICATE_IGNORED"] == retransmitted_count and
        ack_statuses["ERROR"] == 0
    )

    if is_idempotency_valid:
        print("[SUCCESS] Strictly-once tally guarantee passed under burst!")
    else:
        print("[WARNING] Discrepancy detected in tally counts.")

    # Export metrics for plot_metrics.py
    results_export = {
        "timestamp": time.time(),
        "total_voters": TOTAL_VOTERS,
        "total_frames": len(latencies_ms),
        "accepted_votes": ack_statuses["ACK_ACCEPTED"],
        "duplicate_ignored": ack_statuses["ACK_DUPLICATE_IGNORED"],
        "errors": ack_statuses["ERROR"],
        "min_ms": min_lat,
        "mean_ms": mean_lat,
        "p50_ms": p50_lat,
        "p95_ms": p95_lat,
        "p99_ms": p99_lat,
        "max_ms": max_lat,
        "latencies_ms": latencies_ms
    }

    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(results_export, f, indent=2)
    print(f"[+] Raw benchmark telemetry saved to {RESULTS_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
