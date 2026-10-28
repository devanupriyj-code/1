"""Shared WebSocket load test. Run through load_test_1000.py or load_test_10000.py."""
import argparse
import asyncio
import json
import math
import random
import time
import urllib.request
import uuid
from pathlib import Path

import websockets


def request_json(url, data=None):
    body = json.dumps(data).encode() if data is not None else None
    request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    position = (len(values) - 1) * p
    lower = math.floor(position)
    upper = math.ceil(position)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


async def benchmark(count, args):
    base = args.server.rstrip("/")
    ws_url = base.replace("https://", "wss://", 1).replace("http://", "ws://", 1) + "/"
    # A fresh poll avoids resetting existing data. Creating it makes it the active poll.
    poll = await asyncio.to_thread(request_json, base + "/api/polls", {
        "question": f"Concurrency benchmark: {count} voters ({uuid.uuid4().hex[:8]})",
        "options": {"A": "A", "B": "B", "C": "C", "D": "D"},
    })
    poll_id = poll["poll"]["poll_id"]
    start_vote = asyncio.Event()
    connections_ready = asyncio.Event()
    semaphore = asyncio.Semaphore(args.connect_parallelism)
    ready = 0
    statuses = {"ACK_ACCEPTED": 0, "ACK_DUPLICATE_IGNORED": 0, "errors": 0}
    errors = []
    latencies = []
    retry_count = count // 10
    connection_start = time.perf_counter()

    async def voter(index):
        nonlocal ready
        ws = None
        try:
            # Limit simultaneous handshakes, but retain every established socket.
            async with semaphore:
                ws = await websockets.connect(ws_url, open_timeout=args.timeout, ping_interval=None,
                                              close_timeout=2, max_queue=4)
                greeting = json.loads(await asyncio.wait_for(ws.recv(), args.timeout))
                if greeting.get("poll_id") != poll_id:
                    raise RuntimeError(f"Unexpected greeting: {greeting}")
                ready += 1
                if ready == count:
                    connections_ready.set()
            await start_vote.wait()
            payload = json.dumps({"type": "CAST_VOTE", "poll_id": poll_id,
                                      "voter_token": f"bot-{index}-{poll_id}",
                                      "vote_id": str(uuid.uuid4()),
                                      "choice": random.choice(["A", "B", "C", "D"])})

            async def send_vote(expected):
                started = time.perf_counter()
                await ws.send(payload)
                ack = json.loads(await asyncio.wait_for(ws.recv(), args.timeout))
                elapsed = (time.perf_counter() - started) * 1000
                status = ack.get("status")
                if status != expected:
                    raise RuntimeError(f"Expected {expected}, got {ack}")
                statuses[status] += 1
                latencies.append(elapsed)

            await send_vote("ACK_ACCEPTED")
            if index < retry_count:
                await send_vote("ACK_DUPLICATE_IGNORED")
        except Exception as exc:
            statuses["errors"] += 1
            if len(errors) < 10:
                errors.append(f"voter {index}: {type(exc).__name__}: {exc}")
        finally:
            if ws is not None:
                await ws.close()

    tasks = [asyncio.create_task(voter(i)) for i in range(count)]
    try:
        # Fail promptly if any connection fails, rather than hanging at the barrier.
        deadline = time.monotonic() + args.timeout
        while not connections_ready.is_set():
            if statuses["errors"] or time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.1)
        connection_seconds = time.perf_counter() - connection_start
        all_connected = ready == count
        vote_start = time.perf_counter()
        if all_connected:
            start_vote.set()
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=args.timeout)
        else:
            errors.append(f"Only {ready}/{count} connections became ready")
    except asyncio.TimeoutError:
        errors.append("Voting or connection cleanup timed out")
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    vote_seconds = time.perf_counter() - vote_start
    metrics = await asyncio.to_thread(request_json, base + "/api/metrics")
    passed = (all_connected and not errors and statuses["errors"] == 0
              and statuses["ACK_ACCEPTED"] == count
              and statuses["ACK_DUPLICATE_IGNORED"] == retry_count
              and metrics["poll_id"] == poll_id and metrics["totalVotes"] == count
              and metrics["duplicateAttempts"] == retry_count
              and sum(metrics["tally"].values()) == count)
    return {"passed": passed, "server": base, "poll_id": poll_id,
            "requested_connections": count, "established_connections": ready,
            "connection_seconds": round(connection_seconds, 3),
            "vote_and_cleanup_seconds": round(vote_seconds, 3),
            "acknowledgements": statuses, "expected_duplicate_retries": retry_count,
            "latency_ms": {"samples": len(latencies), "min": min(latencies) if latencies else None,
                           "mean": sum(latencies) / len(latencies) if latencies else None,
                           "p50": percentile(latencies, .50), "p95": percentile(latencies, .95),
                           "p99": percentile(latencies, .99), "max": max(latencies) if latencies else None},
            "server_metrics": metrics, "errors": errors}


def main(count):
    parser = argparse.ArgumentParser(description=f"Test {count} concurrent WebSocket voters")
    parser.add_argument("--server", default="http://127.0.0.1:3000", help="HTTP URL of a running server")
    parser.add_argument("--timeout", type=float, default=180, help="Deadline in seconds per phase/operation")
    parser.add_argument("--connect-parallelism", type=int, default=200, help="Maximum simultaneous connection handshakes")
    parser.add_argument("--output", type=Path, help="Optional path for JSON results")
    args = parser.parse_args()
    if args.timeout <= 0 or args.connect_parallelism <= 0:
        parser.error("timeout and connect-parallelism must be positive")
    result = asyncio.run(benchmark(count, args))
    output = json.dumps(result, indent=2)
    print(output)
    if args.output:
        args.output.write_text(output + "\n", encoding="utf-8")
    raise SystemExit(0 if result["passed"] else 1)
