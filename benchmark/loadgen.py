"""A/B load harness for the WS vs SSE+POST signalling transports.

One process drives N virtual users against the benchmark server (benchmark.asgi)
over either transport. Each signal carries the sender's perf_counter timestamp;
the receiving client computes one-way latency off the same process clock, so the
numbers are real end-to-end signal-delivery latencies, not request round-trips.

Scenarios:
  pingpong  - users paired; each sends to its peer at --rate for --duration.
              Measures end-to-end A->server->B latency. (the headline metric)
  broadcast - all users join one room; user 0 sends recipient-less signals at
              --rate; everyone else receives. Measures server->client fan-out.
  firehose  - paired users send as fast as they can. Measures client->server
              throughput ceiling and where latency falls apart.
  idle      - open N connections, no traffic. Measures the cost of *holding* a
              connection (sample server RSS with --server-pid).

Examples:
  uvicorn benchmark.asgi:application --port 8000 &        # one server, both wires
  python -m benchmark.loadgen --transport ws  --scenario pingpong --users 50 --rate 20 --duration 20
  python -m benchmark.loadgen --transport sse --scenario pingpong --users 50 --rate 20 --duration 20

Fairness/reliability notes are in benchmark/README.md (use Postgres, raise
ulimit, run the server on a separate box for serious numbers).
"""
import argparse
import asyncio
import csv
import json
import os
import statistics
import time
from pathlib import Path

import httpx

try:
    import psutil
except ImportError:
    psutil = None


# ── one virtual user ──────────────────────────────────────────────────────

class Client:
    def __init__(self, idx, base_url):
        self.idx = idx
        self.base = base_url
        self.http = httpx.AsyncClient(base_url=base_url, timeout=30.0)
        self.channel_id = None
        self.peer_cid = None
        self.ready = asyncio.Event()
        self.latencies = []      # ms, recorded on the receiving side
        self.sent = 0
        self._task = None

    async def login(self):
        await self.http.get("/login/")
        token = self.http.cookies.get("csrftoken")
        r = await self.http.post("/login/", data={
            "csrfmiddlewaretoken": token,
            "screen_name": f"b{self.idx}",
            "name": f"benchuser{self.idx}",
            "next": "/",
        }, headers={"Referer": self.base})
        if r.status_code not in (200, 302):
            raise RuntimeError(f"login {self.idx} failed: {r.status_code}")

    def _on_rtc(self, rtc):
        kind = rtc.get("type")
        if kind == "connect":
            self.channel_id = rtc["channel_name"]
            self.ready.set()
        elif kind == "signal":
            bench = rtc.get("signal", {}).get("bench")
            if bench:
                self.latencies.append((time.perf_counter() - bench["t"]) * 1000.0)

    def _signal_body(self, recipient):
        body = {"type": "signal", "sender": self.channel_id,
                "signal": {"bench": {"t": time.perf_counter(), "src": self.idx}}}
        if recipient:
            body["recipient"] = recipient
        return body

    async def close(self):
        if self._task:
            self._task.cancel()
        await self.http.aclose()


class SseClient(Client):
    lean = False  # True -> the /raw/ (no-middleware) endpoints for the perf ceiling

    async def connect(self):
        self._task = asyncio.create_task(self._reader())
        await asyncio.wait_for(self.ready.wait(), timeout=15)

    async def _reader(self):
        event, data = None, []
        async with self.http.stream("GET", "/sse/lobby/",
                                    headers={"Accept": "text/event-stream"}) as resp:
            async for line in resp.aiter_lines():
                if line == "":
                    if data and (event or "html") == "rtc":
                        self._on_rtc(json.loads("\n".join(data))["rtc"])
                    event, data = None, []
                elif line.startswith(":"):
                    continue
                elif line.startswith("event:"):
                    event = line[6:].strip()
                elif line.startswith("data:"):
                    data.append(line[5:].lstrip(" "))

    async def _post(self, path, body):
        await self.http.post(path, content=json.dumps(body), headers={
            "Content-Type": "application/json",
            "X-CSRFToken": self.http.cookies.get("csrftoken"),
            "Referer": self.base,
        })

    async def join(self, room):
        await self._post("/raw/join/" if self.lean else "/join", {"room": room})

    async def send_signal(self, recipient):
        await self._post("/raw/signal/" if self.lean else "/signals",
                         self._signal_body(recipient))
        self.sent += 1


class WsClient(Client):
    async def connect(self):
        from websockets.asyncio.client import connect
        cookies = "; ".join(f"{c.name}={c.value}" for c in self.http.cookies.jar)
        ws_url = self.base.replace("http", "ws", 1) + "/ws/lobby/"
        self.ws = await connect(ws_url, additional_headers={"Cookie": cookies})
        self._task = asyncio.create_task(self._reader())
        await asyncio.wait_for(self.ready.wait(), timeout=15)

    async def _reader(self):
        async for frame in self.ws:
            msg = json.loads(frame)
            if msg.get("event") == "rtc":
                self._on_rtc(json.loads(msg["data"])["rtc"])

    async def join(self, room):
        await self.ws.send(json.dumps({"cmd": "join", "room": room}))

    async def send_signal(self, recipient):
        await self.ws.send(json.dumps({"cmd": "signal", "rtc": self._signal_body(recipient)}))
        self.sent += 1

    async def close(self):
        if self._task:
            self._task.cancel()
        await self.ws.close()
        await self.http.aclose()


# ── resource sampling (optional) ──────────────────────────────────────────

class ResourceSampler:
    def __init__(self, pid):
        self.proc = psutil.Process(pid) if (pid and psutil) else None
        self.rss_mb = 0.0
        self.cpu = []
        self._stop = False

    async def run(self):
        if not self.proc:
            return
        self.proc.cpu_percent(None)  # prime
        while not self._stop:
            await asyncio.sleep(0.5)
            try:
                self.rss_mb = max(self.rss_mb, self.proc.memory_info().rss / 1e6)
                self.cpu.append(self.proc.cpu_percent(None))
            except psutil.Error:
                break

    def stop(self):
        self._stop = True


# ── scenarios ─────────────────────────────────────────────────────────────

async def _spawn(args):
    """Log in + connect every client, with bounded concurrency (sqlite-friendly)."""
    cls = WsClient if args.transport == "ws" else SseClient
    clients = [cls(i, args.url) for i in range(args.users)]
    if args.transport == "sse" and args.lean:
        for c in clients:
            c.lean = True
    sem = asyncio.Semaphore(args.connect_concurrency)

    async def bring_up(c):
        async with sem:
            await c.login()
            await c.connect()
    await asyncio.gather(*(bring_up(c) for c in clients))
    return clients


async def _scheduled_sender(send, rate, duration, recipient):
    """Fire `rate` msgs/s for `duration`s at fixed times (coordinated-omission safe)."""
    start = time.perf_counter()
    n = int(rate * duration)
    for k in range(n):
        target = start + k / rate
        now = time.perf_counter()
        if target > now:
            await asyncio.sleep(target - now)
        await send(recipient)


async def scenario_pingpong(clients, args):
    pairs = [(clients[i], clients[i + 1]) for i in range(0, len(clients) - 1, 2)]
    for a, b in pairs:
        a.peer_cid, b.peer_cid = b.channel_id, a.channel_id
    senders = []
    for a, b in pairs:
        senders.append(_scheduled_sender(a.send_signal, args.rate, args.duration, a.peer_cid))
        senders.append(_scheduled_sender(b.send_signal, args.rate, args.duration, b.peer_cid))
    await asyncio.gather(*senders)
    await asyncio.sleep(1.0)  # drain in-flight


async def scenario_broadcast(clients, args):
    await asyncio.gather(*(c.join("bench") for c in clients))
    await asyncio.sleep(0.5)
    await _scheduled_sender(clients[0].send_signal, args.rate, args.duration, None)
    await asyncio.sleep(1.0)


async def scenario_firehose(clients, args):
    pairs = [(clients[i], clients[i + 1]) for i in range(0, len(clients) - 1, 2)]
    for a, b in pairs:
        a.peer_cid, b.peer_cid = b.channel_id, a.channel_id
    deadline = time.perf_counter() + args.duration

    async def blast(c):
        while time.perf_counter() < deadline:
            await c.send_signal(c.peer_cid)
    await asyncio.gather(*(blast(c) for a, b in pairs for c in (a, b)))
    await asyncio.sleep(1.0)


async def scenario_idle(clients, args):
    await asyncio.sleep(args.duration)


SCENARIOS = {
    "pingpong": scenario_pingpong,
    "broadcast": scenario_broadcast,
    "firehose": scenario_firehose,
    "idle": scenario_idle,
}


# ── runner ────────────────────────────────────────────────────────────────

def _summary(clients, args, elapsed, sampler):
    lat = [x for c in clients for x in c.latencies]
    sent = sum(c.sent for c in clients)
    recv = len(lat)
    lat.sort()

    def pct(p):
        return lat[min(len(lat) - 1, int(p / 100 * len(lat)))] if lat else 0.0

    return {
        "transport": getattr(args, "transport_label", args.transport),
        "scenario": args.scenario,
        "users": args.users,
        "rate": args.rate,
        "duration": args.duration,
        "sent": sent,
        "recv": recv,
        "send_rate": round(sent / elapsed, 1) if elapsed else 0,
        "recv_rate": round(recv / elapsed, 1) if elapsed else 0,
        "p50_ms": round(pct(50), 2),
        "p95_ms": round(pct(95), 2),
        "p99_ms": round(pct(99), 2),
        "max_ms": round(max(lat), 2) if lat else 0,
        "mean_ms": round(statistics.fmean(lat), 2) if lat else 0,
        "rss_mb": round(sampler.rss_mb, 1),
        "cpu_pct": round(statistics.fmean(sampler.cpu), 1) if sampler.cpu else 0,
    }


def _write_csv(row, path):
    p = Path(path)
    new = not p.exists()
    with p.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row))
        if new:
            w.writeheader()
        w.writerow(row)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8000")
    ap.add_argument("--transport", choices=["ws", "sse"], required=True)
    ap.add_argument("--scenario", choices=list(SCENARIOS), default="pingpong")
    ap.add_argument("--users", type=int, default=10)
    ap.add_argument("--rate", type=float, default=10, help="msgs/sec per sender")
    ap.add_argument("--duration", type=float, default=10)
    ap.add_argument("--connect-concurrency", type=int, default=50)
    ap.add_argument("--server-pid", type=int, default=None)
    ap.add_argument("--lean", action="store_true",
                    help="SSE: POST to the /raw/ endpoints (no Django middleware)")
    ap.add_argument("--out", default="benchmark/results.csv")
    args = ap.parse_args()
    if args.lean:
        args.transport_label = "sse-lean"

    clients = await _spawn(args)
    sampler = ResourceSampler(args.server_pid)
    sample_task = asyncio.create_task(sampler.run())

    started = time.perf_counter()
    try:
        await SCENARIOS[args.scenario](clients, args)
    finally:
        elapsed = time.perf_counter() - started
        sampler.stop()
        await sample_task
        await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)

    row = _summary(clients, args, elapsed, sampler)
    _write_csv(row, args.out)
    width = max(len(k) for k in row)
    print(f"\n=== {row['transport'].upper()} / {args.scenario} ===")
    for k, v in row.items():
        print(f"  {k:<{width}}  {v}")


if __name__ == "__main__":
    asyncio.run(main())
