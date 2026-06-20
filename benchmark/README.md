# Signalling benchmark — WebSocket vs SSE + POST

A transport-isolated A/B. Both wires run in **one process**, share rtc.sse's
in-process state and the same `_do_join/_do_signal/_do_hangup` handlers, and are
served by the same ASGI app — so the only variable is the transport itself
(WS frames vs an SSE stream + HTTP POSTs), not Redis-vs-dicts or daphne-vs-uvicorn.

```
benchmark/
  ws_consumer.py  raw ASGI websocket consumer; reuses rtc.sse verbatim
  asgi.py         combined app: Django (SSE+POST+login) for http, WS for websocket
  loadgen.py      the async load harness (drives either transport, writes CSV)
```

## Run

```bash
uv pip install -r benchmark/requirements.txt
python manage.py migrate

# one server serves BOTH transports:
uvicorn benchmark.asgi:application --host 127.0.0.1 --port 8000 &
SPID=$!

# same scenario over each wire (pass --server-pid for RSS/CPU sampling):
python -m benchmark.loadgen --transport ws  --scenario pingpong --users 50 --rate 20 --duration 30 --server-pid $SPID
python -m benchmark.loadgen --transport sse --scenario pingpong --users 50 --rate 20 --duration 30 --server-pid $SPID
```

Each run appends a row to `benchmark/results.csv` and prints a summary.

## Scenarios

| `--scenario` | what it stresses | key columns |
|---|---|---|
| `pingpong`  | end-to-end A→server→B latency (the headline) | `p50/p95/p99_ms` |
| `broadcast` | server→client fan-out (1 sender → N receivers in a room) | `recv_rate`, `p*_ms` |
| `firehose`  | client→server throughput ceiling; where latency collapses | `send_rate`, `p99_ms`, `cpu_pct` |
| `idle`      | cost of *holding* N connections, no traffic | `rss_mb` |

## Reading the output

Latency is **one-way**: each signal carries the sender's `perf_counter` and the
receiver subtracts it off the same process clock. Sends are scheduled at fixed
times (coordinated-omission safe). `sent`/`recv` differ in `broadcast` by the
fan-out factor; if they differ elsewhere, messages were dropped or still
in-flight.

## Make the numbers trustworthy

- **Use Postgres, not sqlite.** The SSE+POST path reads the session per POST;
  on sqlite that serializes on the write lock and dominates the result. (sqlite
  is fine for a functional smoke run, useless for real numbers.)
- **Set `DEBUG = False`.** Dev mode adds per-request overhead and disables
  template caching — it taxes the HTTP/POST path unfairly.
- **Run the load generator on a different box** from the server, over a real
  NIC (+ TLS ideally). Loopback hides HTTP/1.1 POST overhead and TCP costs.
- **Raise `ulimit -n`.** An SSE user holds one stream plus a churn of short POST
  sockets; you'll exhaust fds/ephemeral ports before the server does otherwise.
- **Warm up, fix the duration, run ≥3 times, report the distribution** (the CSV
  gives percentiles, not just the mean).

## Where the SSE+POST cost actually goes

A naive run makes SSE+POST look terrible (≈40× WS latency). Most of that is
**not the protocol** — it's how the two paths hit Django. The WS consumer is raw
ASGI; every `/api/*` POST traverses Django's full middleware stack, and under an
async view each *sync* middleware (session, auth, CSRF) is `sync_to_async`-wrapped
with `thread_sensitive=True`, i.e. funnelled onto **one thread**. cProfile of a
single POST shows ~16 thread-pool hand-offs per request; under load that one
thread saturates and latency grows with concurrency.

The `--lean` mode (POST to `/raw/*`, same per-request work minus the middleware)
isolates it. Indicative numbers (DEBUG=0, **sqlite**, loopback, single box,
`pingpong` @ 10 msg/s/user — treat as ratios, not absolutes):

| transport | 8 users p50 / CPU | 24 users p50 / CPU |
|---|---|---|
| WS (raw ASGI)            | ~1 ms  / ~2%   | ~2 ms   / ~4%        |
| SSE, lean POST           | ~13 ms / ~12%  | ~25 ms  / ~35%       |
| SSE, full Django stack   | ~38 ms / ~62%  | ~113 ms / ~106% (1 core maxed) |

Reading it:

1. **In-process core is ~1 ms** — the queue + `_do_*` fan-out is cheap (WS shows it).
2. **Django's middleware pipeline is the bulk** of the SSE+POST cost (full→lean is
   ~3× latency, ~5× CPU). This is removable: a native-async stack with async
   middleware (Starlette/FastAPI — cf. `scriptogre/hyperspace`) doesn't pay it.
3. **The lean-vs-WS gap (~12 ms) is the genuine per-request HTTP cost** — request
   parse + a per-request session read (on sqlite here) + response build, versus a
   WS frame on an already-authenticated connection. Postgres + a pooled/cookie
   session + keepalive shrink it; it never reaches WS levels, and that gap *is*
   the protocol's price.

Takeaway for the thesis: SSE+POST is viable. Its irreducible cost is modest
per-request overhead (tens of ms at low rates) — and WebRTC signalling is bursty
and low-volume, so it lands nowhere near where that cost bites. Just don't
benchmark it through Django's sync middleware and call that "SSE+POST".

## What this does and does NOT compare

- **Does:** in-process WS vs in-process SSE+POST — a clean *transport* A/B. The
  expected finding is that WS amortizes auth/parse over the connection while
  every POST re-pays the HTTP request cost (routing, middleware, CSRF, session
  read); that asymmetry is the point, not a bug.
- **Does NOT:** benchmark Ken's full stack on `main` (Channels + Redis channel
  layer + `channels_presence` + client heartbeat). That's an *architecture*
  comparison; run it separately and label it as such — don't conflate the two.
- Both sides are single-process by design. Compare 1 process vs 1 process, or
  state the topology; Redis-backed WS scales out differently.
