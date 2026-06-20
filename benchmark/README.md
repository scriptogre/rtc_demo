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
3. **The lean-vs-WS gap is mostly queuing, not per-request work.** Drilling in
   with a single-connection microbench (`/raw/signal/`, no SSE-delivery leg):

   | concurrency | lean POST round-trip p50 | min |
   |---|---|---|
   | 1  | 2.6 ms  | 2.2 ms |
   | 4  | 6.6 ms  | 3.1 ms |
   | 8  | 12.5 ms | 2.2 ms |
   | 16 | 22.7 ms | 2.2 ms |

   The **min stays ~2.2 ms at every concurrency** — the work per request is
   constant; the p50 rises because requests *wait*. uvicorn runs one event loop
   on one core, and a POST is far more loop-work than a WS frame (HTTP parse +
   ASGI `http.request`/`http.response` events + a thread-hopped session read +
   response build), so the POST path saturates that one core at a low rate while
   featherweight WS frames don't. On this 4-core box the load generator competes
   for the same cores too. Giving the server 4 workers pulled conc-8 from 12.5 →
   8.8 ms (only partly, because the single-process load-gen is then the limit) —
   confirming it's loop/core serialization, not per-message cost.

   So the **irreducible per-message protocol tax is ~1–1.5 ms** (one POST ≈ 2.2 ms
   of which the sqlite session read is ~0.45 ms; a WS frame ≈ 1 ms end-to-end).
   The scary "13 ms" was concurrency queuing amplified by a single event loop and
   by co-locating the load generator with the server — exactly why the rules above
   say run the load generator off-box and give the server cores. The real ceiling
   SSE+POST has versus WS is *signals/sec/core* (a POST costs more loop time than a
   frame), which only bites at rates far above bursty WebRTC signalling. Note the
   single-process design can't simply add uvicorn workers — the queues are
   in-process, so a POST and its peer's SSE stream must land in the same worker.

Takeaway for the thesis: SSE+POST is viable. Its irreducible cost is modest
per-request overhead (tens of ms at low rates) — and WebRTC signalling is bursty
and low-volume, so it lands nowhere near where that cost bites. Just don't
benchmark it through Django's sync middleware and call that "SSE+POST".

## Getting SSE+POST close to WS, and the HTTP/2 server shootout

The naive Django POST was ~13–38 ms. Three fixes (`benchmark/fast.py`, probed by
`benchmark/rtt.py` — a self-addressed round-trip) close almost all of it:

1. **Skip Django's middleware** (raw ASGI POST).
2. **Resolve identity once**, like a WS does: mint a secret capability token at
   SSE-connect, authenticate POSTs by an **O(1) in-memory token lookup** — no
   per-request DB read, no `sync_to_async` thread hop.
3. **uvloop + httptools** (`uvicorn[standard]`).

Self round-trip (this 4-core box, loopback, p50 ms):

| transport / server | conc=1 | conc=8 |
|---|---|---|
| **WS** (uvicorn h1.1)                  | 0.35 | 0.87 |
| SSE+POST optimized, **uvicorn h1.1**   | 1.37 | 7.8  |
| SSE+POST optimized, uvicorn h1.1 + TLS | 1.46 | —    |

Optimized SSE+POST is **~1.4 ms vs WS ~0.35 ms** — a ~1 ms gap, down from ~13 ms.
That residual is the irreducible cost of an HTTP request+response vs a single WS
frame; TLS adds only ~0.1 ms. (At conc=8 the gap widens because each POST is more
event-loop work than a frame and the single-process load-gen/httpx is itself a
bottleneck — a throughput-per-core story, not a floor.)

### Which HTTP/2 server is fastest?

ASGI servers that speak HTTP/2: **Granian** (Rust) and **Hypercorn** (asyncio or
uvloop worker). uvicorn and daphne do **not**. Plus a reverse proxy terminating
h2 in front of uvicorn (**Caddy** here; nginx/Envoy equivalent). All h2 needs TLS.

Same optimized SSE+POST round-trip, p50 ms:

| HTTP/2 server | conc=1 | conc=8 |
|---|---|---|
| **Caddy (h2) → uvicorn**        | **2.01** | **10.9** |
| **Granian** (native h2)         | 2.10 | 11.2 |
| Hypercorn (uvloop worker)       | 2.28 | 13.3 |
| Hypercorn (asyncio worker)      | 2.27 | 13.4 |
| *(uvicorn h1.1, for reference)* | 1.37 | 7.8  |

**Findings:**
- **HTTP/2 does not lower per-message latency** — it adds ~0.6 ms of framing
  overhead over HTTP/1.1 (and that's not TLS; TLS alone is ~0.1 ms). On a fast
  link, h1.1 wins on raw latency.
- Among h2 servers, **Caddy→uvicorn ≈ Granian** are fastest; **Hypercorn is
  slowest**, and its uvloop worker doesn't help. Caddy edges it because the
  upstream is uvicorn (the fastest h1 server) behind Caddy's efficient Go h2.
- **HTTP/2's real value for SSE+POST is operational, not latency:** one
  multiplexed connection carries the EventSource stream *and* the POSTs (vs two
  on h1.1, and browsers cap ~6 conns/host), no head-of-line blocking across a
  client's concurrent POSTs, and — with HTTP/3/QUIC (Caddy serves it) —
  resilience to packet loss on real networks, which loopback can't show.

**Recommendation:** for lowest signalling latency, uvicorn (uvloop+httptools) on
HTTP/1.1. When you want h2's connection multiplexing for real browsers, put Caddy
(h2/h3) in front of uvicorn, or use Granian for a single native binary — both
cost ~0.6 ms vs h1.1, which is irrelevant for bursty WebRTC signalling.

To reproduce the h2 runs you need a cert and the servers:
```bash
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 1 -nodes -subj "/CN=127.0.0.1"
openssl pkcs8 -topk8 -nocrypt -in key.pem -out key8.pem            # granian wants PKCS#8
uvicorn  benchmark.asgi:application --port 8000 --loop uvloop --http httptools          # h1.1
granian  --interface asgi --http 2 --port 8444 --ssl-certificate cert.pem --ssl-keyfile key8.pem benchmark.asgi:application
hypercorn benchmark.asgi:application --bind 127.0.0.1:8443 --certfile cert.pem --keyfile key.pem
python -m benchmark.rtt --transport sse-fast --url https://127.0.0.1:8444 --conc 1
```

## Does the gap survive a real network?

Loopback has ~0 RTT, which makes a 1 ms server-side difference look enormous. To
see real-world behaviour, `benchmark/netsim.py` puts a propagation delay in the
path (a transparent L4 relay; ~50 ms RTT here) and `rtt.py --burst K` models a
call-setup burst of K signals (SDP + ICE) fired at once. Self round-trip, p50:

| | loopback (~0 RTT) | **+50 ms RTT** |
|---|---|---|
| WS, per message            | 0.38 | **52.0** |
| SSE+POST h1.1, per message | 1.44 | **53.7** |
| SSE+POST h2, per message   | 2.07 | **54.9** |
| WS, call-setup burst×10            | 1.1  | 53.0 |
| SSE+POST h1.1, burst×10            | 18.6 | 66.3 |
| SSE+POST h2,   burst×10            | 12.7 | 66.5 |

**Findings:**
- **Per-message latency: the gap collapses.** Loopback 0.38 vs 1.44 ms looks like
  4×; under a real 50 ms RTT it's 52.0 vs 53.7 ms — a ~3% difference, imperceptible.
  The transport no longer decides latency; the network does. WS does **not**
  dominate latency in real-world usage for this workload.
- **Call-setup burst: a residual ~13 ms** (66 vs 53 ms), because 10 POSTs cost
  ~13 ms of serialized per-request server work where 10 frames cost ~0. It happens
  once per call and is humanly invisible next to RTT. (h1.1 and h2 are equal here
  once connections are warm — h2's win is connection *count*, not this latency.)

**Caveat — what this does NOT cover:** the proxy models propagation delay only,
not **packet loss** (a byte relay can't drop packets without breaking TCP, and
this container has no `netem` kernel module). Loss is the one regime where the
transport genuinely matters: on a single connection (WS, or h2 multiplexing the
stream + POSTs) a lost segment head-of-line-blocks everything until retransmit;
h1.1's separate stream/POST connections isolate it; HTTP/3 (QUIC) avoids TCP HOL
per-stream. To test that you need real `netem` (`tc qdisc add dev <if> root netem
delay 25ms loss 1%`) on a host where the module is available, or two real boxes.

**Bottom line:** over a real network, optimized SSE+POST is within a couple ms of
WebSocket for signalling — indistinguishable to users. WS's real edges are
throughput-per-core at high message rates and behaviour under packet loss,
neither of which bursty WebRTC signalling exercises.

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
