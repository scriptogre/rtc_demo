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

## Under packet loss (connection-level approximation)

`netsim.py <l> <u> <delay> <loss> <rto>` also models loss as an RTO-sized stall
that, crucially, holds the *one connection's* pipe — so it reproduces the
structural difference between single-connection transports and h1.1's split
connections. At +50ms RTT + 2% loss, RTO 200ms (p50 / p99 ms):

| | per-message | call-setup burst×10 |
|---|---|---|
| WS                | 52.1 / 252 | 52.9 / 254 (p90) |
| SSE+POST **h1.1** | 53.3 / 254 | **258 / 463** |
| SSE+POST **h2**   | 54.6 / 256 | 66.3 / 465 (p90) |

- **Per message: identical.** One message is one round-trip; a loss adds ~RTO to
  the unlucky ~2% for every transport equally. No transport advantage.
- **Bursts expose head-of-line blocking — and h2 wins for SSE+POST.** WS and h2
  use ONE connection: usually fast, occasionally a loss stalls the whole burst
  (bimodal tail). h1.1 spreads 10 POSTs over ~10 connections and must wait for
  the *slowest*, so with 2% loss something stalls most of the time → its p50
  jumps to ~258ms. Counterintuitively, **more connections hurt a synchronized
  burst.** So for SSE+POST under loss, run it over **HTTP/2** (one multiplexed
  connection), which tracks WS; HTTP/3/QUIC would do better still (per-stream
  loss recovery, no shared-connection HOL) but needs an h3 client to measure.

Honest limit: this is a connection-level approximation for sparse traffic, not
packet-level `netem` (no fast-retransmit, no congestion control, no QUIC). The
direction it shows is right; exact magnitudes need real `netem` or two boxes.

## Real-browser validation (Playwright)

`browser_rtt.py` drives headless Chromium's actual `EventSource`+`fetch` vs
`WebSocket` (same self-echo). It matches the httpx/websockets harness, so the
numbers aren't a client-library artifact:

| (p50) | loopback | +50ms RTT (+2% loss p99) |
|---|---|---|
| SSE+POST (browser) | 1.70 | 53.7  (p99 254) |
| WebSocket (browser)| 0.50 | 52.1  (p99 252) |

Same conclusion as the synthetic probe: a ~1 ms gap on loopback that vanishes
into RTT on a real link.

## Not testable in this sandbox

- **Two-machine / cross-region RTT** and **production canary/RUM** — need real
  hosts; the container is single-box with a locked-down network.
- **Packet-level `netem` loss** — the kernel `sch_netem` module isn't present
  here; the proxy approximation above stands in, with the caveat noted.
- **HTTP/3 client** — Caddy serves h3, but neither httpx nor Chromium-headless
  here drive h3 against a self-signed local cert, so QUIC-under-loss is argued,
  not measured.

## Throughput per core (measured — and a correction)

Server pinned to one core (`taskset -c 0`), load from the other cores, throughput
read from an authoritative server-side counter (`/fast/stats`).

This figure was corrected twice (the process matters):

1. First attempt (httpx/websockets load-gen) couldn't saturate the SSE core (52%);
   per-signal CPU at partial util gave a wrong ~214 µs / "3.5x".
2. A raw socket blaster (`rawblast.py`) saturated it → ~104 µs / "1.7x". But that
   still had uvicorn's **default per-request access logging on** — a `stderr`
   write + formatting *every POST*, which py-spy showed was ~half the cost. **WS
   never pays this** (it logs the handshake once, not each frame), so it was an
   unfair handicap.
3. Fair run (`--no-access-log --no-proxy-headers`; WS is unaffected, it has no
   per-message logging):

| full round-trip, 1 core saturated, no per-message logging | signals/s | per-signal |
|---|---|---|
| **WS**                              | 16,150 | ~60 µs |
| **SSE+POST** (POST in + SSE out)    | **11,702** | ~80 µs |
| *SSE+POST, ingestion only (one leg)* | 22,373 | ~44 µs |

**Corrected finding: the fair gap is ~1.35x (≈80 vs 60 µs/signal), not 1.7x and
certainly not 3.5x.** Per leg: the SSE *downlink* (server→client stream write) is
nearly frame-cheap (~36 µs); the POST *uplink* is the heavier leg (~44 µs) because
each is a full HTTP request. py-spy on the saturated POST path: only ~10% of CPU
is the actual signalling (`_do_signal`, json, our handler); ~90% is request
plumbing — event-loop scheduling across the per-request awaits (~18%), HTTP parse
+ scope construction (~11%), response serialize + completion (~11%), and the
removable access-log/proxy-headers (~halved throughput when on). The irreducible
remainder (~20 µs over a frame) is the request lifecycle: a fresh scope, extra
event-loop hops, parse+serialize — which a long-lived WS connection amortizes.

So the real limit, best case: **~11,700 signals/s/core vs WS ~16,000 — within
~1.35x.** That's ~1,170 WebRTC call-setups/sec/core, then silence — orders of
magnitude above what bursty signalling needs; the gap only bites for *sustained*
high-rate streams. Caveat: single-box; both are true saturation points; a Go/Rust
framework lowers both (the frame-vs-request ratio persists).

## Can SSE+POST get within ±10% of WS? (yes — what it takes)

All numbers below are fair (no per-message logging on either side).

**One signal per POST is ~1.35x WS on full round-trip (11.7k vs 16.1k) — just
outside ±10%.** The uplink POST is the heavier leg (~44 µs vs a ~30 µs frame leg);
the SSE downlink is already frame-cheap. The residual is the HTTP request
lifecycle (fresh scope, extra event-loop hops, parse+serialize) a long-lived
WS connection amortizes. A faster runtime (Go/Rust) lowers it but lowers WS too.

**The lever is amortization: put N signals in one POST** (a JSON array), which
pays the per-request cost once per N. Measured ingestion (bombardier, no logging,
signals/s = reqs/s x N, one core; WS full-round-trip baseline 16,150):

| signals per POST | uplink signals/s | full round-trip (uplink/N + ~36µs downlink) |
|---|---|---|
| 1  | 21,916 | ~11,700  (0.73x WS) |
| 2  | 38,402 | ~17,000  (~1.05x — within ±10%) |
| 3  | 52,919 | ~19,600  (1.2x) |
| 5  | 70,309 | ~22,000  (1.4x) |
| 10 | 100,132 | ~26,000 (1.6x) |

**±10% on the full round-trip is reached at ~2 signals/POST.** (Uplink-only — i.e.
ingestion — already beats WS at N=1, 1.36x, but a fair signal needs delivery too,
hence the round-trip column.) The deep reason: amortize the per-message transport
cost and both transports converge to the app-work floor (~11 µs/signal); the
frame-vs-request difference only exists *per message*.

**Is batching a FAIR comparison? No — read it carefully.** Batching isn't
something SSE+POST has that WS lacks; WS can batch per frame too. It *looks*
decisive only because the per-message overhead it amortizes is large for a POST
(a full request) and tiny for a frame — so it helps SSE+POST a lot and WS little.
Batch both, or neither, and WS stays ahead at 1:1 while at large N both converge
to the app-work floor. So "batched SSE+POST beats WS" is optimized-vs-unoptimized,
not parity — it proves the overhead is amortizable, which is true for any transport.

**What it costs / when it's realistic:** batching trades a little latency for
throughput (accumulate for a few ms before sending). For signalling it's a partial
fit — ICE candidates arrive in bursts and coalesce cleanly with a short timer; SDP
offer/answer are latency-critical singletons you would not batch. It's still
standard SSE + HTTP POST, just a richer body. But for *this* workload you never
need it: unbatched 1:1 already does ~11.7k signals/s/core (~1,170 call-setups/s/core),
far above bursty signalling, so throughput-per-core is never the binding constraint.

**Verdict:** ±10% of WS throughput-per-core needs only ~2 signals/POST; strict 1:1
lands at ~1.35x (≈11.7k vs 16.1k) — a real but small floor from the HTTP request
lifecycle, and already ~1,170 call-setups/s/core, far beyond what bursty signalling
needs (and latency is a tie on a real network regardless). A batch-free route to
parity would be a streaming request body (one long-lived POST carrying framed
signals,
EventSource for the downlink) — that removes per-message request overhead
entirely, but needs `fetch` upload streaming (Chrome/h2 only today), so it trades
portability for parity.

## HTTP/3 on a fast (Go) server — measured

To remove the Python server as a variable, ran a Go server (Caddy) doing pure
`respond 204`, pinned to one core, hammered by native clients (bombardier for
h1/h2, a quic-go client `/tmp/h3load` for h3 — both fast, non-bottlenecking).
All single-core:

| stack (1 core, POST→204 or equivalent, no per-msg logging) | req/s |
|---|---|
| Caddy (Go) raw 204, **h1.1** | 21,527 |
| uvicorn (Python) POST `/fast/signal` (one leg) | 21,916 |
| Caddy (Go) raw 204, **h3 (QUIC)** | ~18,000 |
| WS (Python `ws_consumer`, full round-trip) | 16,150 |
| Caddy (Go) raw 204, **h2** | 14,915 |

**HTTP/3 is not cheaper per request — it's slower than h1.1.** On the same fast Go
server, h3 (~18k) sits between h2 (15k) and h1.1 (21.5k). QUIC runs the transport
in userspace with per-packet crypto, pacing, and stream management — that CPU
offsets the smaller frame. So the "h3 frame ≈ WS message" intuition is right on the
*wire* but wrong on *cost*: the bottleneck is CPU, and h3 adds CPU. h1.1 is the
fastest HTTP version here.

**Language barely matters for the request itself — it was the *access logging*.**
Once per-message logging is off, Python uvicorn's POST (21.9k, one leg) is right
alongside Caddy's Go raw 204 (21.5k). The earlier "Go ~2x Python" was the logging
artifact (default uvicorn `stderr`-per-request was ~half the cost), not the
runtime. The real structural cost is the request *lifecycle*: same-language proof,
a full SSE+POST round-trip (~80 µs) > a WS round-trip (~60 µs) because the POST
re-creates a scope + runs extra event-loop hops + parse/serialize each message,
which a long-lived WS connection amortizes. That ~1.35x is the floor.

**So a powerful h3 server does not get one-POST-per-message SSE close to WS.** The
only levers remain: amortize the request (batch — measured above) or drop the
request model (WebTransport over h3 — streams/datagrams, no per-request
lifecycle). h3 by itself, one POST per message, is if anything a small step
backward from h1.1.

## On HTTP/3 framing vs WebSocket framing

True on the wire: an h3 POST's framing can be nearly as small as a WS frame. h1.1
re-sends fat text headers every request; h2/HPACK and h3/QPACK index them so after
warmup the per-request header bytes shrink to almost nothing, and a WebTransport
datagram is tiny (a few bytes), ~a WS frame's 2-14.

But that is *wire bytes*, and wire bytes were never our bottleneck — on localhost
bombardier moved a few MB/s, nowhere near a limit. The ~44 µs/POST uplink cost is
**server CPU in the HTTP request lifecycle**: a fresh ASGI scope, three event
crossings (`http.request` / `response.start` / `response.body`), the Python await
machinery, response serialization. QPACK shrinks header *parsing* (a few µs), but
the request-lifecycle cost is transport-agnostic and dominates — so smaller h3
frames do not close the throughput-per-core gap for one-POST-per-message. Measured
direction agrees: h2/h3 were *slower* per message than h1.1 (HPACK/QPACK decode +
stream management add CPU); their wins are loss resilience and multiplexing, not
per-message CPU.

The h3 route that genuinely matches WS per-message cost is **WebTransport**
(QUIC streams/datagrams): it drops the per-message HTTP request lifecycle entirely
— one session scope, a receive/send per message, exactly WS's amortized model — so
the server handles each message frame-cheaply. That can hit WS parity (or better,
via datagrams). The trade: it's no longer "SSE + POST" (different API, an ASGI
extension server-side), and browser support is Chrome/Edge today (Firefox behind a
flag, Safari none) versus SSE+POST's universal reach. Not measured here — this
sandbox has no h3 client (Caddy serves h3, but httpx/curl/headless-Chromium can't
drive h3 against the local self-signed cert), so this part is reasoned, not benched.

Bottom line: h3 makes the *frame* small, but not the *request* cheap. To match WS
throughput-per-core you either amortize the request (batch, measured above) or drop
the request model (WebTransport) — h3 alone, still doing one POST per message,
doesn't do it.

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
