"""Self round-trip latency probe.

Each client sends a signal addressed to ITSELF and times until it returns on its
own stream -- one connection per client, so it isolates transport+server cost
from peer pairing (and at conc=1 from harness contention).

  --transport ws        WebSocket frame out / frame in
  --transport sse-fast  optimized SSE+POST (token auth, no per-request DB)

httpx negotiates HTTP/2 automatically when --url is https:// -- then the SSE
stream and the POSTs share ONE multiplexed connection, the way a browser drives
EventSource + fetch. With http:// it is HTTP/1.1 (POSTs on a second connection).

  python -m benchmark.rtt --transport ws        --url http://127.0.0.1:8000
  python -m benchmark.rtt --transport sse-fast  --url http://127.0.0.1:8000
  python -m benchmark.rtt --transport sse-fast  --url https://127.0.0.1:8443   # HTTP/2
"""
import argparse, asyncio, ssl, statistics, time, json
import httpx


def _http(url):
    return httpx.AsyncClient(base_url=url, timeout=30, verify=False,
                             http2=url.startswith("https"))


async def _login(c, base, i):
    await c.get("/login/")
    tok = c.cookies.get("csrftoken")
    await c.post("/login/", data={"csrfmiddlewaretoken": tok, "screen_name": f"r{i}",
                                  "name": f"rtt{i}", "next": "/"}, headers={"Referer": base})


async def sse_fast_client(base, i, n, lat):
    c = _http(base)
    await _login(c, base, i)
    state = {"cid": None, "token": None}
    done = asyncio.Queue()

    async def reader():
        ev, dl = None, []
        async with c.stream("GET", "/fast/sse/lobby/",
                            headers={"Accept": "text/event-stream"}) as r:
            async for line in r.aiter_lines():
                if line == "":
                    if dl:
                        if ev == "token":
                            state["token"] = "\n".join(dl)
                        elif ev == "rtc":
                            m = json.loads("\n".join(dl))["rtc"]
                            if m.get("type") == "connect":
                                state["cid"] = m["channel_name"]
                            elif m.get("type") == "signal":
                                done.put_nowait(time.perf_counter())
                    ev, dl = None, []
                elif line.startswith("event:"):
                    ev = line[6:].strip()
                elif line.startswith("data:"):
                    dl.append(line[5:].lstrip(" "))

    t = asyncio.create_task(reader())
    while not (state["cid"] and state["token"]):
        await asyncio.sleep(0.01)
    hdr = {"Content-Type": "application/json", "X-Post-Token": state["token"]}
    for _ in range(n):
        body = json.dumps({"type": "signal", "recipient": state["cid"],
                           "sender": state["cid"], "signal": {"x": 1}})
        s = time.perf_counter()
        await c.post("/fast/signal/", content=body, headers=hdr)
        recv = await done.get()
        lat.append((recv - s) * 1000)
    t.cancel()
    await c.aclose()


async def ws_client(base, i, n, lat):
    from websockets.asyncio.client import connect
    c = _http(base)
    await _login(c, base, i)
    cookies = "; ".join(f"{ck.name}={ck.value}" for ck in c.cookies.jar)
    ws_url = base.replace("http", "ws", 1) + "/ws/lobby/"
    sslctx = None
    if base.startswith("https"):
        sslctx = ssl.create_default_context()
        sslctx.check_hostname = False
        sslctx.verify_mode = ssl.CERT_NONE
    conn = await connect(ws_url, additional_headers={"Cookie": cookies}, ssl=sslctx)
    state = {"cid": None}
    done = asyncio.Queue()

    async def reader():
        async for frame in conn:
            msg = json.loads(frame)
            if msg.get("event") == "rtc":
                m = json.loads(msg["data"])["rtc"]
                if m.get("type") == "connect":
                    state["cid"] = m["channel_name"]
                elif m.get("type") == "signal":
                    done.put_nowait(time.perf_counter())

    t = asyncio.create_task(reader())
    while not state["cid"]:
        await asyncio.sleep(0.01)
    for _ in range(n):
        frame = json.dumps({"cmd": "signal", "rtc": {"type": "signal",
                            "recipient": state["cid"], "sender": state["cid"], "signal": {"x": 1}}})
        s = time.perf_counter()
        await conn.send(frame)
        recv = await done.get()
        lat.append((recv - s) * 1000)
    t.cancel()
    await conn.close()
    await c.aclose()


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--transport", choices=["ws", "sse-fast"], required=True)
    ap.add_argument("--url", default="http://127.0.0.1:8000")
    ap.add_argument("--n", type=int, default=400)
    ap.add_argument("--conc", type=int, default=1)
    args = ap.parse_args()

    # probe the actually-negotiated HTTP version (don't guess from the scheme)
    proto = "ws"
    if args.transport != "ws":
        async with _http(args.url) as probe:
            proto = (await probe.get("/login/")).http_version
    client = ws_client if args.transport == "ws" else sse_fast_client
    lat = []
    await asyncio.gather(*(client(args.url, i, args.n, lat) for i in range(args.conc)))
    lat.sort()
    p = lambda q: lat[min(len(lat) - 1, int(q * len(lat)))]
    print(f"{args.transport:9s} {proto:5s} conc={args.conc:<2d} n={len(lat):5d}  "
          f"min={lat[0]:5.2f}  p50={p(.5):5.2f}  p90={p(.9):5.2f}  p99={p(.99):5.2f}  "
          f"mean={statistics.fmean(lat):5.2f} ms")

asyncio.run(main())
