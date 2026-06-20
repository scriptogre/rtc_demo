"""Self round-trip latency probe.

Each client signals ITSELF and times the echo on its own stream -- one
connection per client, isolating transport+server cost.

  --transport ws | sse-fast
  --burst K      fire K signals at once and time until the LAST echo returns
                 (models a call-setup burst of SDP + ICE candidates). K=1 (default)
                 is a plain per-message round-trip.

httpx uses HTTP/2 automatically when --url is https:// (the SSE stream and the
POSTs then share ONE multiplexed connection, like a browser). Put a real RTT in
the path with benchmark.netsim.

  python -m benchmark.rtt --transport ws       --url http://127.0.0.1:8000
  python -m benchmark.rtt --transport sse-fast --url https://127.0.0.1:8444 --burst 10
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


class FastSse:
    def __init__(self, base, i):
        self.base, self.i = base, i
        self.c = _http(base)
        self.done = asyncio.Queue()
        self.cid = self.token = None

    async def open(self):
        await _login(self.c, self.base, self.i)
        self._t = asyncio.create_task(self._reader())
        while not (self.cid and self.token):
            await asyncio.sleep(0.01)

    async def _reader(self):
        ev, dl = None, []
        async with self.c.stream("GET", "/fast/sse/lobby/",
                                 headers={"Accept": "text/event-stream"}) as r:
            async for line in r.aiter_lines():
                if line == "":
                    if dl:
                        if ev == "token":
                            self.token = "\n".join(dl)
                        elif ev == "rtc":
                            m = json.loads("\n".join(dl))["rtc"]
                            if m.get("type") == "connect":
                                self.cid = m["channel_name"]
                            elif m.get("type") == "signal":
                                self.done.put_nowait(time.perf_counter())
                    ev, dl = None, []
                elif line.startswith("event:"):
                    ev = line[6:].strip()
                elif line.startswith("data:"):
                    dl.append(line[5:].lstrip(" "))

    async def send_one(self):
        body = json.dumps({"type": "signal", "recipient": self.cid,
                           "sender": self.cid, "signal": {"x": 1}})
        await self.c.post("/fast/signal/", content=body,
                          headers={"Content-Type": "application/json", "X-Post-Token": self.token})

    async def close(self):
        self._t.cancel()
        await self.c.aclose()


class Ws:
    def __init__(self, base, i):
        self.base, self.i = base, i
        self.c = _http(base)
        self.done = asyncio.Queue()
        self.cid = None

    async def open(self):
        from websockets.asyncio.client import connect
        await _login(self.c, self.base, self.i)
        cookies = "; ".join(f"{ck.name}={ck.value}" for ck in self.c.cookies.jar)
        url = self.base.replace("http", "ws", 1) + "/ws/lobby/"
        sslctx = None
        if self.base.startswith("https"):
            sslctx = ssl.create_default_context()
            sslctx.check_hostname = False
            sslctx.verify_mode = ssl.CERT_NONE
        self.conn = await connect(url, additional_headers={"Cookie": cookies}, ssl=sslctx)
        self._t = asyncio.create_task(self._reader())
        while not self.cid:
            await asyncio.sleep(0.01)

    async def _reader(self):
        async for frame in self.conn:
            msg = json.loads(frame)
            if msg.get("event") == "rtc":
                m = json.loads(msg["data"])["rtc"]
                if m.get("type") == "connect":
                    self.cid = m["channel_name"]
                elif m.get("type") == "signal":
                    self.done.put_nowait(time.perf_counter())

    async def send_one(self):
        await self.conn.send(json.dumps({"cmd": "signal", "rtc": {"type": "signal",
                             "recipient": self.cid, "sender": self.cid, "signal": {"x": 1}}}))

    async def close(self):
        self._t.cancel()
        await self.conn.close()
        await self.c.aclose()


async def drive(client, n, burst, lat):
    for _ in range(max(1, n // burst)):
        t0 = time.perf_counter()
        await asyncio.gather(*(client.send_one() for _ in range(burst)))
        last = t0
        for _ in range(burst):
            last = max(last, await client.done.get())
        lat.append((last - t0) * 1000)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--transport", choices=["ws", "sse-fast"], required=True)
    ap.add_argument("--url", default="http://127.0.0.1:8000")
    ap.add_argument("--n", type=int, default=300)
    ap.add_argument("--conc", type=int, default=1)
    ap.add_argument("--burst", type=int, default=1)
    args = ap.parse_args()

    proto = "ws"
    if args.transport != "ws":
        async with _http(args.url) as probe:
            proto = (await probe.get("/login/")).http_version
    Cls = Ws if args.transport == "ws" else FastSse
    clients = [Cls(args.url, i) for i in range(args.conc)]
    await asyncio.gather(*(c.open() for c in clients))

    lat = []
    await asyncio.gather(*(drive(c, args.n, args.burst, lat) for c in clients))
    await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)

    lat.sort()
    p = lambda q: lat[min(len(lat) - 1, int(q * len(lat)))]
    label = "call-setup" if args.burst > 1 else "msg-rtt  "
    print(f"{args.transport:9s} {proto:8s} burst={args.burst:<3d} conc={args.conc:<2d} "
          f"n={len(lat):4d}  p50={p(.5):6.2f}  p90={p(.9):6.2f}  p99={p(.99):6.2f} ms  [{label}]")

if __name__ == "__main__":
    asyncio.run(main())
