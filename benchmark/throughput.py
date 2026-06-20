"""Saturation throughput: how many signals/sec can ONE server core deliver?

Opens C connections, keeps `--inflight` sends in flight per connection (each a
self-addressed signal), and counts delivered echoes over --duration. Pin the
server to one core and run this on the others:

  taskset -c 0 uvicorn benchmark.asgi:application --port 8000 --loop uvloop --http httptools &
  taskset -c 1-3 python -m benchmark.throughput --transport ws       --conns 8 --server-pid <pid>
  taskset -c 1-3 python -m benchmark.throughput --transport sse-fast --conns 8 --server-pid <pid>
"""
import argparse, asyncio, time
import httpx

from benchmark.rtt import Ws, FastSse

try:
    import psutil
except ImportError:
    psutil = None


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--transport", choices=["ws", "sse-fast"], required=True)
    ap.add_argument("--url", default="http://127.0.0.1:8000")
    ap.add_argument("--conns", type=int, default=8)
    ap.add_argument("--inflight", type=int, default=8)
    ap.add_argument("--duration", type=float, default=8.0)
    ap.add_argument("--server-pid", type=int, default=None)
    args = ap.parse_args()

    Cls = Ws if args.transport == "ws" else FastSse
    clients = [Cls(args.url, i) for i in range(args.conns)]
    await asyncio.gather(*(c.open() for c in clients))

    stop = False
    sent = 0
    recv = 0

    async def drain(c):
        nonlocal recv
        while not stop:
            await c.done.get()
            recv += 1

    async def sender(c):
        nonlocal sent
        while not stop:
            await c.send_one()
            sent += 1

    proc = psutil.Process(args.server_pid) if (args.server_pid and psutil) else None
    if proc:
        proc.cpu_percent(None)

    runners = [asyncio.create_task(drain(c)) for c in clients]
    runners += [asyncio.create_task(sender(c)) for c in clients for _ in range(args.inflight)]

    t0 = time.perf_counter()
    cpu = []
    while time.perf_counter() - t0 < args.duration:
        await asyncio.sleep(0.5)
        if proc:
            cpu.append(proc.cpu_percent(None))
    stop = True
    await asyncio.sleep(0.5)
    elapsed = time.perf_counter() - t0
    for r in runners:
        r.cancel()
    await asyncio.gather(*runners, return_exceptions=True)
    await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)

    proto = "ws"
    if args.transport != "ws":
        async with httpx.AsyncClient(base_url=args.url, verify=False,
                                     http2=args.url.startswith("https")) as p:
            proto = (await p.get("/login/")).http_version
    server_cpu = f"{max(cpu):.0f}%/{sum(cpu)/len(cpu):.0f}%" if cpu else "n/a"
    print(f"{args.transport:9s} {proto:8s} conns={args.conns} inflight={args.inflight}  "
          f"delivered={recv/elapsed:8.0f} msg/s  (sent={sent/elapsed:.0f}/s)  "
          f"server_cpu(max/mean 1 core)={server_cpu}")


asyncio.run(main())
