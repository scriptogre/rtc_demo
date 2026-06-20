"""Pure load generator: open --conns connections and push self-signals as fast
as possible for --duration, draining (discarding) echoes so TCP backpressure
doesn't stall it. Does NOT measure -- read the server's /fast/stats counter
around the run for an authoritative, client-independent throughput. Run several
of these (one per core) to actually saturate the server:

  taskset -c 1 python -m benchmark.blast --transport ws &
  taskset -c 2 python -m benchmark.blast --transport ws &
  taskset -c 3 python -m benchmark.blast --transport ws &
"""
import argparse, asyncio
from benchmark.rtt import Ws, FastSse


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--transport", choices=["ws", "sse-fast"], required=True)
    ap.add_argument("--url", default="http://127.0.0.1:8000")
    ap.add_argument("--conns", type=int, default=4)
    ap.add_argument("--inflight", type=int, default=8)
    ap.add_argument("--duration", type=float, default=8.0)
    ap.add_argument("--base", type=int, default=0, help="user-id offset (unique per process)")
    args = ap.parse_args()

    Cls = Ws if args.transport == "ws" else FastSse
    clients = [Cls(args.url, args.base + i) for i in range(args.conns)]
    await asyncio.gather(*(c.open() for c in clients))

    stop = False

    async def drain(c):
        while not stop:
            await c.done.get()

    async def sender(c):
        while not stop:
            await c.send_one()

    runners = [asyncio.create_task(drain(c)) for c in clients]
    runners += [asyncio.create_task(sender(c)) for c in clients for _ in range(args.inflight)]
    await asyncio.sleep(args.duration)
    stop = True
    for r in runners:
        r.cancel()
    await asyncio.gather(*runners, return_exceptions=True)
    await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)


asyncio.run(main())
