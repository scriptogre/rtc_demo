"""Find the SERVER's true POST-ingestion ceiling, free of client (httpx) cost.

Opens one /fast SSE stream to mint a valid token, then hammers /fast/signal/ with
raw, pipelined HTTP/1.1 over plain sockets (recipient='nobody' so it measures
pure ingestion: HTTP parse + token lookup + _do_signal, no delivery/memory
growth). Throughput is read from the server-side /fast/stats counter, and server
CPU from psutil -- so the number is server-bound, not limited by the generator.

  taskset -c 0 uvicorn benchmark.asgi:application --port 8000 --loop uvloop --http httptools &
  taskset -c 1-3 python -m benchmark.rawblast --server-pid <pid> --conns 8
"""
import argparse, asyncio, json, time
import httpx
import psutil

from benchmark.rtt import FastSse


def _req(token, recipient):
    body = json.dumps({"type": "signal", "recipient": recipient,
                       "sender": "x", "signal": {"x": 1}}).encode()
    return (f"POST /fast/signal/ HTTP/1.1\r\nHost: x\r\n"
            f"X-Post-Token: {token}\r\nContent-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n\r\n").encode() + body


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8000")
    ap.add_argument("--conns", type=int, default=200)
    ap.add_argument("--duration", type=float, default=6.0)
    ap.add_argument("--recipients", type=int, default=0,
                    help="0 = ingestion only (recipient offline); >0 = full "
                         "round-trip, POST to that many draining SSE streams")
    ap.add_argument("--server-pid", type=int, required=True)
    args = ap.parse_args()

    host = args.url.split("//")[1].split(":")[0]
    port = int(args.url.rsplit(":", 1)[1])

    holder = FastSse(args.url, 99999)          # keeps a valid token alive
    await holder.open()
    stop = [False]

    # optional draining SSE recipients (for the full round-trip measurement)
    holders, drains = [], []
    if args.recipients:
        holders = [FastSse(args.url, 90000 + i) for i in range(args.recipients)]
        sem = asyncio.Semaphore(4)           # don't melt sqlite with concurrent logins

        async def _open(h):
            async with sem:
                await h.open()
        await asyncio.gather(*(_open(h) for h in holders))

        async def drain_done(h):
            while not stop[0]:
                await h.done.get()
        drains = [asyncio.create_task(drain_done(h)) for h in holders]
        reqs = [_req(holder.token, h.cid) for h in holders]
    else:
        reqs = [_req(holder.token, "nobody")]

    def req_for(i):
        return reqs[i % len(reqs)]

    async def worker(req):
        # one in-flight request at a time per connection -> no backlog
        try:
            r, w = await asyncio.open_connection(host, port)
        except OSError:
            return
        try:
            while not stop[0]:
                w.write(req)
                await w.drain()
                buf = b''
                while b'\r\n\r\n' not in buf:
                    d = await r.read(4096)
                    if not d:
                        return
                    buf += d
        except OSError:
            pass
        finally:
            try:
                w.close()
            except Exception:
                pass

    async def stats():
        async with httpx.AsyncClient() as c:
            return (await c.get(args.url + "/fast/stats")).json()["signals"]

    p = psutil.Process(args.server_pid)
    p.cpu_percent(None)
    s0 = await stats()                       # idle read, before load
    t0 = time.perf_counter()
    workers = [asyncio.create_task(worker(req_for(i))) for i in range(args.conns)]
    cpu = []
    for _ in range(int(args.duration * 2)):
        await asyncio.sleep(0.5)
        cpu.append(p.cpu_percent(None))
    stop[0] = True
    t1 = time.perf_counter()
    await asyncio.gather(*workers, return_exceptions=True)
    for d in drains:
        d.cancel()
    await asyncio.sleep(0.3)
    s1 = await stats()                       # idle read, after load drains
    await asyncio.gather(holder.close(), *(h.close() for h in holders),
                         return_exceptions=True)

    tput = (s1 - s0) / (t1 - t0)
    mode = f"full round-trip ({args.recipients} recipients)" if args.recipients else "ingestion only"
    print(f"raw POST [{mode}]: conns={args.conns}  {tput:>10,.0f} signals/s   "
          f"server_cpu(1 core) max={max(cpu):.0f}% mean={sum(cpu)/len(cpu):.0f}%   "
          f"=> {(sum(cpu)/len(cpu))/100/tput*1e6:.1f} us/signal")


asyncio.run(main())
