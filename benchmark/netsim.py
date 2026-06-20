"""Tiny TCP delay-proxy to put a realistic RTT in the path (no root / no netem).

    python -m benchmark.netsim <listen> <upstream> <delay_ms> [loss_prob] [rto_ms]

Relays bytes to 127.0.0.1:<upstream>, delaying each chunk by <delay> per
direction (~2*delay RTT). With <loss_prob> it also models packet loss as an
RTO-sized stall (<rto_ms>, default 200) on the affected chunk -- and because the
stall holds that one connection's pipe, later bytes on the SAME connection wait
behind it. That reproduces the structural effect that matters: a single
connection (WS, or h2 multiplexing the SSE stream + POSTs) head-of-line-blocks
under loss, while h1.1's separate stream/POST connections isolate it.

It is a transparent L4 relay, so TLS (h2) passes straight through. Honest limits:
this is a CONNECTION-level loss approximation for sparse traffic, not packet-level
netem -- it does not model fast-retransmit, congestion control, or QUIC's
per-stream recovery. Use real netem / two boxes for a definitive loss study.
"""
import asyncio
import random
import sys
import time


async def _pipe(reader, writer, delay, loss, rto):
    # Forward each chunk at (arrival + delay) WITHOUT blocking later reads, so
    # back-to-back bytes pipeline (headers+body don't each cost a full RTT).
    q = asyncio.Queue()

    async def rd():
        try:
            while True:
                data = await reader.read(65536)
                q.put_nowait((time.monotonic(), data))
                if not data:
                    break
        except (ConnectionError, asyncio.IncompleteReadError):
            q.put_nowait((time.monotonic(), b''))

    rt = asyncio.create_task(rd())
    try:
        while True:
            ts, data = await q.get()
            if not data:
                break
            extra = rto if (loss and random.random() < loss) else 0.0
            wait = (delay + extra) - (time.monotonic() - ts)
            if wait > 0:
                await asyncio.sleep(wait)
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        rt.cancel()
        try:
            writer.close()
        except Exception:
            pass


async def _handle(cr, cw, up_port, delay, loss, rto):
    try:
        sr, sw = await asyncio.open_connection('127.0.0.1', up_port)
    except OSError:
        cw.close()
        return
    await asyncio.gather(_pipe(cr, sw, delay, loss, rto),
                         _pipe(sr, cw, delay, loss, rto))


async def main():
    listen, upstream, delay_ms = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
    loss = float(sys.argv[4]) if len(sys.argv) > 4 else 0.0
    rto = (float(sys.argv[5]) if len(sys.argv) > 5 else 200.0) / 1000.0
    delay = delay_ms / 1000.0
    server = await asyncio.start_server(
        lambda r, w: _handle(r, w, upstream, delay, loss, rto), '127.0.0.1', listen)
    print(f"netsim :{listen} -> :{upstream}  +{delay_ms}ms/dir (~{2*delay_ms}ms RTT) "
          f"loss={loss*100:.1f}% rto={rto*1000:.0f}ms")
    async with server:
        await server.serve_forever()


asyncio.run(main())
