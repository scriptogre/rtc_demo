"""Tiny TCP delay-proxy to put a realistic RTT in the path (no root / no netem).

    python -m benchmark.netsim <listen_port> <upstream_port> <delay_ms_per_direction>

Relays bytes to 127.0.0.1:<upstream>, delaying each chunk by <delay> in each
direction, so round-trip latency gains ~2*delay. It's a transparent L4 relay, so
TLS (h2) passes straight through (the client's handshake is end-to-end with the
upstream). It models propagation delay only -- it canNOT simulate packet loss
(a byte relay can't drop packets without breaking the TCP stream); use real
netem for loss/HOL experiments.
"""
import asyncio
import sys
import time


async def _pipe(reader, writer, delay):
    # Forward each chunk at (arrival + delay) WITHOUT blocking later reads, so
    # back-to-back bytes pipeline (headers+body don't each cost a full RTT) --
    # this models propagation delay, not per-write stalls.
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
            wait = delay - (time.monotonic() - ts)
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


async def _handle(cr, cw, up_port, delay):
    try:
        sr, sw = await asyncio.open_connection('127.0.0.1', up_port)
    except OSError:
        cw.close()
        return
    await asyncio.gather(_pipe(cr, sw, delay), _pipe(sr, cw, delay))


async def main():
    listen, upstream, delay_ms = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
    delay = delay_ms / 1000.0
    server = await asyncio.start_server(
        lambda r, w: _handle(r, w, upstream, delay), '127.0.0.1', listen)
    print(f"netsim :{listen} -> :{upstream}  +{delay_ms}ms/dir (~{2*delay_ms}ms RTT)")
    async with server:
        await server.serve_forever()


asyncio.run(main())
