"""Open a /fast SSE stream, print the capability token, and hold the connection
open (so the token stays valid) until killed. Used to feed off-the-shelf HTTP
load tools (bombardier/h2load/oha) that POST to /fast/signal/.

    python -m benchmark.mint_token [url]   # prints: TOKEN <hex>
"""
import asyncio
import sys

from benchmark.rtt import FastSse


async def main():
    url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    fs = FastSse(url, 88888)
    await fs.open()
    print(f"TOKEN {fs.token}", flush=True)
    while True:
        await asyncio.sleep(3600)


asyncio.run(main())
