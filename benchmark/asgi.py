"""Benchmark ASGI app: the stock Django app (SSE + POST + login + static) for
HTTP, plus the in-process WS consumer for websocket connections. Both transports
share rtc.sse's logic and state, so the only variable is the wire.

    uvicorn benchmark.asgi:application --host 127.0.0.1 --port 8000
"""
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rtc_demo.settings')

from rtc_demo.asgi import application as django_app  # noqa: E402
from benchmark.ws_consumer import websocket_app  # noqa: E402
from benchmark.raw_post import http_app as raw_http  # noqa: E402
from benchmark import fast  # noqa: E402


async def application(scope, receive, send):
    if scope['type'] == 'websocket':
        await websocket_app(scope, receive, send)
    elif scope['type'] == 'http':
        path = scope['path']
        if path.startswith('/fast/sse/'):
            await fast.sse_app(scope, receive, send)       # optimized SSE (token, no DB)
        elif path == '/fast/stats':
            await fast.stats_app(scope, receive, send)      # benchmark: signals handled
        elif path == '/fast/signal/':
            await fast.post_app(scope, receive, send)       # optimized POST (O(1) token)
        elif path == '/fast/batch/':
            await fast.batch_app(scope, receive, send)       # N signals per POST

        elif path.startswith('/raw/'):
            await raw_http(scope, receive, send)            # lean POST (session read, no middleware)
        else:
            await django_app(scope, receive, send)          # full Django stack
    else:
        await django_app(scope, receive, send)
