"""Optimized SSE+POST path, for the benchmark.

Goal: show SSE+POST can approach WebSocket latency once the POST stops doing
per-message work a WS frame never does. A WS resolves identity ONCE at connect;
here we do the same:

  * /fast/sse/<room>/ resolves the session once, mints a secret capability
    token, registers the connection, and streams (raw ASGI -- no Django stack).
    The token is sent to the client on its own stream (event: token), never to
    peers.
  * /fast/signal/ authenticates by an O(1) in-memory token lookup -- pure async,
    no DB, no sync_to_async thread hop, no middleware -- then _do_signal.

The token is a 122-bit UUID4 bearer bound to the live connection (the SSE analog
of a WS socket binding identity). Single-process by design, like the rest.
"""
import asyncio
import json
import uuid

from asgiref.sync import sync_to_async

from rtc import sse
from benchmark.raw_post import _cookie, _read_body, _reply

post_tokens: dict[str, str] = {}   # secret token -> channel_id
signals = 0                        # benchmark: server-side count of signals handled


@sync_to_async(thread_sensitive=False)
def _channel_for(sessionid):
    """Resolve (and persist) the channel_id from the session -- ONCE per
    connection, exactly like Ken's WS connect."""
    from django.contrib.sessions.backends.db import SessionStore
    if not sessionid:
        return None
    store = SessionStore(session_key=sessionid)
    cid = store.get('channel_id')
    if not cid:
        cid = str(uuid.uuid4())
        store['channel_id'] = cid
        store.save()
    return cid


def _room(scope):
    parts = [p for p in scope['path'].split('/') if p]
    return parts[-1] if parts else 'lobby'


async def sse_app(scope, receive, send):
    channel_id = await _channel_for(_cookie(scope, 'sessionid'))
    if not channel_id:
        await _reply(send, 403)
        return

    token = uuid.uuid4().hex
    post_tokens[token] = channel_id
    queue = asyncio.Queue()
    await sse._register(channel_id, 'bench', _room(scope), queue)

    await send({'type': 'http.response.start', 'status': 200, 'headers': [
        (b'content-type', b'text/event-stream'),
        (b'cache-control', b'no-cache'),
        (b'x-accel-buffering', b'no'),
    ]})
    # hand the capability token to this client only
    await send({'type': 'http.response.body',
                'body': f'event: token\ndata: {token}\n\n'.encode(),
                'more_body': True})

    async def disconnected():
        while True:
            if (await receive())['type'] == 'http.disconnect':
                return
    watcher = asyncio.ensure_future(disconnected())
    try:
        while not watcher.done():
            try:
                event_name, data = await asyncio.wait_for(queue.get(), timeout=25.0)
                body = sse._format_sse(event_name, data).encode()
            except asyncio.TimeoutError:
                body = b':ka\n\n'
            await send({'type': 'http.response.body', 'body': body, 'more_body': True})
    finally:
        watcher.cancel()
        post_tokens.pop(token, None)
        await sse._cleanup(channel_id, queue)


async def post_app(scope, receive, send):
    token = None
    for key, value in scope.get('headers', []):
        if key == b'x-post-token':
            token = value.decode()
            break
    channel_id = post_tokens.get(token)
    if not channel_id:
        await _reply(send, 403)
        return
    await sse._do_signal(channel_id, json.loads(await _read_body(receive) or b'{}'))
    global signals
    signals += 1
    await _reply(send, 204)


async def stats_app(scope, receive, send):
    body = json.dumps({"signals": signals}).encode()
    await send({'type': 'http.response.start', 'status': 200,
                'headers': [(b'content-type', b'application/json')]})
    await send({'type': 'http.response.body', 'body': body})
