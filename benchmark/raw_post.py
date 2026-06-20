"""Lean raw-ASGI POST endpoints, for the benchmark only.

The /api/* POST views go through Django's full middleware stack. Under an async
view every *sync* middleware (session, auth, CSRF, ...) is sync_to_async-wrapped
with thread_sensitive=True, i.e. funnelled onto one thread -- which is what
dominates the SSE+POST latency/CPU, not the protocol. These /raw/* handlers do
the same real per-request work (resolve channel_id from the session cookie, then
_do_signal/_do_join/_do_hangup) but skip the middleware bounce, so the benchmark
can separate "cost of SSE+POST" from "cost of Django's request pipeline".

Identity is still resolved per request (the genuine price of statelessness),
just with a parallel thread pool (thread_sensitive=False) like Ken's WS side
used via channels' database_sync_to_async. CSRF is dropped here; it is an
HMAC compare, microseconds, not the bottleneck.
"""
import json
from http.cookies import SimpleCookie

from asgiref.sync import sync_to_async

from rtc import sse


@sync_to_async(thread_sensitive=False)
def _channel_id(sessionid):
    from django.contrib.sessions.backends.db import SessionStore
    if not sessionid:
        return None
    return SessionStore(session_key=sessionid).get('channel_id')


def _cookie(scope, name):
    for key, value in scope.get('headers', []):
        if key == b'cookie':
            jar = SimpleCookie()
            jar.load(value.decode())
            morsel = jar.get(name)
            return morsel.value if morsel else None
    return None


async def _read_body(receive):
    buf = b''
    while True:
        event = await receive()
        buf += event.get('body', b'')
        if not event.get('more_body'):
            break
    return buf


async def _reply(send, status):
    await send({'type': 'http.response.start', 'status': status, 'headers': []})
    await send({'type': 'http.response.body', 'body': b''})


async def http_app(scope, receive, send):
    channel_id = await _channel_id(_cookie(scope, 'sessionid'))
    if not channel_id:
        await _reply(send, 403)
        return
    data = json.loads(await _read_body(receive) or b'{}')
    path = scope['path']
    if path == '/raw/signal/':
        await sse._do_signal(channel_id, data)
    elif path == '/raw/join/':
        if data.get('room'):
            await sse._do_join(channel_id, data['room'])
    elif path == '/raw/hangup/':
        await sse._do_hangup(channel_id)
    else:
        await _reply(send, 404)
        return
    await _reply(send, 204)
