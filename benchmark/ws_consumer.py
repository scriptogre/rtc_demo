"""In-process WebSocket transport, for the A/B benchmark only.

Same job as the SSE+POST layer, different wire. It reuses rtc.sse's in-process
state and the _do_* command handlers verbatim, so comparing this against the SSE
stream isolates the *transport* (WS frames vs SSE + HTTP POST) instead of two
different backplanes (Redis/presence vs in-process dicts).

Like Ken's consumer, identity is resolved once at connect. The SSE+POST side
pays a session read per POST instead -- that per-request HTTP cost is exactly
what the benchmark is meant to expose, so we don't paper over it here.

This is a raw ASGI websocket app (no Channels) mounted by benchmark/asgi.py.
"""
import asyncio
import json
import uuid
from http.cookies import SimpleCookie

from asgiref.sync import sync_to_async

from rtc import sse


@sync_to_async
def _identity(sessionid):
    """Resolve (user, channel_id) from the session cookie, creating+saving the
    channel_id on first connect -- the same session DB work the SSE connect does."""
    from django.contrib.auth.models import User
    from django.contrib.sessions.backends.db import SessionStore

    if not sessionid:
        return None, None
    store = SessionStore(session_key=sessionid)
    uid = store.get('_auth_user_id')
    if not uid:
        return None, None
    cid = store.get('channel_id')
    if not cid:
        cid = str(uuid.uuid4())
        store['channel_id'] = cid
        store.save()
    try:
        return User.objects.get(pk=uid), cid
    except User.DoesNotExist:
        return None, None


def _cookie(scope, name):
    for key, value in scope.get('headers', []):
        if key == b'cookie':
            jar = SimpleCookie()
            jar.load(value.decode())
            morsel = jar.get(name)
            return morsel.value if morsel else None
    return None


def _room(scope):
    parts = [p for p in scope['path'].split('/') if p]
    return parts[-1] if parts else 'lobby'


async def websocket_app(scope, receive, send):
    if (await receive())['type'] != 'websocket.connect':
        return

    user, channel_id = await _identity(_cookie(scope, 'sessionid'))
    if user is None:
        await send({'type': 'websocket.close', 'code': 4403})
        return
    await send({'type': 'websocket.accept'})

    queue = asyncio.Queue()
    user_name = user.first_name or user.username
    await sse._register(channel_id, user_name, _room(scope), queue)

    async def pump():
        while True:
            event_name, data = await queue.get()
            await send({'type': 'websocket.send',
                        'text': json.dumps({'event': event_name, 'data': data})})

    sender = asyncio.create_task(pump())
    try:
        while True:
            event = await receive()
            if event['type'] == 'websocket.disconnect':
                break
            if event['type'] != 'websocket.receive':
                continue
            msg = json.loads(event.get('text') or '{}')
            cmd = msg.get('cmd')
            if cmd == 'signal':
                await sse._do_signal(channel_id, msg['rtc'])
            elif cmd == 'join':
                await sse._do_join(channel_id, msg['room'])
            elif cmd == 'hangup':
                await sse._do_hangup(channel_id)
    finally:
        sender.cancel()
        await sse._cleanup(channel_id, queue)
