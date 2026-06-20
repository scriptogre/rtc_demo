"""SSE + POST signalling layer.

This replaces Ken's Channels WebSocket consumer (rtc/consumers.py).

  * Server -> client: one persistent SSE stream per user (GET /sse/<room>/),
    delivering two kinds of message:
        event: rtc   -> JSON, re-dispatched as a DOM event for JS handlers
        (unnamed)    -> HTML fragment, auto-swapped into the page by htmx 4
  * Client -> server: three plain POST endpoints that return 204:
        POST /api/signal/  -> forward a signal to a peer  (was {rtc: {...}})
        POST /api/join/    -> join a room                 (was {join: ...})
        POST /api/hangup/  -> leave the current room      (was {hangup: ...})

All signalling state lives in-process (single process by design); there is no
Channels layer, no Redis, no DB-backed presence. Presence == an open SSE
connection.
"""
import asyncio
import json
import uuid
from functools import wraps

from asgiref.sync import sync_to_async
from django.http import HttpResponse, StreamingHttpResponse
from django.template.loader import render_to_string

# --- In-process signalling state (protected by a single asyncio.Lock) -------
state_lock = asyncio.Lock()
connections: dict[str, asyncio.Queue] = {}   # channel_id -> per-user SSE queue
rooms: dict[str, set[str]] = {}              # room name  -> set of channel_ids
channel_meta: dict[str, dict] = {}           # channel_id -> {user_name, short_name, room}

KEEPALIVE_TIMEOUT = 25.0  # seconds between server keepalive comments


# --- helpers ----------------------------------------------------------------
def _short_name(channel_id):
    """Mirror of RtcConsumer.short_name (Ken used the channel name's tail)."""
    return 'peer-' + channel_id[-6:]


def _format_sse(event_name, data):
    """Serialize one SSE message. Multi-line data becomes multiple `data:`
    lines, which the client rejoins with '\\n'.

    htmx 4's hx-sse splits the two channels by whether the message is named:
      * an *unnamed* message (no `event:` field) is auto-swapped into the DOM
        -> we use this for 'html' fragments (OOB swaps).
      * a *named* message is re-dispatched as a DOM event of that name
        -> we use 'rtc' for typed events handled in JS.
    """
    body = ''.join('data: %s\n' % line for line in data.split('\n'))
    if event_name == 'html':
        return '%s\n' % body
    return 'event: %s\n%s\n' % (event_name, body)


def _render_panel(short_name, user_name):
    return render_to_string('rtc/video_panel.html', {
        'id': short_name, 'user_name': user_name,
    })


def _occupants(room):
    """List of {channel_name, user_name, short_name} for everyone in `room`.

    Replaces channels_presence's Presence/Room query (_room_occupants).
    """
    result = []
    for cid in rooms.get(room, set()):
        meta = channel_meta.get(cid)
        if meta:
            result.append({
                'channel_name': cid,
                'user_name': meta['user_name'],
                'short_name': meta['short_name'],
            })
    return result


async def _enqueue(channel_id, event_name, data):
    queue = connections.get(channel_id)
    if queue is not None:
        await queue.put((event_name, data))


async def _all_but_me(room, channel_id, event_name, data):
    """Enqueue an event to every channel in `room` except `channel_id`.

    Replaces RtcConsumer._all_but_me (+ channel_layer.send fan-out).
    """
    for cid in list(rooms.get(room, set())):
        if cid != channel_id:
            await _enqueue(cid, event_name, data)


@sync_to_async
def _get_channel_id(request, create=False):
    """Read (or, on first SSE connect, create) the per-user channel_id stored
    in the session. Session access is sync ORM, so it is wrapped for use from
    async views. (Kept on the DB session, like Ken's Channels setup, so the
    SSE+POST side does the same per-request session work the WS side did -- the
    benchmark must compare protocols, not skipped DB hits.)"""
    cid = request.session.get('channel_id')
    if not cid and create:
        cid = str(uuid.uuid4())
        request.session['channel_id'] = cid
        request.session.save()
    return cid


# --- command handlers (the three POST verbs) --------------------------------
async def _do_join(channel_id, room_name):
    """Port of RtcConsumer._join."""
    async with state_lock:
        meta = channel_meta.get(channel_id)
        if meta is None or channel_id not in connections:
            return  # no live SSE connection for this user

        # leave the previous room
        old_room = meta.get('room')
        if old_room and old_room in rooms:
            rooms[old_room].discard(channel_id)
        meta['room'] = room_name

        # occupants already in the room (self is not added yet, so excluded)
        occupants = _occupants(room_name)
        all_divs = '\n'.join(
            _render_panel(occ['short_name'], occ['user_name'])
            for occ in occupants
        )

        # --- to self ---
        # NOTE: the html must be sent before the connections (rtc), otherwise
        # the connection functions in the client will try to access the div
        # before it exists. (Same ordering constraint Ken documents; on a
        # single ordered stream this just works because we enqueue in order.)
        await _enqueue(channel_id, 'html',
                       render_to_string('rtc/header.html', {'room': room_name}))
        await _enqueue(channel_id, 'html', all_divs)
        await _enqueue(channel_id, 'rtc',
                       json.dumps({'rtc': {'type': 'others', 'ids': occupants}}))

        # --- to all other occupants: html (my panel) before rtc (my info) ---
        self_div = _render_panel(meta['short_name'], meta['user_name'])
        await _all_but_me(room_name, channel_id, 'html', self_div)
        await _all_but_me(room_name, channel_id, 'rtc', json.dumps({
            'rtc': {
                'type': 'other',
                'channel_name': channel_id,
                'user_name': meta['user_name'],
                'short_name': meta['short_name'],
            },
        }))

        # finally, add self to the room
        rooms.setdefault(room_name, set()).add(channel_id)


async def _do_hangup(channel_id):
    """Port of RtcConsumer._hangup."""
    async with state_lock:
        meta = channel_meta.get(channel_id)
        if meta is None:
            return
        room = meta.get('room')
        if room and room in rooms:
            rooms[room].discard(channel_id)
            await _all_but_me(room, channel_id, 'rtc', json.dumps({
                'rtc': {'type': 'disconnected', 'channel_name': channel_id},
            }))
        meta['room'] = None


async def _do_signal(channel_id, rtc):
    """Port of RtcConsumer._rtc."""
    async with state_lock:
        recipient = rtc.get('recipient')
        if recipient:
            await _enqueue(recipient, 'rtc', json.dumps({'rtc': rtc}))
        else:
            meta = channel_meta.get(channel_id)
            if meta and meta.get('room'):
                await _all_but_me(meta['room'], channel_id, 'rtc',
                                  json.dumps({'rtc': rtc}))


# --- views ------------------------------------------------------------------
async def sse_stream(request, room_name):
    """Persistent SSE stream. Setup/teardown here replace the consumer's
    connect()/disconnect()."""
    user = await request.auser()
    if not user.is_authenticated:
        return HttpResponse(status=403)

    channel_id = await _get_channel_id(request, create=True)
    user_name = user.first_name or user.username
    queue = asyncio.Queue()

    async def event_stream():
        # --- connect (port of RtcConsumer.connect) ---
        async with state_lock:
            # Idempotent (re)connect: drop any stale room membership left by a
            # previous connection for this channel_id before re-registering.
            stale = channel_meta.get(channel_id)
            if stale and stale.get('room') and stale['room'] in rooms:
                rooms[stale['room']].discard(channel_id)

            connections[channel_id] = queue
            channel_meta[channel_id] = {
                'user_name': user_name,
                'short_name': _short_name(channel_id),
                'room': room_name,
            }
            rooms.setdefault(room_name, set()).add(channel_id)

            await queue.put(('rtc', json.dumps(
                {'rtc': {'type': 'connect', 'channel_name': channel_id}})))
            await queue.put(('html',
                render_to_string('rtc/header.html', {'room': room_name})))

        try:
            while True:
                try:
                    event_name, data = await asyncio.wait_for(
                        queue.get(), timeout=KEEPALIVE_TIMEOUT)
                    yield _format_sse(event_name, data)
                except asyncio.TimeoutError:
                    yield ':ka\n\n'  # keepalive comment, defeats proxy timeouts
        finally:
            await _cleanup(channel_id, queue)

    response = StreamingHttpResponse(event_stream(),
                                     content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    response['X-Accel-Buffering'] = 'no'  # disable nginx buffering
    return response


async def _cleanup(channel_id, queue):
    """SSE disconnect cleanup (port of RtcConsumer.disconnect)."""
    async with state_lock:
        # Only tear down if THIS connection still owns the channel. A fast
        # EventSource reconnect may have already installed a new queue; in that
        # case we must not remove it or notify peers (reconnect idempotency).
        if connections.get(channel_id) is not queue:
            return
        meta = channel_meta.pop(channel_id, None)
        connections.pop(channel_id, None)
        if meta and meta.get('room') and meta['room'] in rooms:
            room = meta['room']
            rooms[room].discard(channel_id)
            await _all_but_me(room, channel_id, 'rtc', json.dumps({
                'rtc': {'type': 'disconnected', 'channel_name': channel_id},
            }))


def channel_post(handler):
    """POST-only endpoint: resolve the caller's channel_id (405/403 otherwise),
    run `handler(request, channel_id)`, return 204. The 405 guard matters —
    a GET isn't CSRF-checked, and these mutate state."""
    @wraps(handler)
    async def view(request):
        if request.method != 'POST':
            return HttpResponse(status=405)
        channel_id = await _get_channel_id(request)
        if not channel_id:
            return HttpResponse(status=403)
        await handler(request, channel_id)
        return HttpResponse(status=204)
    return view


@channel_post
async def api_signal(request, channel_id):
    await _do_signal(channel_id, json.loads(request.body or b'{}'))


@channel_post
async def api_join(request, channel_id):
    room = json.loads(request.body or b'{}').get('room')
    if room:
        await _do_join(channel_id, room)


@channel_post
async def api_hangup(request, channel_id):
    await _do_hangup(channel_id)
