# SSE + POST signalling (a WebSocket-free port of the WebRTC demo)

This branch replaces the WebSocket signalling layer of Ken Whitesell's
`rtc_demo` with **SSE (server → client) + plain HTTP POST (client → server)**.
The WebRTC media path is untouched; only the signalling transport changed.

It is a deliberate, faithful port: the goal is an A/B baseline that matches
Ken's semantics so the comparison is *protocol vs protocol*, not
implementation vs implementation.

## Architecture

```
                  GET /sse/<room>/   (one long-lived stream per tab)
   browser  ◀───────────────────────────────────────────  Django async view
            ─ event: rtc   (JSON; dispatched to JS handlers) ─
            ─ (unnamed)    (HTML fragment; htmx swaps it OOB) ─
            ─ :ka          (keepalive comment every 25s) ─

   browser  ──── POST /api/signal/ ┐
            ──── POST /api/join/   ├─▶  Django async views, all return 204
            ──── POST /api/hangup/ ┘     (CSRF + session auth via middleware)
```

* **Server → client: SSE.** One persistent `StreamingHttpResponse`
  (`text/event-stream`) per user, produced by an async generator. Two message
  kinds share the one ordered stream:
  * **`event: rtc`** — JSON, the typed events Ken sent as `{rtc: {...}}`. The
    htmx 4 `hx-sse` extension re-dispatches every *named* SSE event as a DOM
    `CustomEvent` of that name, so `tr.js` listens for `rtc` and fans messages
    out to `apps._forward(key, type, message)` — identical to Ken's `tr-ext`.
  * **unnamed messages** — HTML fragments (Ken's `{html: "..."}`). htmx 4
    auto-swaps *unnamed* SSE messages into the DOM. Every fragment is an
    out-of-band swap (`hx-swap-oob`), so they land by id exactly as the old
    `ws` extension did (which treated every top-level child as an OOB swap).
  * **`:ka`** keepalive comment every 25 s to defeat proxy idle timeouts.
* **Client → server: POST returning 204.** Three endpoints replace Ken's three
  WS command keys (`{rtc}`, `{join}`, `{hangup}`). No response body; the SSE
  stream carries any resulting state back.
* **In-process state only.** Module-level dicts under one `asyncio.Lock`. No
  Channels, no Redis, no DB-backed presence. **Presence == an open SSE
  connection.** Single-process by design.

### Server state (`rtc/sse.py`)

```python
connections : dict[str, asyncio.Queue]   # channel_id -> that user's SSE queue
rooms       : dict[str, set[str]]         # room name  -> channel_ids in it
channel_meta: dict[str, dict]             # channel_id -> {user_name, short_name, room}
```

`channel_meta` is the one structure not named in the brief; it holds the
per-user info Ken read from the `channels_presence` DB tables (`user_name`,
`short_name`) plus each channel's current room.

### Mapping from Ken's consumer

| Ken (`RtcConsumer`)        | Here (`rtc/sse.py`)                                    |
|----------------------------|-------------------------------------------------------|
| `connect()`                | SSE view setup (register queue, enqueue connect+header)|
| `disconnect()`             | `_cleanup()` in the generator's `finally`             |
| `receive_json` dispatch    | three POST views (`api_signal/join/hangup`)           |
| `_join()`                  | `_do_join()`                                           |
| `_rtc()`                   | `_do_signal()`                                         |
| `_hangup()`                | `_do_hangup()`                                         |
| `_all_but_me()`            | `_all_but_me()` (enqueue to each other queue)         |
| `channel_layer.send(to)`   | `connections[to].put(...)`                            |
| `_room_occupants()` (DB)   | `_occupants()` (reads `rooms` + `channel_meta`)       |
| `self.channel_name`        | `channel_id` (random UUID stored in session)          |

## Files changed

**New**
* `rtc/sse.py` — the whole signalling layer: SSE view, three POST views,
  in-process state, and the `_do_*` / `_all_but_me` helpers.
* `static/lib/htmx.org/dist/ext/hx-sse.js` — htmx 4 SSE extension.
* `SSE_NOTES.md` — this file.

**Changed**
* `rtc_demo/urls.py` — routes for `/sse/<room>/` and `/api/{signal,join,hangup}/`.
* `rtc_demo/asgi.py` — the stock `get_asgi_application()`, wrapped in Django's
  `ASGIStaticFilesHandler` while `DEBUG` is on so a bare ASGI server still
  serves the static assets in dev (pass-through for every non-static path).
* `rtc_demo/settings.py` — removed `channels` / `channels_presence` *and*
  `daphne` from `INSTALLED_APPS` and the `CHANNEL_LAYERS` block; added
  `localhost` / `127.0.0.1` to `ALLOWED_HOSTS` for local dev.
* `requirements.txt` — slimmed to the actual dependency set (Django, asgiref,
  sqlparse, crispy-forms/-bootstrap5, **uvicorn**). The entire
  Channels + daphne/Twisted stack (channels, channels-redis,
  channels-presence, redis, msgpack, daphne, Twisted, autobahn, …) is gone —
  8 packages install where ~30 used to.
* `rtc/admin.py` — emptied; it had registered the removed presence models.
* `static/lib/htmx.org/dist/htmx.js` — upgraded to **htmx 4.0** (`four-dev`).
* `static/js/tr.js` — `tr-ext` + ws-heartbeat replaced by the `rtc`
  DOM-event listener and the (unchanged) `apps` registry.
* `static/js/client.js` — every `$self.ws_json({...})` replaced by a POST;
  added `getCookie`/`api_post`; removed the WS-open handler and heartbeat.
* `rtc/templates/rtc/index.html` — `ws-connect`/`hx-ext="tr-ext, ws"` →
  `hx-sse:connect="/sse/lobby/"` + `hx-swap="none"`; swapped the extension
  script.
* `rtc/templates/rtc/header.html` — added `hx-swap-oob="true"`.

**Deleted**
* `rtc/consumers.py`, `rtc/routing.py`,
  `rtc/management/commands/channel_cleanup.py`, `rtc_demo/apps.py`,
  `static/lib/htmx.org/dist/ext/ws.js`, and the stale v2
  `htmx.min.js` / `htmx.min.js.gz`.

## The real cost of SSE+POST — things Ken's WS code never had to do

These are the honest "tax" findings; with a WebSocket none of them exist.

1. **CSRF on every client→server message.** A WS frame isn't CSRF-checked; a
   POST is. The client now reads the `csrftoken` cookie and sends it as
   `X-CSRFToken` on all three endpoints (`api_post` in `client.js`). Stock
   Django CSRF middleware enforces it on the async views.

2. **An explicit identity, stored in the session.** Channels handed Ken a
   `self.channel_name` for free, and it was the *same* object across his WS
   `connect`/`receive`/`disconnect`. With independent HTTP requests there is no
   such handle, so we mint a `channel_id` (UUID) on first SSE connect and store
   it in the session; every later POST re-reads it from the session to know who
   is calling. (This is the routing key for direct signals and `_all_but_me`.)

3. **Session/auth access from async views.** Reading the session is sync ORM,
   illegal directly inside an async view, so it is wrapped in
   `asgiref.sync.sync_to_async` (`_get_channel_id`). The user is fetched with
   Django 5's native `await request.auser()`. Ken got `scope["user"]`
   pre-resolved by `AuthMiddlewareStack`.

4. **Reconnection idempotency.** htmx's SSE transport auto-reconnects. A WS
   "reconnect" was just a fresh `connect`. Here a fast reconnect can install a
   new queue for a `channel_id` before the old stream's cleanup runs, so:
   * connect drops any stale room membership for the `channel_id` before
     re-registering, and
   * `_cleanup` only tears down if `connections[channel_id] is` *this* stream's
     queue (identity check) — otherwise it would delete the live reconnection
     and wrongly tell peers it left.

5. **Disconnect detection is implicit.** A WS has a clean `disconnect(code)`.
   Here "the user left" == "the SSE generator was cancelled". Cleanup lives in
   the generator's `try/finally`; when the tab closes (or the TCP drops) the
   `finally` removes the channel from its room and broadcasts `disconnected`
   to the remaining peers. (Verified: closing one stream makes the other
   peer's `disconnected` arrive within ~1s.)

6. **Manual message framing & ordering discipline.** The WS layer framed JSON
   for Ken. Here we hand-serialize SSE: split multi-line data into multiple
   `data:` lines, and choose named (`rtc`) vs unnamed (`html`) per the htmx 4
   convention. Ken's ordering note ("html must be sent before the
   connections") is preserved by enqueuing onto the single per-user queue in
   that order — one ordered stream guarantees ordered delivery.

7. **No client heartbeat (a *win*, noted for the A/B).** Ken's client sent a
   `{"signal":"hb"}` every ~25 s to keep `channels_presence` fresh. That is
   gone: presence is just the live SSE connection, and only the *server* sends
   keepalives (`:ka`). One fewer client→server chatter source.

### htmx 4.0 notes (this branch tracks the `four-dev` beta)

* The SSE extension is the new **`hx-sse`** (`src/ext/hx-sse.js`): it uses
  **fetch streaming**, not `EventSource`, and the connect attribute is
  **`hx-sse:connect`** (colon-namespaced). `sse-swap` is gone — unnamed
  messages auto-swap, named ones dispatch as DOM events.
* Extensions self-register globally when loaded (`htmx.config.extensions`
  defaults to allow-all), so no `hx-ext="sse"` is needed.
* `hx-swap-oob` and `hx-swap="none"` behave as before: OOB elements are applied
  regardless of the main swap style, so `hx-swap="none"` on the connecting
  element makes the (empty) main swap a no-op while OOB fragments still land.
* `htmx.config.sse = { pauseOnBackground: false }` is set in `tr.js` so a
  backgrounded tab doesn't drop signalling (a WebSocket wouldn't); auto-reconnect
  stays on.

## Running it

Dependencies are managed with [`uv`](https://docs.astral.sh/uv/), and the app
is served by **uvicorn** (an ASGI server — Django's WSGI `runserver` would
*buffer* the SSE stream). `granian` works equally well.

```bash
uv venv && source .venv/bin/activate
uv pip install -r requirements.txt
python manage.py migrate
uvicorn rtc_demo.asgi:application --host 127.0.0.1 --port 8000
# alternatively:  granian --interface asgi rtc_demo.asgi:application --port 8000
```

Open two browser windows, log in as two different users, both click **Join
Call**. POSTs return **204**; one `/sse/lobby/` stream stays open per tab;
zero WebSocket traffic.

> Do **not** use `python manage.py runserver` here — without daphne it is the
> WSGI dev server and will buffer the event stream. Use uvicorn/granian.

### Running both versions side by side for the A/B

Keep Ken's WS implementation on `main` (or his upstream repo) and this branch
in a second working copy; run them on different ports. Both serve the identical
WebRTC client logic and HTML, so a load generator can drive the two signalling
transports under matching conditions and the only variable is WebSocket vs
SSE+POST.
```bash
git worktree add ../rtc_demo_ws main
# terminal 1 (this branch):  uvicorn rtc_demo.asgi:application --port 8000
# terminal 2 (../rtc_demo_ws, Ken's WS): python manage.py runserver 8001
```
