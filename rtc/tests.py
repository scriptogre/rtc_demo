"""One runnable check for the non-trivial signalling logic: peer notification
ordering, disconnect broadcast, and SSE framing. No server, no DB, no
fixtures -- it drives rtc/sse.py against its in-process state directly.

    python manage.py test rtc
"""
import asyncio
import json

from django.test import SimpleTestCase

from rtc import sse


def _drain(queue):
    out = []
    while not queue.empty():
        out.append(queue.get_nowait())
    return out


class SignallingTests(SimpleTestCase):
    def setUp(self):
        sse.connections.clear()
        sse.rooms.clear()
        sse.channel_meta.clear()

    def _connect(self, cid, name, room='lobby'):
        queue = asyncio.Queue()
        sse.connections[cid] = queue
        sse.channel_meta[cid] = {
            'user_name': name, 'short_name': sse._short_name(cid), 'room': room,
        }
        sse.rooms.setdefault(room, set()).add(cid)
        return queue

    async def test_join_sends_peer_html_before_rtc(self):
        qa = self._connect('aaa-111111', 'Alice')
        self._connect('bbb-222222', 'Bob')
        await sse._do_join('aaa-111111', 'video')   # Alice joins, no peers yet
        _drain(qa)
        await sse._do_join('bbb-222222', 'video')   # Bob joins -> Alice notified

        events = _drain(qa)
        names = [name for name, _ in events]
        # Ken's invariant: the panel html must arrive before the rtc 'other'.
        self.assertLess(names.index('html'), names.index('rtc'))
        self.assertIn('peer-222222', events[names.index('html')][1])
        other = json.loads(events[names.index('rtc')][1])['rtc']
        self.assertEqual(other['type'], 'other')
        self.assertEqual(other['channel_name'], 'bbb-222222')

    async def test_disconnect_broadcasts_to_peers(self):
        qa = self._connect('aaa-111111', 'Alice', room='video')
        qb = self._connect('bbb-222222', 'Bob', room='video')
        await sse._cleanup('aaa-111111', qa)        # Alice's SSE stream drops

        self.assertNotIn('aaa-111111', sse.connections)
        msg = json.loads(_drain(qb)[-1][1])['rtc']
        self.assertEqual(msg['type'], 'disconnected')
        self.assertEqual(msg['channel_name'], 'aaa-111111')

    def test_format_sse_named_and_multiline(self):
        # html is unnamed (htmx auto-swaps it); multi-line -> separate data: lines
        self.assertEqual(sse._format_sse('html', 'a\nb'), 'data: a\ndata: b\n\n')
        self.assertEqual(sse._format_sse('rtc', '{}'), 'event: rtc\ndata: {}\n\n')
