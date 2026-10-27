import asyncio
import importlib
import logging
import pathlib
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone

import aiohttp
from aiohttp import web


ROOT = pathlib.Path(__file__).resolve().parents[1]
package = types.ModuleType("welink_under_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("welink_under_test", package)
WeLinkClient = importlib.import_module("welink_under_test.welink_client").WeLinkClient
stream_module = importlib.import_module("welink_under_test.welink_stream")
WeLinkStream, StreamGap = stream_module.WeLinkStream, stream_module.StreamGap


def event(key, account="acc_test", created=None, kind="message.received"):
    return {"event_id": key, "account_id": account, "type": kind,
            "created_at": created or datetime.now(timezone.utc).isoformat(),
            "data": {"message_id": "msg_" + key, "type": "text", "text": "fixture"}}


class StreamTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stop = asyncio.Event()
        self.delivered = []
        self.errors = []
        self.log = logging.getLogger("welink-tests")

        async def consume(value):
            self.delivered.append(value["event_id"])

        self.consume = consume

    def stream(self, client):
        return WeLinkStream(client, "acc_test", self.consume, self.stop, self.log,
                            on_error=self.errors.append)

    async def test_cursor_moves_only_after_success_and_never_backwards(self):
        stream = self.stream(None)
        item = event("evt_a")

        async def fail(value):
            raise RuntimeError("enqueue failed")

        stream.consume = fail
        with self.assertRaises(RuntimeError):
            await stream.accept(item)
        self.assertIsNone(stream.cursor)
        self.assertFalse(stream.seen)
        stream.consume = self.consume
        await stream.accept(item)
        await stream.accept(item)
        await stream.accept(event("foreign", account="acc_other"))
        older = datetime.now(timezone.utc) - timedelta(days=1)
        await stream.accept(event("older_control", created=older.isoformat(), kind="account.online"))
        self.assertEqual(stream.cursor, "evt_a")
        self.assertEqual(self.delivered, ["evt_a"])

    async def test_fixed_snapshot_pagination_and_dedup(self):
        items = [event("evt_1"), event("evt_2")]
        calls = []

        class Client:
            async def events(_, **query):
                calls.append(query)
                if len(calls) == 1:
                    return {"items": [items[0]], "has_more": True, "next_cursor": "evt_1"}
                return {"items": items, "has_more": False}

        stream = self.stream(Client())
        await stream.compensate()
        self.assertEqual(self.delivered, ["evt_1", "evt_2"])
        self.assertEqual(calls[0]["until"], calls[1]["until"])
        self.assertEqual(calls[1]["cursor"], "evt_1")
        self.assertEqual(calls[0]["since"], calls[1]["since"])
        self.assertEqual(stream.cursor, "evt_2")

    async def test_stalled_pagination_is_rejected(self):
        class Client:
            async def events(_, **query):
                return {"items": [], "has_more": True}

        with self.assertRaisesRegex(ValueError, "分页未推进"):
            await self.stream(Client()).compensate()

    async def test_overflow_is_bounded_and_gap_cursor_is_not_ack(self):
        values = [event("evt_" + str(i)) for i in range(257)]

        class Socket:
            def __aiter__(self):
                async def frames():
                    import json
                    for value in values:
                        yield types.SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(value))
                return frames()

        stream = self.stream(None)
        queue = asyncio.Queue(maxsize=256)
        with self.assertRaisesRegex(StreamGap, "slow_consumer"):
            await stream._reader(Socket(), queue)
        self.assertEqual(queue.qsize(), 256)
        self.assertIsNone(stream.cursor)

        class GapSocket:
            def __aiter__(self):
                async def frames():
                    yield types.SimpleNamespace(type=aiohttp.WSMsgType.TEXT,
                        data='{"type":"gap","reason":"replay_limit","resume_cursor":"unhandled"}')
                return frames()

        with self.assertRaises(StreamGap):
            await stream._reader(GapSocket(), queue)
        self.assertIsNone(stream.cursor)

    async def start_server(self, app):
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        self.addAsyncCleanup(runner.cleanup)
        return "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])

    async def test_live_socket_auth_ping_during_slow_handler_and_stop(self):
        app = web.Application()
        ping_answered = asyncio.Event()
        handler_started = asyncio.Event()
        release = asyncio.Event()
        delivered = asyncio.Event()
        calls = []

        async def history(request):
            self.assertEqual(request.headers.get("Authorization"), "Bearer fixture-key")
            calls.append(dict(request.query))
            return web.json_response({"code": 0, "data": {"items": [], "has_more": False}})

        async def socket(request):
            self.assertEqual(request.headers.get("Authorization"), "Bearer fixture-key")
            self.assertNotIn("api_key", request.query)
            ws = web.WebSocketResponse(autoping=False)
            await ws.prepare(request)
            await ws.send_json({"type": "connected", "scope": "instance", "account_id": "acc_test"})
            await ws.send_json(event("live"))
            await handler_started.wait()
            await ws.ping(b"test")
            frame = await asyncio.wait_for(ws.receive(), 2)
            if frame.type == aiohttp.WSMsgType.PONG and frame.data == b"test":
                ping_answered.set()
            async for _ in ws:
                pass
            return ws

        app.router.add_get("/v1/events", history)
        app.router.add_get("/v1/accounts/acc_test/ws", socket)
        client = WeLinkClient(await self.start_server(app), "fixture-key")
        self.addAsyncCleanup(client.close)
        stream = self.stream(client)

        async def consume(value):
            handler_started.set()
            await release.wait()
            await self.consume(value)
            delivered.set()

        stream.consume = consume
        task = asyncio.create_task(stream.run())
        try:
            await asyncio.wait_for(ping_answered.wait(), 3)
            self.assertIsNone(stream.cursor)
            release.set()
            await asyncio.wait_for(delivered.wait(), 2)
            await asyncio.sleep(0)
            self.assertEqual(stream.cursor, "live")
            self.assertEqual(len(calls), 1)  # startup compensation only
        finally:
            release.set()
            await stream.close()
            await asyncio.wait_for(task, 3)
        self.assertEqual(self.delivered, ["live"])

    async def test_reconnect_gap_drains_more_than_replay_limit(self):
        app = web.Application()
        items = [event("evt_" + str(i).zfill(4)) for i in range(230)]
        first = asyncio.Event()
        complete = asyncio.Event()
        queries, connections = [], []

        async def history(request):
            queries.append(dict(request.query))
            cursor = request.query.get("cursor")
            if len(connections) < 2:
                return web.json_response({"code": 0, "data": {"items": [], "has_more": False}})
            start = 1 if cursor == items[0]["event_id"] else next(
                (i + 1 for i, item in enumerate(items) if item["event_id"] == cursor), 0)
            page = items[start:start + 100]
            return web.json_response({"code": 0, "data": {"items": page,
                "has_more": start + len(page) < len(items),
                "next_cursor": page[-1]["event_id"] if page else ""}})

        async def socket(request):
            connections.append(request.query.get("cursor"))
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            await ws.send_json({"type": "connected", "scope": "instance", "account_id": "acc_test"})
            if len(connections) == 1:
                await ws.send_json(items[0])
                await first.wait()
            elif len(connections) == 2:
                await ws.send_json({"type": "gap", "reason": "replay_limit", "resume_cursor": "never_ack_this"})
            else:
                complete.set()
                async for _ in ws:
                    pass
                return ws
            await ws.close()
            return ws

        app.router.add_get("/v1/events", history)
        app.router.add_get("/v1/accounts/acc_test/ws", socket)
        client = WeLinkClient(await self.start_server(app), "fixture-key")
        self.addAsyncCleanup(client.close)
        stream = self.stream(client)

        async def consume(value):
            await self.consume(value)
            first.set()

        stream.consume = consume
        task = asyncio.create_task(stream.run())
        try:
            await asyncio.wait_for(complete.wait(), 12)
            self.assertEqual(self.delivered, [item["event_id"] for item in items])
            self.assertEqual(connections[1], items[0]["event_id"])
            self.assertEqual(connections[2], items[-1]["event_id"])
            self.assertNotIn("never_ack_this", [query.get("cursor") for query in queries])
        finally:
            await stream.close()
            await asyncio.wait_for(task, 3)

    async def test_auth_failure_stops_without_polling_fallback(self):
        app = web.Application()

        async def denied(request):
            return web.Response(status=401)

        app.router.add_get("/v1/accounts/acc_test/ws", denied)
        client = WeLinkClient(await self.start_server(app), "fixture-key")
        self.addAsyncCleanup(client.close)
        stream = self.stream(client)
        await asyncio.wait_for(stream.run(), 2)
        self.assertTrue(self.errors)
        self.assertIsNone(stream.cursor)


if __name__ == "__main__":
    unittest.main()
