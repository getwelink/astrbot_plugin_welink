"""Adapter integration checks with a minimal AstrBot API double, no model calls."""
import asyncio
import importlib
import logging
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

from test_stream import event  # also installs an isolated package import path


defaults = {}


class Platform:
    def __init__(self, config, queue):
        self.config = config
        self.errors = []

    def record_error(self, error):
        self.errors.append(error)

    def clear_errors(self):
        self.errors.clear()


def register(*args, **kwargs):
    defaults.update(kwargs["default_config_tmpl"])
    return lambda cls: cls


for name, values in {
    "astrbot": {}, "astrbot.api": {"logger": logging.getLogger("adapter-tests")},
    "astrbot.api.event": {"MessageChain": object},
    "astrbot.api.message_components": {"At": object, "Image": object, "Plain": object},
    "astrbot.api.platform": {"Platform": Platform, "AstrBotMessage": object,
        "MessageMember": object, "MessageType": object, "PlatformMetadata": object,
        "register_platform_adapter": register},
    "astrbot.core": {}, "astrbot.core.platform": {},
    "astrbot.core.platform.message_session": {"MessageSesion": object},
    "welink_under_test.welink_event": {"WeLinkMessageEvent": object},
}.items():
    module = types.ModuleType(name)
    for key, value in values.items():
        setattr(module, key, value)
    sys.modules[name] = module

module = importlib.import_module("welink_under_test.welink_adapter")
Adapter = module.WeLinkPlatformAdapter


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    def build(self, **config):
        return Adapter({"base_url": "https://example.invalid", "api_key": "fixture",
                        "account_id": "acc_test", **config}, {}, asyncio.Queue())

    async def test_new_template_uses_ws_old_config_keeps_polling(self):
        self.assertEqual(defaults["receive_mode"], "websocket")
        self.assertEqual(self.build().receive_mode, "polling")
        self.assertEqual(self.build(receive_mode="websocket").receive_mode, "websocket")

    async def test_websocket_mode_does_not_start_poll_loop(self):
        adapter = self.build(receive_mode="websocket")
        adapter._pick_account = AsyncMock(return_value=True)
        adapter._poll_once = AsyncMock()
        stream = types.SimpleNamespace(run=AsyncMock())
        with patch.object(module, "WeLinkStream", return_value=stream):
            await adapter.run()
        stream.run.assert_awaited_once()
        adapter._poll_once.assert_not_awaited()

    async def test_failed_poll_handler_does_not_skip_or_dedup_failed_event(self):
        adapter = self.build()
        items = [event("first"), event("second")]
        adapter.client = types.SimpleNamespace(events=AsyncMock(return_value={"items": items, "has_more": False}))
        processed = []

        async def fail_second(value):
            if value["event_id"] == "second":
                raise RuntimeError("enqueue failed")
            processed.append(value["event_id"])

        adapter._handle = fail_second
        with self.assertRaises(RuntimeError):
            await adapter._poll_once(None)
        self.assertEqual(adapter._cursor, "first")
        self.assertNotIn("second", adapter._seen)

        async def consume(value):
            processed.append(value["event_id"])

        adapter._handle = consume
        await adapter._poll_once(None)
        self.assertEqual(processed, ["first", "second"])
        self.assertEqual(adapter._cursor, "second")

    async def test_cancelled_handler_remains_retryable(self):
        adapter = self.build()
        adapter._handle = AsyncMock(side_effect=asyncio.CancelledError)
        with self.assertRaises(asyncio.CancelledError):
            await adapter._consume_event(event("cancelled"))
        self.assertNotIn("cancelled", adapter._seen)

    async def test_termination_closes_stream_and_http(self):
        adapter = self.build(receive_mode="websocket")
        adapter._stream = types.SimpleNamespace(close=AsyncMock())
        adapter.client = types.SimpleNamespace(close=AsyncMock())
        await adapter.terminate()
        self.assertTrue(adapter._stop.is_set())
        adapter._stream.close.assert_awaited_once()
        adapter.client.close.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
