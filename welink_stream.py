"""Bounded WebSocket reception with HTTP compensation, without AstrBot imports."""

from __future__ import annotations

import asyncio
import json
import random
import time
from collections import OrderedDict
from datetime import datetime, timezone

import aiohttp

from .welink_client import WeLinkError


class StreamGap(Exception):
    pass


class WeLinkStream:
    def __init__(self, client, account_id, consume, stop, logger, limit=100,
                 on_ready=lambda: None, on_error=lambda text: None):
        self.client, self.account_id, self.consume = client, account_id, consume
        self.stop, self.logger, self.limit = stop, logger, limit
        self.on_ready, self.on_error = on_ready, on_error
        self.cursor = None
        self.since = datetime.now(timezone.utc).isoformat()
        self.position = None
        self.seen = OrderedDict()
        self._socket = None
        self._catch_up = True

    async def close(self):
        self.stop.set()
        if self._socket is not None:
            await self._socket.close()

    async def accept(self, event):
        if not isinstance(event, dict) or event.get("account_id") != self.account_id:
            return
        key = event.get("event_id")
        if not isinstance(key, str) or not key:
            raise ValueError("事件缺少 event_id")
        if key in self.seen:
            return
        created = datetime.fromisoformat(str(event.get("created_at", "")).replace("Z", "+00:00"))
        if created.tzinfo is None:
            raise ValueError("事件时间缺少时区")
        # The adapter queues message.received; other instance events are valid
        # cursor positions too. Never acknowledge an event before consumption.
        if event.get("type") == "message.received":
            await self.consume(event)
        self.seen[key] = None
        if len(self.seen) > 2000:
            self.seen.popitem(last=False)
        position = (created, key)
        if self.position is None or position > self.position:
            self.position, self.cursor = position, key
            self.since = created.isoformat()

    async def compensate(self):
        """Drain a fixed HTTP snapshot; no write/sync call to WeChat itself."""
        until = datetime.now(timezone.utc).isoformat()
        cursor = self.cursor
        since = self.since
        while not self.stop.is_set():
            page = await self.client.events(account_id=self.account_id, cursor=cursor,
                                            since=since, until=until, limit=self.limit,
                                            event_type="message.received")
            if not isinstance(page, dict) or not isinstance(page.get("items", []), list):
                raise ValueError("HTTP 补拉返回格式不正确")
            items = page.get("items") or []
            for event in items:
                await self.accept(event)
            if not page.get("has_more", len(items) >= self.limit):
                return
            nxt = page.get("next_cursor") or (items[-1].get("event_id") if items else None)
            if not nxt or nxt == cursor or not items:
                raise ValueError("HTTP 补拉分页未推进")
            cursor = nxt

    async def _reader(self, ws, queue):
        async for frame in ws:
            if frame.type == aiohttp.WSMsgType.TEXT:
                event = json.loads(frame.data)
                if not isinstance(event, dict):
                    raise ValueError("WebSocket 返回格式不正确")
                kind = event.get("type")
                if kind == "connected":
                    if event.get("scope") != "instance" or event.get("account_id") != self.account_id:
                        raise ValueError("WebSocket 实例与配置不一致")
                    self.on_ready()
                elif kind == "pong":
                    continue
                elif kind == "gap":
                    # resume_cursor is the server's write position, not our ACK.
                    raise StreamGap(str(event.get("reason") or "unknown"))
                elif kind == "error":
                    raise WeLinkError(40300, "推送访问被拒绝，请检查实例、额度及 Webhook 绑定")
                else:
                    try:
                        queue.put_nowait(event)
                    except asyncio.QueueFull:
                        raise StreamGap("slow_consumer") from None
            elif frame.type == aiohttp.WSMsgType.ERROR:
                raise ConnectionError("WebSocket 连接异常")
        raise ConnectionError("WebSocket 连接已断开")

    async def _consumer(self, queue):
        if self._catch_up:
            await self.compensate()
            self._catch_up = False
        while True:
            await self.accept(await queue.get())

    async def once(self):
        cm = await self.client.websocket(self.account_id, self.cursor)
        async with cm as ws:
            self._socket = ws
            queue = asyncio.Queue(maxsize=256)
            tasks = [asyncio.create_task(self._reader(ws, queue)),
                     asyncio.create_task(self._consumer(queue)),
                     asyncio.create_task(self.stop.wait())]
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                if not self.stop.is_set():
                    for task in tasks[:2]:
                        if task in done:
                            task.result()
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self._socket = None

    async def run(self):
        delay = 1
        while not self.stop.is_set():
            started, before = time.monotonic(), self.cursor
            try:
                await self.once()
            except asyncio.CancelledError:
                raise
            except StreamGap as error:
                reason = str(error)
                self._catch_up = True
                self.logger.warning("WeLink 推送缺口（%s），尝试 HTTP 补拉；不可见或已清理的历史无法恢复", reason)
                self.on_error("推送缺口：" + reason)
                if reason in ("cursor_expired", "history_hidden"):
                    self.cursor = None  # keep the local time boundary, never rewind seven days
                # Drain after the socket closes, before replaying another 199.
                try:
                    await self.compensate()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.logger.warning("WeLink HTTP 补拉暂未完成，将在重连后重试")
            except aiohttp.WSServerHandshakeError as error:
                if error.status in (400, 401, 403, 404):
                    self.on_error("WebSocket 握手被拒绝，请检查服务版本、API Key 和实例推送模式")
                    self.logger.error("WeLink WebSocket 握手被拒绝（HTTP %s）；请检查服务版本、API Key，并将实例推送设为 WebSocket", error.status)
                    return
                self._catch_up = True
                if error.status == 429:
                    delay = max(delay, 30)
                self.on_error("WebSocket 握手暂不可用")
            except WeLinkError as error:
                if error.code in (40000, 40100, 40300, 40301, 40400):
                    self.on_error(str(error))
                    self.logger.error("WeLink 推送停止：%s", error)
                    return
                self._catch_up = True
                self.on_error(str(error))
            except Exception:
                self._catch_up = True
                self.on_error("WebSocket 已断开，正在重连")
                self.logger.warning("WeLink WebSocket 暂时断开，正在重连")
            if self.stop.is_set():
                return
            if self.cursor != before or time.monotonic() - started >= 30:
                delay = 1
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=delay + random.uniform(0, delay / 4))
            except asyncio.TimeoutError:
                pass
            delay = min(60, delay * 2)
