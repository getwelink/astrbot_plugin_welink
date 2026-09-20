"""WeLink platform adapter: a personal WeChat account as an AstrBot channel.

It polls instead of taking a callback. That is deliberate — most people run
AstrBot at home or in a container with no address the outside world can
reach, and polling removes that requirement entirely. The cost is one HTTP
request per interval, which the platform answers from an indexed keyset
query.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

from astrbot.api import logger
from astrbot.api.event import MessageChain
from astrbot.api.message_components import Image, Plain
from astrbot.api.platform import (
    AstrBotMessage,
    MessageMember,
    MessageType,
    Platform,
    PlatformMetadata,
    register_platform_adapter,
)
from astrbot.core.platform.message_session import MessageSesion

from .welink_client import WeLinkClient, WeLinkError
from .welink_event import WeLinkMessageEvent

# Message kinds that carry a file rather than text.
MEDIA_KINDS = {"image", "voice", "video", "file", "sticker"}

# What to show the model in place of media it cannot see.
KIND_LABEL = {
    "image": "[图片]",
    "voice": "[语音]",
    "video": "[视频]",
    "file": "[文件]",
    "sticker": "[表情]",
    "link": "[链接]",
    "miniapp": "[小程序]",
    "location": "[位置]",
}


@register_platform_adapter(
    "welink",
    "WeLink 微信个人号适配器（轮询，不需要公网地址）",
    default_config_tmpl={
        "base_url": "",
        "api_key": "",
        "account_id": "",
        "poll_interval": 3,
        "poll_limit": 100,
        "download_image": True,
        "group_reply_needs_at": True,
    },
    adapter_display_name="WeLink 微信个人号",
    support_streaming_message=False,
)
class WeLinkPlatformAdapter(Platform):
    def __init__(
        self,
        platform_config: dict,
        platform_settings: dict,
        event_queue: asyncio.Queue,
    ) -> None:
        super().__init__(platform_config, event_queue)
        self.settings = platform_settings

        self.base_url = str(platform_config.get("base_url", "")).strip()
        self.api_key = str(platform_config.get("api_key", "")).strip()
        self.account_id = str(platform_config.get("account_id", "")).strip()
        self.poll_interval = max(1, int(platform_config.get("poll_interval", 3) or 3))
        self.poll_limit = min(200, max(1, int(platform_config.get("poll_limit", 100) or 100)))
        self.download_image = bool(platform_config.get("download_image", True))
        self.group_reply_needs_at = bool(
            platform_config.get("group_reply_needs_at", True)
        )

        self.client = WeLinkClient(self.base_url, self.api_key)
        self._cursor: str | None = None
        self._stop = asyncio.Event()

    # --- lifecycle --------------------------------------------------------

    def meta(self) -> PlatformMetadata:
        return PlatformMetadata(
            "welink",
            "WeLink 微信个人号适配器",
            id=self.config.get("id", "welink"),
            support_streaming_message=False,
            support_proactive_message=True,
        )

    def get_client(self) -> WeLinkClient:
        return self.client

    async def terminate(self) -> None:
        self._stop.set()
        await self.client.close()
        logger.info("WeLink 适配器已关闭")

    async def run(self) -> None:
        if not self.base_url or not self.api_key:
            logger.error("WeLink 适配器没有配置 base_url 或 api_key，不会启动")
            self.record_error("缺少 base_url 或 api_key")
            return

        if not await self._pick_account():
            return

        # Start from now, so restarting the bot does not replay old chats.
        since = datetime.now(timezone.utc).isoformat()
        logger.info(
            "WeLink 适配器开始轮询，实例 %s，间隔 %ds",
            self.account_id,
            self.poll_interval,
        )

        backoff = self.poll_interval
        while not self._stop.is_set():
            try:
                await self._poll_once(since if self._cursor is None else None)
                self.clear_errors()
                backoff = self.poll_interval
            except asyncio.CancelledError:
                raise
            except WeLinkError as e:
                logger.error("WeLink 拉取事件失败：%s", e)
                self.record_error(str(e))
                backoff = min(60, backoff * 2)
            except Exception as e:
                logger.exception("WeLink 轮询出错：%s", e)
                self.record_error(str(e))
                backoff = min(60, backoff * 2)

            try:
                await asyncio.wait_for(self._stop.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass

    async def _pick_account(self) -> bool:
        """Confirm the key works, and choose an account when none was given."""
        try:
            accounts = await self.client.accounts()
        except WeLinkError as e:
            logger.error("WeLink 连不上或者 Key 不对：%s", e)
            self.record_error(str(e))
            return False

        if self.account_id:
            return True
        if not accounts:
            logger.error("WeLink 这个 Key 下面一个实例都没有，先去控制台扫码登录一个")
            self.record_error("没有可用的实例")
            return False

        online = [a for a in accounts if a.get("status") == "online"]
        chosen = (online or accounts)[0]
        self.account_id = chosen.get("account_id", "")
        logger.info(
            "WeLink 没有指定 account_id，自动选了 %s（%s）",
            self.account_id,
            chosen.get("name") or chosen.get("status"),
        )
        return bool(self.account_id)

    # --- polling ----------------------------------------------------------

    async def _poll_once(self, since: str | None) -> None:
        data = await self.client.events(
            account_id=self.account_id,
            cursor=self._cursor,
            since=since,
            limit=self.poll_limit,
            event_type="message.received",
        )
        items = data.get("items") or []

        # The cursor only moves when something actually came back. An empty
        # page has confirmed nothing, so moving past it would step over a
        # message that had not arrived yet when the query ran.
        if items:
            nxt = data.get("next_cursor")
            if nxt:
                self._cursor = nxt

        for event in items:
            try:
                await self._handle(event)
            except Exception as e:
                logger.exception("WeLink 处理事件出错，跳过这一条：%s", e)

    async def _handle(self, event: dict[str, Any]) -> None:
        if event.get("type") != "message.received":
            return
        data = event.get("data") or {}

        # Messages this account sent from another device come back too.
        # Acting on them would make the bot answer itself.
        if data.get("self"):
            return

        abm = await self._convert(data)
        if abm is None:
            return

        if (
            abm.type == MessageType.GROUP_MESSAGE
            and self.group_reply_needs_at
            and not data.get("mentions_me")
        ):
            return

        self.commit_event(
            WeLinkMessageEvent(
                message_str=abm.message_str,
                message_obj=abm,
                platform_meta=self.meta(),
                session_id=abm.session_id,
                client=self.client,
                account_id=self.account_id,
                chat_id=data.get("chat_id", ""),
            )
        )

    async def _convert(self, data: dict[str, Any]) -> AstrBotMessage | None:
        chat_id = data.get("chat_id")
        if not chat_id:
            return None

        is_group = bool(data.get("is_group"))
        kind = data.get("type") or "text"
        text = data.get("text") or ""

        abm = AstrBotMessage()
        abm.type = MessageType.GROUP_MESSAGE if is_group else MessageType.FRIEND_MESSAGE
        abm.self_id = self.account_id
        abm.message_id = str(data.get("message_id") or "")
        abm.session_id = str(chat_id)
        abm.sender = MessageMember(user_id=str(data.get("sender") or data.get("from") or ""))
        abm.raw_message = data
        abm.timestamp = int(data.get("created_ts") or time.time())
        if is_group:
            abm.group_id = str(chat_id)

        if kind == "text":
            abm.message = [Plain(text)]
            abm.message_str = text
            return abm

        if kind == "image" and self.download_image and abm.message_id:
            url = None
            try:
                url = await self.client.media_url(self.account_id, abm.message_id)
            except WeLinkError as e:
                logger.warning("WeLink 取图片地址失败，当作文字处理：%s", e)
            if url:
                abm.message = [Image.fromURL(url)]
                abm.message_str = text or ""
                return abm

        # Anything else reaches the model as a label, so it at least knows
        # something arrived rather than seeing an empty turn.
        label = KIND_LABEL.get(kind, f"[{kind}]")
        body = f"{label}{text}" if text else label
        abm.message = [Plain(body)]
        abm.message_str = body
        return abm

    # --- proactive --------------------------------------------------------

    async def send_by_session(
        self, session: MessageSesion, message_chain: MessageChain
    ) -> None:
        event = WeLinkMessageEvent(
            message_str="",
            message_obj=AstrBotMessage(),
            platform_meta=self.meta(),
            session_id=session.session_id,
            client=self.client,
            account_id=self.account_id,
            chat_id=session.session_id,
        )
        await event._send_chain(message_chain, session.session_id)
        await super().send_by_session(session, message_chain)
