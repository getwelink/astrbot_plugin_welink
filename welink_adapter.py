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
from collections import OrderedDict, deque
from datetime import datetime, timezone
from typing import Any

from astrbot.api import logger
from astrbot.api.event import MessageChain
from astrbot.api.message_components import At, Image, Plain
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

# How many handled events to remember, to never answer the same one twice.
# A poll returns at most 200; this covers the last several of them.
SEEN_MAX = 2000

# Message kinds that are notices about a chat rather than something somebody
# said in it: someone joined, a message was withdrawn, a friend request came
# in. A bot that answers these talks to nobody — or to WeChat itself.
NOTICE_KINDS = {"system", "revoke", "friend_request"}

# Accounts that are WeChat rather than a person. The WeChat team account
# answers anything sent to it with the same canned line, which the bot would
# answer, which it would answer: 105 rounds in eight minutes before anyone
# noticed. Official accounts (gh_...) auto-reply the same way.
SYSTEM_ACCOUNTS = frozenset({
    "weixin", "fmessage", "medianote", "floatbottle", "qqmail", "qmessage",
    "qqsync", "tmessage", "newsapp", "notifymessage", "notification_messages",
    "brandsessionholder", "brandservicesessionholder", "officialaccounts",
    "mphelper", "voipnotify", "exmail_tool", "userexperience_alarm",
    "helper_entry", "weibo", "qqfriend", "lbsapp", "shakeapp", "blogapp",
    "masssendapp", "feedsapp", "cardpackage", "wxitil",
})

# The loop guard: the most times one chat may set the bot off in a minute,
# and how long the chat is ignored once it goes over.
LOOP_WINDOW = 60.0
LOOP_PAUSE = 180.0


# What WeChat puts after a name it inserted with @: a four-per-em space, not
# an ordinary one. Stripping the mention has to know it.
MENTION_SEP = "\u2005"


def wake_prefixes() -> list[str]:
    """The prefixes AstrBot answers to in a group, as its own settings say.

    Read each time rather than once, so a change in the WebUI applies at
    once. Should the settings not be reachable, "/" is what AstrBot ships
    with.
    """
    try:
        from astrbot.core import astrbot_config

        prefixes = astrbot_config.get("wake_prefix") or []
    except Exception:
        prefixes = []
    if isinstance(prefixes, str):
        prefixes = [prefixes]
    return [p for p in prefixes if isinstance(p, str) and p] or ["/"]


def strip_self_mention(text: str, names: set[str], alone: bool) -> str:
    """Take the "@bot" out of a message that mentions the bot.

    WeChat writes the mention into the text as "@name" and a four-per-em
    space. AstrBot recognises a command only when the text starts with its
    prefix, so "@ink /help" has to reach it as "/help", with the mention
    carried alongside as an At instead.

    The name is the one the bot has in that group, which is not always its
    nickname. When none of the names we know matches and the bot is the only
    one mentioned, the leading mention must be it, whatever it is called.
    """
    out = text
    for name in names:
        if not name:
            continue
        tag = "@" + name
        for sep in (MENTION_SEP, " "):
            out = out.replace(tag + sep, "")
        trimmed = out.rstrip()
        if trimmed.endswith(tag):
            out = trimmed[: -len(tag)]
    if out == text and alone and text.startswith("@"):
        cut = text.find(MENTION_SEP)
        if cut > 0:
            out = text[cut + 1 :]
    return out.strip()


def from_wechat_itself(chat_id: str, sender: str) -> bool:
    """Whether a message comes from WeChat or an official account, not a person."""
    for who in (chat_id, sender):
        if who in SYSTEM_ACCOUNTS or who.startswith("gh_"):
            return True
    return False


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
    "WeLink 微信个人号适配器，通过轮询获取消息，不需要公网地址",
    default_config_tmpl={
        "base_url": "",
        "api_key": "",
        "account_id": "",
        "poll_interval": 3,
        "poll_limit": 100,
        "download_image": True,
        "group_reply_needs_at": True,
        "loop_guard_per_minute": 12,
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
        guard = platform_config.get("loop_guard_per_minute", 12)
        self.loop_guard = max(0, int(12 if guard is None else guard))

        self.client = WeLinkClient(self.base_url, self.api_key)
        self._cursor: str | None = None
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._triggers: dict[str, deque[float]] = {}
        # Who the bot is in WeChat: its wxid, and the names people @ it by.
        self._self_wxid = ""
        self._self_names: set[str] = set()
        self._paused_until: dict[str, float] = {}
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
            logger.error("WeLink 适配器缺少 base_url 或 api_key，未启动")
            self.record_error("缺少 base_url 或 api_key")
            return

        if not await self._pick_account():
            return

        # Start from now, so restarting the bot does not replay old chats.
        since = datetime.now(timezone.utc).isoformat()
        logger.info(
            "WeLink 适配器已开始拉取消息，实例 %s，间隔 %d 秒",
            self.account_id,
            self.poll_interval,
        )

        backoff = self.poll_interval
        while not self._stop.is_set():
            try:
                more = await self._poll_once(since if self._cursor is None else None)
                self.clear_errors()
                backoff = self.poll_interval
                if more:
                    # A full page: what is behind it has already happened, so
                    # read it now rather than one interval late.
                    continue
            except asyncio.CancelledError:
                raise
            except WeLinkError as e:
                logger.error("WeLink 拉取消息失败：%s", e)
                self.record_error(str(e))
                backoff = min(60, backoff * 2)
            except Exception as e:
                logger.exception("WeLink 拉取消息时出错：%s", e)
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
            logger.error("无法连接 WeLink 服务，或 API Key 不正确：%s", e)
            self.record_error(str(e))
            return False

        if not self.account_id:
            if not accounts:
                logger.error("这个 WeLink API Key 下没有任何实例，请先在 WeLink 控制台扫码登录一个微信号")
                self.record_error("没有可用的实例")
                return False
            online = [a for a in accounts if a.get("status") == "online"]
            chosen = (online or accounts)[0]
            self.account_id = chosen.get("account_id", "")
            logger.info(
                "WeLink 未指定 account_id，已自动选择实例 %s（%s）",
                self.account_id,
                chosen.get("name") or chosen.get("status"),
            )

        # The bot's own wxid and nickname, for recognising "@bot" in groups.
        # Each message also names the receiving account, so this is only
        # the starting point.
        for a in accounts:
            if a.get("account_id") == self.account_id:
                profile = a.get("profile") or {}
                self._self_wxid = str(profile.get("wxid") or "")
                if profile.get("nickname"):
                    self._self_names.add(str(profile["nickname"]))
        return bool(self.account_id)

    # --- polling ----------------------------------------------------------

    async def _poll_once(self, since: str | None) -> bool:
        """Read one page and hand it on. Returns whether more is waiting."""
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
        #
        # Where it moves to is the last event on the page. The platform says
        # so in next_cursor, but versions before 2026-09-27 said it only for a
        # full page: a short one came back with none, the cursor stayed put,
        # and every poll returned the same messages — one "/help" answered
        # every three seconds. The last event's own id is the same position,
        # so it stands in whenever next_cursor is missing.
        if items:
            nxt = data.get("next_cursor") or items[-1].get("event_id")
            if nxt:
                self._cursor = nxt

        for event in items:
            if self._already_handled(event):
                continue
            try:
                await self._handle(event)
            except Exception as e:
                logger.exception("WeLink 处理消息时出错，已跳过这条消息：%s", e)

        more = data.get("has_more")
        if more is None:
            more = len(items) >= self.poll_limit
        return bool(more)

    def _already_handled(self, event: dict[str, Any]) -> bool:
        """Remember each event, and say whether it has been seen before.

        The cursor is what keeps a page from being read twice; this is what
        keeps a reply from being sent twice when something gets past the
        cursor anyway — a replay, a retry, a platform that forgets to move it.
        Answering twice is the one failure a chat bot cannot take back.
        """
        key = event.get("event_id") or (event.get("data") or {}).get("message_id")
        if not key:
            return False
        if key in self._seen:
            return True
        self._seen[key] = None
        if len(self._seen) > SEEN_MAX:
            self._seen.popitem(last=False)
        return False

    async def _handle(self, event: dict[str, Any]) -> None:
        if event.get("type") != "message.received":
            return
        data = event.get("data") or {}

        # Messages this account sent from another device come back too.
        # Acting on them would make the bot answer itself.
        if data.get("self"):
            return

        # Notices, and anything from WeChat or an official account, are not
        # somebody talking to the bot. Answering them starts a conversation
        # with an auto-reply that never ends.
        if (data.get("type") or "text") in NOTICE_KINDS:
            return
        if from_wechat_itself(str(data.get("chat_id") or ""), str(data.get("sender") or "")):
            return

        # Decided before converting: converting a picture asks the platform
        # for a download address, and a group full of pictures nobody sent to
        # the bot should not cost a request each.
        if (
            data.get("is_group")
            and self.group_reply_needs_at
            and not self._addressed_in_group(data)
        ):
            return

        if self._looping(str(data.get("chat_id") or "")):
            return

        abm = await self._convert(data)
        if abm is None:
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

    def _addressed_in_group(self, data: dict[str, Any]) -> bool:
        """Whether a group message is meant for the bot.

        Either it @s the bot, or it starts with one of AstrBot's wake
        prefixes. The second used to be missing, and a plain "/help" in a
        group never reached AstrBot at all.
        """
        if data.get("mentions_me"):
            return True
        text = str(data.get("text") or "").lstrip()
        return any(text.startswith(p) for p in wake_prefixes())

    def _looping(self, chat_id: str) -> bool:
        """Whether this chat has set the bot off too often to be a person.

        The filters above catch the auto-replies we know of. This catches the
        ones we do not — a friend's own bot, a shop's auto-responder — by the
        one thing every such loop has in common: it goes faster than anybody
        types. Past the limit the chat is left alone for a few minutes, which
        breaks the loop, and a person who really was that quick loses nothing
        but a short wait.
        """
        if not self.loop_guard or not chat_id:
            return False
        now = time.monotonic()
        if now < self._paused_until.get(chat_id, 0.0):
            return True
        recent = self._triggers.setdefault(chat_id, deque())
        recent.append(now)
        while recent and now - recent[0] > LOOP_WINDOW:
            recent.popleft()
        if len(recent) > self.loop_guard:
            recent.clear()
            self._paused_until[chat_id] = now + LOOP_PAUSE
            logger.warning(
                "WeLink 会话 %s 在一分钟内触发机器人超过 %d 次，可能是在和其他自动回复互相回复，"
                "暂停处理该会话 %d 分钟",
                chat_id,
                self.loop_guard,
                int(LOOP_PAUSE // 60),
            )
            return True
        return False

    async def _convert(self, data: dict[str, Any]) -> AstrBotMessage | None:
        chat_id = data.get("chat_id")
        if not chat_id:
            return None

        is_group = bool(data.get("is_group"))
        kind = data.get("type") or "text"
        text = data.get("text") or ""

        # The bot's own wxid, not the instance id: AstrBot takes an At as
        # addressed to the bot only when the two are the same, and people
        # @ the wxid. Every inbound message names it as the recipient.
        self_wxid = str(data.get("to") or "") or self._self_wxid
        if self_wxid:
            self._self_wxid = self_wxid

        abm = AstrBotMessage()
        abm.type = MessageType.GROUP_MESSAGE if is_group else MessageType.FRIEND_MESSAGE
        abm.self_id = self_wxid or self.account_id
        abm.message_id = str(data.get("message_id") or "")
        abm.session_id = str(chat_id)
        abm.sender = MessageMember(user_id=str(data.get("sender") or data.get("from") or ""))
        abm.raw_message = data
        abm.timestamp = int(data.get("created_ts") or time.time())
        if is_group:
            abm.group_id = str(chat_id)

        # In a group, an @ to the bot goes to AstrBot as an At in front, and
        # the "@name" comes out of the text. Otherwise AstrBot sees a message
        # that neither starts with its prefix nor carries an At for it, and
        # ignores it — which is what every "@bot /help" in a group got.
        mention: list[Any] = []
        if is_group and data.get("mentions_me") and abm.self_id:
            mentions = [str(m) for m in (data.get("mentions") or [])]
            text = strip_self_mention(text, self._self_names, mentions == [abm.self_id])
            mention = [At(qq=abm.self_id, name=next(iter(self._self_names), ""))]

        if kind == "text":
            abm.message = mention + [Plain(text)]
            abm.message_str = text
            return abm

        if kind == "image" and self.download_image and abm.message_id:
            url = None
            try:
                url = await self.client.media_url(self.account_id, abm.message_id)
            except WeLinkError as e:
                logger.warning("WeLink 获取图片地址失败，已按文字消息处理：%s", e)
            if url:
                abm.message = mention + [Image.fromURL(url)]
                abm.message_str = text or ""
                return abm

        # Anything else reaches the model as a label, so it at least knows
        # something arrived rather than seeing an empty turn.
        label = KIND_LABEL.get(kind, f"[{kind}]")
        body = f"{label}{text}" if text else label
        abm.message = mention + [Plain(body)]
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
