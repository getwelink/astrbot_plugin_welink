"""Sending side: turn an AstrBot message chain into WeLink calls."""

from __future__ import annotations

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.message_components import At, File, Image, Plain, Record
from astrbot.api.platform import AstrBotMessage, PlatformMetadata

from .welink_client import WeLinkClient, WeLinkError


_LOCAL_FILE_HINT = (
    "本地文件要先配置 callback_api_base，而且那个地址必须是 WeLink 服务端"
    "能访问到的——它是去下载，不是你上传给它。"
)


class WeLinkMessageEvent(AstrMessageEvent):
    def __init__(
        self,
        message_str: str,
        message_obj: AstrBotMessage,
        platform_meta: PlatformMetadata,
        session_id: str,
        client: WeLinkClient,
        account_id: str,
        chat_id: str,
    ) -> None:
        super().__init__(message_str, message_obj, platform_meta, session_id)
        self.client = client
        self.account_id = account_id
        self.chat_id = chat_id

    async def send(self, message: MessageChain) -> None:
        await self._send_chain(message, self.chat_id)
        await super().send(message)

    async def _send_chain(self, message: MessageChain, to: str) -> None:
        """Send one chain, collapsing the text parts into a single message.

        Text arrives split across several Plain components, and sending each
        one separately would arrive as several bubbles in the chat. Mentions
        are collected as the chain is walked and ride along with that text,
        because WeChat only shows an @ when it sits on the message that names
        the person.
        """
        text_parts: list[str] = []
        mentions: list[str] = []

        for comp in message.chain:
            if isinstance(comp, Plain):
                if comp.text:
                    text_parts.append(comp.text)
            elif isinstance(comp, At):
                who = str(comp.qq)
                if who and who not in mentions:
                    mentions.append(who)
                name = getattr(comp, "name", None)
                text_parts.append(f"@{name} " if name else "")
            elif isinstance(comp, Image):
                await self._flush_text(to, text_parts, mentions)
                await self._send_image(comp, to)
            elif isinstance(comp, Record):
                await self._flush_text(to, text_parts, mentions)
                await self._send_media(comp, to, self.client.send_voice, "语音")
            elif isinstance(comp, File):
                await self._flush_text(to, text_parts, mentions)
                await self._send_file(comp, to)
            else:
                logger.debug("WeLink 适配器跳过了不支持的消息段：%s", type(comp).__name__)

        await self._flush_text(to, text_parts, mentions)

    async def _flush_text(
        self, to: str, parts: list[str], mentions: list[str]
    ) -> None:
        text = "".join(parts).strip()
        parts.clear()
        if not text:
            mentions.clear()
            return
        sending = list(mentions)
        mentions.clear()
        try:
            await self.client.send_text(self.account_id, to, text, sending or None)
        except WeLinkError as e:
            logger.error("WeLink 发送文字失败：%s", e)

    @staticmethod
    def _candidate_urls(comp) -> list[str]:
        """Places a component may be keeping an address.

        Image.fromURL builds Image(file=url) — it fills `file`, not `url` —
        so looking only at `url` sends an image that is already public off
        to the local file service for no reason. File is the exception:
        there `.file` is a property that downloads, so it is left alone and
        only `.url` is read.
        """
        out = [getattr(comp, "url", None) or ""]
        if not isinstance(comp, File):
            out.append(getattr(comp, "file", None) or "")
        return [c for c in out if isinstance(c, str)]

    async def _resolve_url(self, comp) -> str | None:
        """Get an address the WeLink server can fetch.

        WeLink takes a URL and downloads it itself; it has no upload endpoint.
        So a component already holding an http(s) address is used as it is,
        and a local file has to be published first. AstrBot's own file service
        does that — but only when callback_api_base is set, and only to an
        address the WeLink server can actually reach.

        Image, Record, Video and File all carry register_to_file_service(),
        so publishing is one call; only the http shortcut comes first.
        """
        for cand in self._candidate_urls(comp):
            if cand.startswith(("http://", "https://")):
                return cand

        try:
            return await comp.register_to_file_service()
        except Exception as e:
            logger.error("WeLink 发布本地文件失败：%s。%s", e, _LOCAL_FILE_HINT)
            return None

    async def _send_image(self, comp, to: str) -> None:
        url = await self._resolve_url(comp)
        if not url:
            return
        try:
            await self.client.send_image(self.account_id, to, url)
        except WeLinkError as e:
            logger.error("WeLink 发送图片失败：%s", e)

    async def _send_file(self, comp, to: str) -> None:
        url = await self._resolve_url(comp)
        if not url:
            return
        try:
            await self.client.send_file(
                self.account_id, to, url, getattr(comp, "name", None)
            )
        except WeLinkError as e:
            logger.error("WeLink 发送文件失败：%s", e)

    async def _send_media(self, comp, to: str, sender, label: str) -> None:
        url = await self._resolve_url(comp)
        if not url:
            return
        try:
            await sender(self.account_id, to, url)
        except WeLinkError as e:
            logger.error("WeLink 发送%s失败：%s", label, e)
