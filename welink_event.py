"""Sending side: turn an AstrBot message chain into WeLink calls."""

from __future__ import annotations

import mimetypes
import os

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.message_components import At, File, Image, Plain, Record
from astrbot.api.platform import AstrBotMessage, PlatformMetadata

from .welink_client import WeLinkClient, WeLinkError



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
                logger.debug("WeLink 暂不支持发送这类消息，已跳过：%s", type(comp).__name__)

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

    async def _attachment(self, comp) -> dict[str, str] | None:
        """Work out how to name this file to the platform.

        An address it can already fetch is passed straight through. Anything
        else is read off disk and uploaded, and the send names the handle
        that comes back. That is the whole reason this adapter no longer
        needs callback_api_base: nothing has to be reachable from outside.
        """
        for cand in self._candidate_urls(comp):
            if cand.startswith(("http://", "https://")):
                return {"url": cand}

        try:
            path = await comp.convert_to_file_path()
        except Exception as e:
            logger.error("WeLink 无法读取文件，这部分内容未发送：%s", e)
            return None

        try:
            with open(path, "rb") as fh:
                body = fh.read()
        except OSError as e:
            logger.error("WeLink 无法读取文件，这部分内容未发送：%s", e)
            return None
        if not body:
            logger.error("WeLink 文件内容为空，未发送")
            return None

        name = os.path.basename(path) or "file"
        try:
            media_id = await self.client.upload(
                self.account_id, name, mimetypes.guess_type(name)[0] or "", body
            )
        except WeLinkError as e:
            logger.error("WeLink 文件上传失败，这部分内容未发送：%s", e)
            return None
        if not media_id:
            logger.error("WeLink 文件上传后没有返回 media_id，这部分内容未发送")
            return None
        return {"media_id": media_id}

    async def _send_image(self, comp, to: str) -> None:
        what = await self._attachment(comp)
        if not what:
            return
        try:
            await self.client.send_image(self.account_id, to, **what)
        except WeLinkError as e:
            logger.error("WeLink 发送图片失败：%s", e)

    async def _send_file(self, comp, to: str) -> None:
        what = await self._attachment(comp)
        if not what:
            return
        try:
            await self.client.send_file(
                self.account_id, to, getattr(comp, "name", None), **what
            )
        except WeLinkError as e:
            logger.error("WeLink 发送文件失败：%s", e)

    async def _send_media(self, comp, to: str, sender, label: str) -> None:
        what = await self._attachment(comp)
        if not what:
            return
        try:
            await sender(self.account_id, to, **what)
        except WeLinkError as e:
            logger.error("WeLink 发送%s失败：%s", label, e)
