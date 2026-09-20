"""A thin async client over the WeLink HTTP API.

Only the handful of calls this adapter needs. Every response comes back in the
same envelope — {"code", "message", "data", "request_id"} — so unwrapping it
and raising on a non-zero code happens in exactly one place.
"""

from __future__ import annotations

from typing import Any

import aiohttp


class WeLinkError(Exception):
    """A call that the platform answered with a non-zero code."""

    def __init__(self, code: int, message: str, request_id: str = "") -> None:
        super().__init__(f"[{code}] {message}" + (f" (request_id={request_id})" if request_id else ""))
        self.code = code
        self.message = message
        self.request_id = request_id


class WeLinkClient:
    def __init__(self, base_url: str, api_key: str, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        if not self.base_url.endswith("/v1"):
            self.base_url += "/v1"
        self.api_key = api_key
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: aiohttp.ClientSession | None = None

    async def _ensure(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            # Only the key is a session-wide header. The content type belongs
            # to the request: json for the ordinary calls, multipart for an
            # upload, and forcing one here would break the other.
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                headers={"Authorization": f"Bearer {self.api_key}"},
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def call(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> Any:
        session = await self._ensure()
        clean = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        async with session.request(
            method, self.base_url + path, params=clean or None, json=body
        ) as resp:
            # A non-JSON body means something in front of the service answered
            # instead of the service; say so rather than raising a parse error.
            try:
                payload = await resp.json(content_type=None)
            except Exception as exc:
                text = (await resp.text())[:200]
                raise WeLinkError(resp.status, f"返回的不是 JSON：{text}") from exc

        if not isinstance(payload, dict):
            raise WeLinkError(resp.status, f"返回的结构不对：{payload!r}"[:200])

        code = payload.get("code")
        if code != 0:
            raise WeLinkError(
                int(code) if isinstance(code, int) else resp.status,
                str(payload.get("message", "")),
                str(payload.get("request_id", "")),
            )
        return payload.get("data")

    # --- the calls the adapter makes -------------------------------------

    async def accounts(self) -> list[dict[str, Any]]:
        data = await self.call("GET", "/accounts")
        if isinstance(data, dict):
            return data.get("items") or []
        return data or []

    async def events(
        self,
        *,
        account_id: str | None = None,
        cursor: str | None = None,
        since: str | None = None,
        limit: int = 100,
        event_type: str | None = None,
    ) -> dict[str, Any]:
        data = await self.call(
            "GET",
            "/events",
            params={
                "account_id": account_id,
                "cursor": cursor,
                "since": since,
                "limit": limit,
                "type": event_type,
                "order": "oldest",
            },
        )
        return data or {}

    async def send_text(
        self,
        account_id: str,
        to: str,
        content: str,
        mentions: list[str] | None = None,
    ) -> Any:
        body: dict[str, Any] = {"to": to, "content": content}
        if mentions:
            body["mentions"] = mentions
        return await self.call(
            "POST", f"/accounts/{account_id}/messages/text", body=body
        )

    async def send_image(self, account_id: str, to: str, **what: str) -> Any:
        """what is either url=... or media_id=..., whichever the caller has."""
        return await self.call(
            "POST", f"/accounts/{account_id}/messages/image",
            body={"to": to, **what},
        )

    async def send_file(
        self, account_id: str, to: str, filename: str | None = None, **what: str
    ) -> Any:
        body: dict[str, Any] = {"to": to, **what}
        if filename:
            body["filename"] = filename
        return await self.call(
            "POST", f"/accounts/{account_id}/messages/file", body=body
        )

    async def send_voice(self, account_id: str, to: str, **what: str) -> Any:
        return await self.call(
            "POST", f"/accounts/{account_id}/messages/voice", body={"to": to, **what},
        )

    async def upload(
        self,
        account_id: str,
        filename: str,
        content_type: str,
        body: bytes,
    ) -> str | None:
        """Hand the bytes over and get a handle back.

        This is what removes the need for AstrBot to publish local files at
        all: a picture the model just produced goes straight up, and the send
        that follows names it by handle.
        """
        form = aiohttp.FormData()
        form.add_field("file", body, filename=filename or "file",
                       content_type=content_type or "application/octet-stream")
        session = await self._ensure()
        url = f"{self.base_url}/accounts/{account_id}/media/upload"
        async with session.post(url, data=form) as resp:
            try:
                payload = await resp.json(content_type=None)
            except Exception as exc:
                text = (await resp.text())[:200]
                raise WeLinkError(resp.status, f"上传返回的不是 JSON：{text}") from exc
        if not isinstance(payload, dict) or payload.get("code") != 0:
            raise WeLinkError(
                int(payload.get("code") or resp.status) if isinstance(payload, dict) else resp.status,
                str((payload or {}).get("message", "")),
                str((payload or {}).get("request_id", "")),
            )
        return ((payload.get("data") or {}).get("media_id")) or None

    async def media_url(self, account_id: str, message_id: str) -> str | None:
        """Ask for a downloadable address for the media on an inbound message."""
        data = await self.call(
            "POST",
            f"/accounts/{account_id}/media/download",
            body={"message_id": message_id},
        )
        if isinstance(data, dict):
            return data.get("url") or data.get("download_url")
        return None
