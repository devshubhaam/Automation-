"""Video bot uploader.

Each video is sent to a Telegram bot (``VIDEO_BOT_USERNAME``) with the existing
userbot session. The bot replies with a link such as
``https://www.domain.com/app/6abfae122a52418b24707585``; that link is parsed out
of the reply (new messages *and* edits of earlier replies are watched).

Mapping video -> link
    * Videos are sent strictly one at a time (a lock).
    * A reply is only accepted if it is newer than the message we sent AND, when
      it is a Telegram reply, it replies to exactly that message. A late reply
      to a previous video can therefore never be attached to the current one.

Memory
    The file is passed to Telethon as a *path*; Telethon streams it from disk in
    small parts. The video is never read into RAM here.

Timeout / retry
    * ``timeout_seconds`` is per video and starts once the bot has the file.
    * Only the *delivery* of the file is retried (``send_attempts``). Once the
      bot has received a video it is never sent again, so a slow bot cannot cause
      duplicate processing.
"""
from __future__ import annotations

import asyncio
import logging
import mimetypes
import re
from pathlib import Path
from typing import Any, Callable

from .common import UploadCancelled, UploadError

logger = logging.getLogger("app.video_bot")

#: Default per-video size limit: 1.5 GB.
DEFAULT_VIDEO_MAX_BYTES = 1536 * 1024 * 1024

#: Default: a link whose path is ``/app/<id>`` or ``/s/<id>`` on any domain, e.g.
#:   https://www.domain.com/app/6abfae122a52418b24707585
#:   https://domain.com/s/dauv7n9bjlnn77sqlrogow6-ryxopea
#: Other links in the bot's reply (channel ads, help links) are ignored.
DEFAULT_URL_PATTERN = r"https?://[^\s/<>\"')\]]+/(?:app|s)/[A-Za-z0-9_-]+"

__all__ = [
    "VideoBotUploader",
    "VideoTooLargeError",
    "video_too_large_message",
    "DEFAULT_VIDEO_MAX_BYTES",
    "extract_url",
    "normalise_username",
]


class VideoTooLargeError(UploadError):
    """A video is larger than the configured per-video limit."""


def video_too_large_message(size: int, limit: int) -> str:
    return f"Video exceeds the size limit ({size} bytes > {limit} bytes)"


def normalise_username(value: str | None) -> str | None:
    """``@FileUploaderBot`` / ``t.me/FileUploaderBot`` -> ``FileUploaderBot``."""
    if not value:
        return None
    value = value.strip()
    value = re.sub(r"^(https?://)?(t\.me/)", "", value, flags=re.I)
    value = value.lstrip("@").strip("/")
    return value or None


def _message_text(message: Any) -> str:
    return str(getattr(message, "raw_text", None) or getattr(message, "message", None) or getattr(message, "text", None) or "")


def _message_urls(message: Any) -> list[str]:
    """Visible text plus hidden link targets (``[text](url)`` entities/buttons)."""
    urls: list[str] = []
    for entity in getattr(message, "entities", None) or []:
        url = getattr(entity, "url", None)
        if url:
            urls.append(str(url))
    for row in getattr(message, "buttons", None) or []:
        for button in (row if isinstance(row, (list, tuple)) else [row]):
            url = getattr(button, "url", None)
            if url:
                urls.append(str(url))
    return urls


def extract_url(message: Any, pattern: "re.Pattern[str]") -> str | None:
    """Return the first URL in ``message`` matching ``pattern``."""
    match = pattern.search(_message_text(message))
    if match:
        return match.group(0).rstrip(".,;:!?")
    for url in _message_urls(message):
        found = pattern.search(url)
        if found:
            return found.group(0).rstrip(".,;:!?")
    return None


class VideoBotUploader:
    """Send a video file to the video bot and return the link it replies with."""

    def __init__(
        self,
        bot_username: str | None,
        client_provider: Callable[[], Any] | None = None,
        *,
        timeout_seconds: float = 1800.0,
        url_pattern: str | None = None,
        poll_seconds: float = 1.0,
        max_size_bytes: int = DEFAULT_VIDEO_MAX_BYTES,
        send_attempts: int = 2,
        retry_delay_seconds: float = 5.0,
    ) -> None:
        self.bot_username = normalise_username(bot_username)
        self.client_provider = client_provider
        self.timeout_seconds = float(timeout_seconds)
        self.poll_seconds = float(poll_seconds)
        self.max_size_bytes = int(max_size_bytes)
        self.send_attempts = max(1, int(send_attempts))
        self.retry_delay_seconds = float(retry_delay_seconds)
        try:
            self._pattern = re.compile(url_pattern or DEFAULT_URL_PATTERN)
        except re.error as exc:
            raise UploadError(f"Invalid VIDEO_URL_PATTERN: {exc}") from exc
        # One video at a time: a reply is always matched to the video just sent.
        self._lock = asyncio.Lock()

    @property
    def configured(self) -> bool:
        return bool(self.bot_username)

    # ------------------------------------------------------------------ #

    async def upload(
        self,
        path: Path,
        *,
        client: Any = None,
        should_cancel: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        """Send ``path`` to the bot; return ``{"provider": "video_bot", "url": ...}``."""
        path = Path(path)
        if not self.bot_username:
            raise UploadError("VIDEO_BOT_USERNAME is not configured")
        if not path.is_file():
            raise UploadError(f"Video file does not exist: {path.name}")
        content_type = mimetypes.guess_type(path.name)[0] or ""
        if not content_type.startswith("video/"):
            raise UploadError(f"Video bot accepts videos only: {path.name}")
        size = path.stat().st_size
        if size > self.max_size_bytes:
            raise VideoTooLargeError(video_too_large_message(size, self.max_size_bytes))

        client = client or (self.client_provider() if self.client_provider else None)
        if client is None:
            raise UploadError("Telegram client is unavailable for the video bot")

        async with self._lock:
            if should_cancel and should_cancel():
                raise UploadCancelled()
            return await self._send_and_wait(client, path, should_cancel)

    async def _send_and_wait(
        self,
        client: Any,
        path: Path,
        should_cancel: Callable[[], bool] | None,
    ) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        state: dict[str, Any] = {"sent_id": None, "early": []}

        def consider(message: Any) -> None:
            if future.done():
                return
            sent_id = state["sent_id"]
            if sent_id is None:
                # Reply arrived before send_file() returned: keep it.
                state["early"].append(message)
                return
            reply_to = getattr(message, "reply_to_msg_id", None)
            if isinstance(reply_to, int) and reply_to != sent_id:
                return  # explicitly a reply to some other (earlier) video
            msg_id = getattr(message, "id", None)
            if isinstance(msg_id, int) and msg_id <= sent_id:
                return  # an older message, not a reply to this video
            url = extract_url(message, self._pattern)
            if url:
                future.set_result(url)

        async def on_event(event: Any) -> None:
            consider(getattr(event, "message", event))

        builders = self._event_builders()
        for builder in builders:
            client.add_event_handler(on_event, builder)

        try:
            sent = await self._send_with_retry(client, path, should_cancel, state)

            state["sent_id"] = getattr(sent, "id", 0) or 0
            for message in state["early"]:
                consider(message)

            # The bot has the file now: never re-send it, only wait.
            deadline = loop.time() + self.timeout_seconds
            while not future.done():
                if should_cancel and should_cancel():
                    raise UploadCancelled()
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise UploadError(
                        f"Video bot did not reply with a link within {int(self.timeout_seconds)}s"
                    )
                await asyncio.wait({future}, timeout=min(self.poll_seconds, remaining))

            return {"provider": "video_bot", "url": future.result()}

        finally:
            for builder in builders:
                try:
                    client.remove_event_handler(on_event, builder)
                except Exception:
                    logger.debug("Could not remove video bot handler", exc_info=True)

    async def _send_with_retry(
        self,
        client: Any,
        path: Path,
        should_cancel: Callable[[], bool] | None,
        state: dict[str, Any],
    ) -> Any:
        """Deliver the file to the bot (cancellable, retried only if delivery failed)."""
        last_error = "unknown error"
        for attempt in range(1, self.send_attempts + 1):
            state["early"].clear()
            logger.info(
                "Sending video to @%s: %s (attempt %s/%s)",
                self.bot_username, path.name, attempt, self.send_attempts,
            )
            # send_file() gets the PATH (streamed from disk), never the bytes.
            task = asyncio.ensure_future(
                client.send_file(self.bot_username, str(path), supports_streaming=True)
            )
            try:
                while not task.done():
                    if should_cancel and should_cancel():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                        raise UploadCancelled()
                    await asyncio.wait({task}, timeout=self.poll_seconds)
                return task.result()
            except UploadCancelled:
                raise
            except asyncio.CancelledError:
                task.cancel()
                raise
            except Exception as exc:
                last_error = type(exc).__name__
                logger.warning(
                    "Could not deliver %s to the video bot (attempt %s/%s): %s",
                    path.name, attempt, self.send_attempts, last_error,
                )

            if attempt < self.send_attempts:
                waited = 0.0
                while waited < self.retry_delay_seconds:
                    if should_cancel and should_cancel():
                        raise UploadCancelled()
                    step = min(self.poll_seconds, self.retry_delay_seconds - waited)
                    await asyncio.sleep(step)
                    waited += step

        raise UploadError(
            f"Could not send video to the bot after {self.send_attempts} attempt(s): {last_error}"
        )

    def _event_builders(self) -> list[Any]:
        from telethon import events  # local import: keeps tests/offline use light

        return [
            events.NewMessage(from_users=self.bot_username, incoming=True),
            events.MessageEdited(from_users=self.bot_username, incoming=True),
        ]
