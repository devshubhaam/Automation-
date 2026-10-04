"""Video bot uploaders (Part 3).

Every video is sent to a *Telegram uploader bot* with the existing userbot
session. The bot replies to the video with a link; that link is parsed out of
the reply (new messages *and* edits of earlier replies are watched).

Two layers
==========

``VideoBotUploader``
    Talks to exactly ONE bot. Sends the file, waits for the link.

``MultiVideoBotUploader``
    Owns an ordered list of ``VideoBotUploader`` objects (``VIDEO_BOTS``) and
    applies the deterministic fallback policy below. This is what the pipeline
    uses. Bot usernames are NEVER hard-coded; they come from ``VIDEO_BOTS``.

Sequential workflow (one video at a time)
=========================================
For every video: send it -> wait for THAT video's reply (``VIDEO_LINK_TIMEOUT``,
default 900 s, counted from the moment the file was delivered) -> the moment
the link arrives the caller moves on to the next video. Only after the full
timeout is a video failed ("Link not received"). Progress is logged at every
step: sending, waiting for reply, URL received, timeout.

Mapping video -> link (the critical part)
=========================================
* Videos are sent strictly one at a time (locks), never to two bots at once.
* A bot message is accepted for the current video only if
    - it comes from the configured bot (Telethon ``from_users`` filter), and
    - it is a Telegram *reply to the message we sent* (``reply_to_msg_id ==
      sent message id``), or it is an EDIT of a bot message that already
      replied to our message (``adopted`` ids), and
    - it is newer than the message we sent, and
    - it contains a URL of a known provider (see ``PROVIDER_URL_PATTERNS``).
  A late reply to an earlier (timed-out) video therefore can never be attached
  to the current video, and unrelated chatter in the bot chat is ignored.
  ``require_reply=False`` (``VIDEO_BOT_REQUIRE_REPLY=false``) additionally
  accepts newer NON-reply messages; only use that for bots that never reply.

Modes (``VIDEO_BOT_MODE``)
==========================
``all`` (default)
    EVERY video is uploaded to EVERY configured bot, one bot after the other
    (never in parallel). Each bot gives its own link (DiskWala link AND Flezen
    link). A bot that fails (delivery, rejection, timeout) does NOT stop the
    other bots: its failure is reported and the links of the other bots are
    kept. A video only fails completely when every bot failed. Bots are
    different providers, so a timeout at one bot never creates a duplicate
    upload at another.

``fallback``
    The first bot that returns a link wins; the next bot is only used
    according to the policy below.

Fallback policy (``fallback`` mode, no duplicate uploads)
=========================================================
For each video the bots are tried in the configured order:

1. Delivery failure (Telegram could not send the file; all
   ``send_attempts`` failed) -> the bot never had the video -> next bot.
2. The bot replied (to our video) that it cannot process it (explicit error
   text, no link) -> the bot refused the file -> next bot.
3. Provider timeout (file delivered, no link within the timeout) -> the video
   may still be processing at that provider, so by DEFAULT it is NOT sent to
   another bot (that would create a duplicate upload). The video is reported
   as failed with reason "timeout". Opt in with
   ``VIDEO_BOT_FALLBACK_ON_TIMEOUT=true`` to try the next bot after the FULL
   timeout has elapsed (a duplicate upload is then possible, but never an
   immediate one).
4. Cancellation / size-limit / "client unavailable" errors are not bot
   specific and are raised immediately.

Memory
======
The file is passed to Telethon as a *path*; Telethon streams it from disk in
small parts. The video is never read into RAM here.

Event handlers
==============
Temporary handlers are registered per attempt and ALWAYS removed in a
``finally`` block (success, timeout, failure, cancellation, shutdown), so they
never accumulate between videos or retries.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import mimetypes
import re
from pathlib import Path
from typing import Any, Callable, Sequence
from urllib.parse import urlsplit, urlunsplit

from .common import UploadCancelled, UploadError

logger = logging.getLogger("app.video_bot")

#: Default wait for ONE video's link after the file was delivered: 15 minutes
#: (``VIDEO_LINK_TIMEOUT``).
DEFAULT_LINK_TIMEOUT_SECONDS = 900.0

#: Default per-video size limit: 1.5 GB.
DEFAULT_VIDEO_MAX_BYTES = 1536 * 1024 * 1024

#: Legacy single-bot default: a link whose path is ``/app/<id>`` or ``/s/<id>``
#: on ANY domain. Kept for ``VideoBotUploader`` used on its own.
DEFAULT_URL_PATTERN = r"https?://[^\s/<>\"')\]]+/(?:app|s)/[A-Za-z0-9_-]+"

#: Provider-aware validation used by ``MultiVideoBotUploader``. Only these
#: provider links are accepted (plus an optional ``VIDEO_URL_PATTERN``), e.g.
#:   https://www.diskwala.com/app/6abfae122a52418b24707585
#:   https://flezen.com/s/dauv7n9bjlnn77sqlrogow6-ryxopea
#: Anything else (google.com, telegram.org, channel ads ...) is ignored.
PROVIDER_URL_PATTERNS: dict[str, str] = {
    "diskwala": r"https?://(?:www\.)?diskwala\.com/app/[A-Za-z0-9_-]+",
    "flezen": r"https?://(?:www\.)?flezen\.com/s/[A-Za-z0-9_-]+",
}

#: Human readable provider names for progress messages.
PROVIDER_LABELS: dict[str, str] = {
    "diskwala": "DiskWala",
    "flezen": "Flezen",
    "custom": "Custom",
}

#: Words that mean "this bot refuses / failed to process the video".
_REJECTION_RE = re.compile(
    r"\b(failed|failure|error|unsupported|not supported|too large|too big|"
    r"exceeds?|invalid|unable|cannot|can't|couldn't|rejected)\b",
    re.IGNORECASE,
)

__all__ = [
    "VideoBotUploader",
    "MultiVideoBotUploader",
    "VideoTooLargeError",
    "VideoDeliveryError",
    "VideoTimeoutError",
    "VideoBotRejectedError",
    "VideoUploadFailed",
    "video_too_large_message",
    "DEFAULT_VIDEO_MAX_BYTES",
    "DEFAULT_LINK_TIMEOUT_SECONDS",
    "DEFAULT_URL_PATTERN",
    "PROVIDER_URL_PATTERNS",
    "PROVIDER_LABELS",
    "extract_url",
    "normalise_url",
    "normalise_username",
    "provider_for_url",
    "provider_label",
    "build_provider_pattern",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class VideoTooLargeError(UploadError):
    """A video is larger than the configured per-video limit."""


class VideoDeliveryError(UploadError):
    """The video could not be delivered to the bot (the bot never got it)."""


class VideoTimeoutError(UploadError):
    """The bot got the video but sent no link within the timeout."""


class VideoBotRejectedError(UploadError):
    """The bot answered that it cannot process the video."""


class VideoUploadFailed(UploadError):
    """Every configured bot failed for one video.

    ``attempts`` lists ``{"bot", "kind", "reason"}`` for each bot tried.
    ``bot`` is the last bot that was tried.
    """

    def __init__(self, message: str, attempts: list[dict[str, str]]) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.bot: str | None = attempts[-1]["bot"] if attempts else None


def video_too_large_message(size: int, limit: int) -> str:
    return f"Video exceeds the size limit ({size} bytes > {limit} bytes)"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def normalise_username(value: str | None) -> str | None:
    """``@FileUploaderBot`` / ``t.me/FileUploaderBot`` -> ``FileUploaderBot``."""
    if not value:
        return None
    value = value.strip()
    value = re.sub(r"^(https?://)?(t\.me/)", "", value, flags=re.I)
    value = value.lstrip("@").strip("/")
    return value or None


def build_provider_pattern(extra_pattern: str | None = None) -> str:
    """One regex that accepts every known provider link (and ``extra_pattern``)."""
    parts = [f"(?:{p})" for p in PROVIDER_URL_PATTERNS.values()]
    if extra_pattern:
        parts.append(f"(?:{extra_pattern})")
    return "|".join(parts)


def normalise_url(url: str) -> str:
    """Tidy an extracted link before it is stored.

    Trailing punctuation, fragments and trailing slashes are removed and the
    scheme/host are lower-cased. The path (the file id) is left untouched.
    """
    url = url.strip().rstrip(".,;:!?)]}>\"'")
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    if not parts.scheme or not parts.netloc:
        return url
    path = parts.path.rstrip("/")
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, parts.query, ""))


def provider_for_url(url: str | None, extra_pattern: str | None = None) -> str | None:
    """``https://www.diskwala.com/app/x`` -> ``"diskwala"`` (``None`` if unknown)."""
    if not url:
        return None
    for name, pattern in PROVIDER_URL_PATTERNS.items():
        if re.match(pattern, url):
            return name
    if extra_pattern:
        try:
            if re.match(extra_pattern, url):
                return "custom"
        except re.error:
            return None
    return None


def provider_label(provider: str | None) -> str:
    if not provider:
        return "unknown"
    return PROVIDER_LABELS.get(provider, provider.title())


def _message_text(message: Any) -> str:
    return str(getattr(message, "raw_text", None) or getattr(message, "message", None) or getattr(message, "text", None) or "")


def _message_urls(message: Any) -> list[str]:
    """Hidden link targets (``[text](url)`` entities and inline URL buttons)."""
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
    """Return the first URL in ``message`` matching ``pattern``, normalised.

    Looks at the visible text first, then at hidden link entities and inline
    URL buttons.
    """
    match = pattern.search(_message_text(message))
    if match:
        return normalise_url(match.group(0))
    for url in _message_urls(message):
        found = pattern.search(url)
        if found:
            return normalise_url(found.group(0))
    return None


# --------------------------------------------------------------------------- #
# One bot
# --------------------------------------------------------------------------- #


class VideoBotUploader:
    """Send a video file to ONE video bot and return the link it replies with."""

    def __init__(
        self,
        bot_username: str | None,
        client_provider: Callable[[], Any] | None = None,
        *,
        timeout_seconds: float = DEFAULT_LINK_TIMEOUT_SECONDS,
        url_pattern: str | None = None,
        poll_seconds: float = 1.0,
        max_size_bytes: int = DEFAULT_VIDEO_MAX_BYTES,
        send_attempts: int = 2,
        retry_delay_seconds: float = 5.0,
        require_reply: bool = False,
        detect_rejection: bool = False,
    ) -> None:
        self.bot_username = normalise_username(bot_username)
        self.client_provider = client_provider
        self.timeout_seconds = float(timeout_seconds)
        self.poll_seconds = float(poll_seconds)
        self.max_size_bytes = int(max_size_bytes)
        self.send_attempts = max(1, int(send_attempts))
        self.retry_delay_seconds = float(retry_delay_seconds)
        self.require_reply = bool(require_reply)
        self.detect_rejection = bool(detect_rejection)
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
        on_sent: Callable[[], Any] | None = None,
    ) -> dict[str, Any]:
        """Send ``path`` to the bot; return ``{"provider": "video_bot", "url": ...}``.

        ``on_sent()`` (sync or async) is called once the file was delivered and
        the wait for the bot's link starts.
        """
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
            return await self._send_and_wait(client, path, should_cancel, on_sent)

    async def _send_and_wait(
        self,
        client: Any,
        path: Path,
        should_cancel: Callable[[], bool] | None,
        on_sent: Callable[[], Any] | None = None,
    ) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        state: dict[str, Any] = {"sent_id": None, "early": [], "adopted": set()}

        def consider(message: Any) -> None:
            if future.done():
                return
            sent_id = state["sent_id"]
            if sent_id is None:
                # Reply arrived before send_file() returned: keep it.
                state["early"].append(message)
                return

            adopted: set[int] = state["adopted"]
            reply_to = getattr(message, "reply_to_msg_id", None)
            msg_id = getattr(message, "id", None)
            known = isinstance(msg_id, int) and msg_id in adopted
            # A reply to our video, or to a bot message that already replied to
            # our video (some bots answer their own "processing" message).
            replies_to_current = isinstance(reply_to, int) and (
                reply_to == sent_id or reply_to in adopted
            )

            if isinstance(reply_to, int) and not replies_to_current and not known:
                return  # explicitly a reply to some other (earlier) video
            if self.require_reply and not replies_to_current and not known:
                return  # strict mode: only replies to THIS video count
            if isinstance(msg_id, int) and msg_id <= sent_id and not known:
                return  # an older message, not a reply to this video

            if replies_to_current and isinstance(msg_id, int):
                # Remember it: the bot may EDIT this message later.
                adopted.add(msg_id)

            url = extract_url(message, self._pattern)
            if url:
                logger.info(
                    "URL received for %s from @%s (reply to message %s): %s",
                    path.name, self.bot_username, reply_to if replies_to_current else msg_id, url,
                )
                future.set_result(url)
                return

            if self.detect_rejection and (replies_to_current or known):
                if _REJECTION_RE.search(_message_text(message)):
                    future.set_exception(
                        VideoBotRejectedError(
                            f"@{self.bot_username} could not process the video"
                        )
                    )

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
            logger.info(
                "Waiting for @%s's reply to %s (message %s, up to %ss)",
                self.bot_username, path.name, state["sent_id"], int(self.timeout_seconds),
            )
            if on_sent is not None:
                try:
                    outcome = on_sent()
                    if inspect.isawaitable(outcome):
                        await outcome
                except Exception:
                    logger.debug("on_sent callback failed", exc_info=True)

            deadline = loop.time() + self.timeout_seconds
            while not future.done():
                if should_cancel and should_cancel():
                    raise UploadCancelled()
                remaining = deadline - loop.time()
                if remaining <= 0:
                    logger.warning(
                        "Timeout: no link from @%s for %s within %ss",
                        self.bot_username, path.name, int(self.timeout_seconds),
                    )
                    raise VideoTimeoutError(
                        f"Video bot did not reply with a link within {int(self.timeout_seconds)}s"
                    )
                await asyncio.wait({future}, timeout=min(self.poll_seconds, remaining))

            return {"provider": "video_bot", "url": future.result()}

        finally:
            # ALWAYS unregister: success, timeout, failure, cancel, shutdown.
            for builder in builders:
                try:
                    client.remove_event_handler(on_event, builder)
                except Exception:
                    logger.debug("Could not remove video bot handler", exc_info=True)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()  # mark retrieved: no "never retrieved" warning

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

        raise VideoDeliveryError(
            f"Could not send video to the bot after {self.send_attempts} attempt(s): {last_error}"
        )

    def _event_builders(self) -> list[Any]:
        from telethon import events  # local import: keeps tests/offline use light

        return [
            events.NewMessage(from_users=self.bot_username, incoming=True),
            events.MessageEdited(from_users=self.bot_username, incoming=True),
        ]


# --------------------------------------------------------------------------- #
# Several bots
# --------------------------------------------------------------------------- #


class MultiVideoBotUploader:
    """Try the configured video bots in order (see the module docstring).

    ``bots`` comes from ``VIDEO_BOTS`` (already normalised). Nothing here knows
    a concrete bot username.
    """

    #: The pipeline passes ``on_attempt`` only to uploaders with this flag.
    supports_progress_callback = True
    #: The pipeline passes ``on_link`` (called for every received link).
    supports_link_callback = True
    #: The pipeline passes ``on_sent`` (called when a bot received the file).
    supports_sent_callback = True

    def __init__(
        self,
        bots: Sequence[str] | None,
        client_provider: Callable[[], Any] | None = None,
        *,
        timeout_seconds: float = DEFAULT_LINK_TIMEOUT_SECONDS,
        extra_url_pattern: str | None = None,
        poll_seconds: float = 1.0,
        max_size_bytes: int = DEFAULT_VIDEO_MAX_BYTES,
        send_attempts: int = 2,
        retry_delay_seconds: float = 5.0,
        fallback_on_timeout: bool = False,
        require_reply: bool = True,
        mode: str = "all",
    ) -> None:
        mode = str(mode or "all").strip().lower()
        if mode not in ("all", "fallback"):
            raise UploadError(f"Invalid video bot mode: {mode!r} (use 'all' or 'fallback')")
        self.mode = mode
        names: list[str] = []
        for bot in bots or ():
            name = normalise_username(bot)
            if name and name.lower() not in {n.lower() for n in names}:
                names.append(name)

        self.client_provider = client_provider
        self.max_size_bytes = int(max_size_bytes)
        self.fallback_on_timeout = bool(fallback_on_timeout)
        self.extra_url_pattern = extra_url_pattern

        try:
            pattern = build_provider_pattern(extra_url_pattern)
            re.compile(pattern)
        except re.error as exc:
            raise UploadError(f"Invalid VIDEO_URL_PATTERN: {exc}") from exc

        self.uploaders: list[VideoBotUploader] = [
            VideoBotUploader(
                name,
                client_provider,
                timeout_seconds=timeout_seconds,
                url_pattern=pattern,
                poll_seconds=poll_seconds,
                max_size_bytes=max_size_bytes,
                send_attempts=send_attempts,
                retry_delay_seconds=retry_delay_seconds,
                require_reply=require_reply,
                detect_rejection=True,
            )
            for name in names
        ]
        # Learned from successful replies: bot -> provider (for failure reports).
        self._bot_providers: dict[str, str] = {}
        # Global lock: never two videos in flight, whatever the bot.
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ #

    @property
    def bots(self) -> list[str]:
        return [u.bot_username for u in self.uploaders if u.bot_username]

    @property
    def configured(self) -> bool:
        return bool(self.uploaders)

    def provider_for_bot(self, bot: str | None) -> str | None:
        """Provider last seen for ``bot`` (``None`` until it produced a link)."""
        return self._bot_providers.get(bot or "")

    # ------------------------------------------------------------------ #

    async def upload(
        self,
        path: Path,
        *,
        client: Any = None,
        should_cancel: Callable[[], bool] | None = None,
        on_attempt: Callable[[str, int, int], Any] | None = None,
        on_link: Callable[[dict[str, str]], Any] | None = None,
        on_sent: Callable[[str], Any] | None = None,
    ) -> dict[str, Any]:
        """Send ``path`` to the bots and return the link(s).

        Result: ``{"provider": "video_bot", "provider_name": "diskwala",
        "bot": "FirstUploaderBot", "url": ..., "attempts": [...],
        "links": [{"bot", "provider_name", "url"}, ...],
        "failures": [{"bot", "kind", "reason"}, ...]}``.
        ``url`` / ``bot`` / ``provider_name`` describe the FIRST link. In
        ``all`` mode ``links`` has one entry per successful bot.
        ``on_attempt(bot, index, total)`` is called (sync or async) right
        before a bot is tried, ``on_link(link)`` right after a bot returned
        its link (so it is kept even if a later bot or a cancel interrupts).
        ``on_sent(bot)`` is called once that bot has received the file (the wait
        for its link starts).
        """
        path = Path(path)
        if not self.uploaders:
            raise UploadError("VIDEO_BOTS is not configured (e.g. VIDEO_BOTS=@YourUploaderBot)")
        if not path.is_file():
            raise UploadError(f"Video file does not exist: {path.name}")
        content_type = mimetypes.guess_type(path.name)[0] or ""
        if not content_type.startswith("video/"):
            raise UploadError(f"Video bot accepts videos only: {path.name}")
        size = path.stat().st_size
        if size > self.max_size_bytes:
            # Never sent to ANY bot.
            raise VideoTooLargeError(video_too_large_message(size, self.max_size_bytes))

        client = client or (self.client_provider() if self.client_provider else None)
        if client is None:
            raise UploadError("Telegram client is unavailable for the video bot")

        async with self._lock:
            if self.mode == "all":
                return await self._try_all(path, client, should_cancel, on_attempt, on_link, on_sent)
            return await self._try_bots(path, client, should_cancel, on_attempt, on_link, on_sent)

    @staticmethod
    async def _call(callback: Callable[..., Any] | None, *args: Any) -> None:
        """Run a sync/async progress callback; its errors never matter."""
        if callback is None:
            return
        try:
            outcome = callback(*args)
            if inspect.isawaitable(outcome):
                await outcome
        except Exception:
            logger.debug("progress callback failed", exc_info=True)

    @staticmethod
    def _sent_hook(
        on_sent: Callable[[str], Any] | None, bot: str
    ) -> Callable[[], Any] | None:
        """Bind ``bot`` into the caller's ``on_sent(bot)`` callback."""
        if on_sent is None:
            return None
        return lambda: on_sent(bot)

    def _make_link(self, bot: str, url: str) -> dict[str, str]:
        provider = provider_for_url(url, self.extra_url_pattern)
        if provider:
            self._bot_providers[bot] = provider
        return {"bot": bot, "provider_name": provider or "unknown", "url": url}

    async def _try_all(
        self,
        path: Path,
        client: Any,
        should_cancel: Callable[[], bool] | None,
        on_attempt: Callable[[str, int, int], Any] | None,
        on_link: Callable[[dict[str, str]], Any] | None,
        on_sent: Callable[[str], Any] | None = None,
    ) -> dict[str, Any]:
        """Upload ``path`` to EVERY bot, sequentially; keep every link."""
        attempts: list[dict[str, str]] = []
        links: list[dict[str, str]] = []
        failures: list[dict[str, str]] = []
        last_exc: UploadError | None = None
        total = len(self.uploaders)

        for index, uploader in enumerate(self.uploaders, start=1):
            bot = uploader.bot_username or ""

            if should_cancel and should_cancel():
                raise UploadCancelled()

            await self._call(on_attempt, bot, index, total)

            try:
                result = await uploader.upload(
                    path,
                    client=client,
                    should_cancel=should_cancel,
                    on_sent=self._sent_hook(on_sent, bot),
                )
            except (UploadCancelled, VideoTooLargeError):
                raise
            except VideoDeliveryError as exc:
                kind, last_exc = "delivery", exc
            except VideoBotRejectedError as exc:
                kind, last_exc = "rejected", exc
            except VideoTimeoutError as exc:
                kind, last_exc = "timeout", exc
            except UploadError:
                raise  # not bot specific (client unavailable, ...)
            else:
                link = self._make_link(bot, str(result.get("url", "")))
                links.append(link)
                attempts.append({"bot": bot, "kind": "ok", "reason": ""})
                await self._call(on_link, dict(link))
                continue

            attempts.append({"bot": bot, "kind": kind, "reason": str(last_exc)})
            failures.append({"bot": bot, "kind": kind, "reason": str(last_exc)})
            logger.warning(
                "Video %s: bot @%s failed (%s); continuing with the remaining bots",
                path.name, bot, kind,
            )

        if not links:
            assert last_exc is not None
            if len(attempts) == 1:
                last_exc.bot = attempts[0]["bot"]  # type: ignore[attr-defined]
                last_exc.attempts = attempts  # type: ignore[attr-defined]
                raise last_exc
            message = "; ".join(f"@{a['bot']}: {a['reason']}" for a in attempts)
            raise VideoUploadFailed(message, attempts) from last_exc

        first = links[0]
        return {
            "provider": "video_bot",
            "provider_name": first["provider_name"],
            "bot": first["bot"],
            "url": first["url"],
            "attempts": attempts,
            "links": links,
            "failures": failures,
        }

    async def _try_bots(
        self,
        path: Path,
        client: Any,
        should_cancel: Callable[[], bool] | None,
        on_attempt: Callable[[str, int, int], Any] | None,
        on_link: Callable[[dict[str, str]], Any] | None = None,
        on_sent: Callable[[str], Any] | None = None,
    ) -> dict[str, Any]:
        attempts: list[dict[str, str]] = []
        last_exc: UploadError | None = None
        total = len(self.uploaders)

        for index, uploader in enumerate(self.uploaders, start=1):
            bot = uploader.bot_username or ""

            if should_cancel and should_cancel():
                raise UploadCancelled()

            if on_attempt is not None:
                try:
                    outcome = on_attempt(bot, index, total)
                    if inspect.isawaitable(outcome):
                        await outcome
                except Exception:
                    logger.debug("on_attempt callback failed", exc_info=True)

            try:
                result = await uploader.upload(
                    path,
                    client=client,
                    should_cancel=should_cancel,
                    on_sent=self._sent_hook(on_sent, bot),
                )
            except (UploadCancelled, VideoTooLargeError):
                raise
            except VideoDeliveryError as exc:
                kind, last_exc = "delivery", exc
                fallback = True  # the bot never had the file
            except VideoBotRejectedError as exc:
                kind, last_exc = "rejected", exc
                fallback = True  # the bot refused the file
            except VideoTimeoutError as exc:
                kind, last_exc = "timeout", exc
                # The bot may still be processing: only fall back if opted in.
                fallback = self.fallback_on_timeout
            except UploadError:
                raise  # not bot specific (client unavailable, ...)

            else:
                link = self._make_link(bot, str(result.get("url", "")))
                attempts.append({"bot": bot, "kind": "ok", "reason": ""})
                await self._call(on_link, dict(link))
                return {
                    "provider": "video_bot",
                    "provider_name": link["provider_name"],
                    "bot": bot,
                    "url": link["url"],
                    "attempts": attempts,
                    "links": [link],
                    "failures": [
                        {"bot": a["bot"], "kind": a["kind"], "reason": a["reason"]}
                        for a in attempts
                        if a["kind"] != "ok"
                    ],
                }

            attempts.append({"bot": bot, "kind": kind, "reason": str(last_exc)})
            logger.warning(
                "Video %s: bot @%s failed (%s)%s",
                path.name, bot, kind,
                "; trying next bot" if fallback and index < total else "",
            )
            if not fallback:
                break

        assert last_exc is not None
        if len(attempts) == 1:
            # Single bot tried: keep the precise original error type/message.
            last_exc.bot = attempts[0]["bot"]  # type: ignore[attr-defined]
            last_exc.attempts = attempts  # type: ignore[attr-defined]
            raise last_exc
        message = "; ".join(f"@{a['bot']}: {a['reason']}" for a in attempts)
        raise VideoUploadFailed(message, attempts) from last_exc
