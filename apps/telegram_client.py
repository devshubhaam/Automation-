"""Telethon userbot client: authentication, handlers, download and progress I/O.

Owner restriction
-----------------
This is a *userbot* running on the owner's own account. It deliberately only
reacts to messages that are **outgoing** (sent by the account itself) or that
live in the account's Saved Messages. Files uploaded by other people are never
processed. The owner id is taken from ``get_me()`` - it is never hardcoded.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

from telethon import TelegramClient, events, utils as telethon_utils

from .config import Settings
from .job_manager import JobManager, JobStatus
from .progress import ProgressReporter, render_progress
from .utils import format_bytes, truncate

__all__ = ["TelegramUserbot"]

logger = logging.getLogger("app.telegram_client")

ZIP_MIME_TYPES = {"application/zip", "application/x-zip-compressed", "multipart/x-zip"}
SUPPORTED_MESSAGE = (
    "❌ Unsupported input.\n\n"
    "Please send a ZIP archive containing your images/videos."
)
ACTIVE_STATUSES = (
    JobStatus.DOWNLOADING,
    JobStatus.EXTRACTING,
    JobStatus.SCANNING,
    JobStatus.QUEUED,
)


def is_zip_document(document) -> bool:
    """True when a Telegram document looks like a ZIP archive."""
    if document is None:
        return False
    filename = (getattr(document, "file_name", None) or "").lower()
    mime = (getattr(document, "mime_type", None) or "").lower()
    return filename.endswith(".zip") or mime in ZIP_MIME_TYPES


def document_filename(document, fallback: str = "archive.zip") -> str:
    """Best-effort filename for a Telegram document."""
    name = getattr(document, "file_name", None)
    if name:
        return Path(name).name
    return fallback


class TelegramUserbot:
    """Thin wrapper around :class:`telethon.TelegramClient`."""

    def __init__(self, settings: Settings, job_manager: JobManager, *, pipeline=None) -> None:
        self.settings = settings
        self.job_manager = job_manager
        self.pipeline = pipeline  # injected worker (set in main.py)
        self.owner_id: Optional[int] = None
        self.owner_username: Optional[str] = None
        self._client: Optional[TelegramClient] = None

    # ------------------------------------------------------------------ #
    # Construction / authentication
    # ------------------------------------------------------------------ #
    @property
    def client(self) -> TelegramClient:
        if self._client is None:
            self.settings.session_dir.mkdir(parents=True, exist_ok=True)
            self._client = TelegramClient(
                str(self.settings.session_path),
                self.settings.api_id,
                self.settings.api_hash,
                device_model="Telegram Media Processor",
                system_version="PART-1",
                app_version="0.1.0",
            )
        return self._client

    async def start(self) -> None:
        """Connect and authenticate, creating/reusing the Telethon session."""
        logger.info("Telegram client starting")
        await self.client.start(phone=self.settings.phone)

        if not await self.client.is_user_authorized():
            raise RuntimeError("Telegram authentication failed")

        me = await self.client.get_me()
        self.owner_id = int(me.id)
        self.owner_username = getattr(me, "username", None)
        logger.info("Telegram authentication successful (owner_id=%s)", self.owner_id)

        if self.settings.owner_id is not None and int(self.settings.owner_id) != self.owner_id:
            logger.warning(
                "OWNER_ID in .env (%s) differs from the authenticated account (%s); "
                "using the authenticated account as owner.",
                self.settings.owner_id,
                self.owner_id,
            )
        self.register_handlers()

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.disconnect()
            logger.info("Telegram client disconnected")

    # ------------------------------------------------------------------ #
    # Owner / message checks
    # ------------------------------------------------------------------ #
    def is_owner_message(self, event) -> bool:
        """Only accept messages sent by the account itself (outgoing)."""
        if event.out:
            return True
        try:
            sender = event.sender_id
        except Exception:  # pragma: no cover - defensive
            return False
        if sender is None or self.owner_id is None:
            return False
        return int(sender) == self.owner_id

    # ------------------------------------------------------------------ #
    # Handlers
    # ------------------------------------------------------------------ #
    def register_handlers(self) -> None:
        client = self.client

        @client.on(events.NewMessage(outgoing=True))
        async def _outgoing_handler(event):  # noqa: ANN001
            await self._handle_message(event)

        # Incoming private messages are registered only so that a foreign sender
        # is explicitly recognised and refused by the owner check above.
        @client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
        async def _incoming_handler(event):  # noqa: ANN001
            await self._handle_message(event)

        logger.debug("Event handlers registered")

    async def _handle_message(self, event) -> None:  # noqa: ANN001
        # Owner restriction: this userbot only ever acts on the owner's own
        # messages. Anything from another account is ignored silently.
        if not self.is_owner_message(event):
            logger.info(
                "Ignoring message from non-owner sender %s", getattr(event, "sender_id", None)
            )
            return

        text = (event.raw_text or "").strip()
        command = text.split()[0].lower() if text else ""

        if command in {"/ping", "/start"}:
            await event.reply("🏓 Pong\n\nUserbot is running.")
            return
        if command == "/status":
            await event.reply(self.render_status())
            return
        if command == "/cancel":
            await self.handle_cancel(event)
            return

        document = getattr(event.message, "document", None)
        if document is None:
            if text and text.startswith("/"):
                await event.reply(f"❓ Unknown command: {truncate(text, 40)}")
            return

        if not is_zip_document(document):
            logger.info("Unsupported document ignored: %s", document_filename(document, "unnamed"))
            await event.reply(SUPPORTED_MESSAGE)
            return

        await self.handle_zip(event, document)

    async def handle_zip(self, event, document) -> None:  # noqa: ANN001
        """Validate, create a job and hand it to the worker queue."""
        filename = document_filename(document)
        size = int(getattr(document, "size", 0) or 0)
        logger.info("ZIP received: %s (%s)", filename, format_bytes(size))

        if size and size > self.settings.max_archive_size_bytes:
            await event.reply(
                "❌ ZIP file is too large.\n\n"
                f"Maximum allowed size: {self.settings.max_archive_size_mb} MB\n"
                f"Received: {format_bytes(size)}"
            )
            logger.warning("Rejected oversized ZIP: %s", filename)
            return

        job = self.job_manager.create_job(
            archive_name=filename,
            source_message_id=getattr(event.message, "id", None),
            chat_id=getattr(event, "chat_id", None),
            sender_id=getattr(event, "sender_id", None),
            archive_size_bytes=size,
        )

        reporter = ProgressReporter(TelegramMessageEditor(event, self.client))
        await reporter.start(job, detail="Waiting for the worker...")
        self.job_manager.set_status_message(job.job_id, reporter.message_id)
        job.status_message_id = reporter.message_id

        self.job_manager.enqueue(job.job_id)
        if self.pipeline is not None:
            await self.pipeline.submit(job)

    async def handle_cancel(self, event) -> None:  # noqa: ANN001
        job = self.job_manager.active_job()
        if job is None:
            await event.reply("ℹ️ No active job.")
            return
        self.job_manager.request_cancel(job.job_id)
        await event.reply(f"🛑 Cancellation requested.\n\nJob: {job.job_id}")

    # ------------------------------------------------------------------ #
    # Status rendering
    # ------------------------------------------------------------------ #
    def render_status(self) -> str:
        job = self.job_manager.active_job() or self.job_manager.latest_job()
        if job is None:
            return "ℹ️ No active job."

        lines = [
            "📊 Job Status",
            "",
            f"ID: {job.job_id}",
            f"Status: {job.status.value}",
        ]
        if job.status in ACTIVE_STATUSES:
            lines.extend(
                [
                    "",
                    f"Images: {job.image_files}",
                    f"Videos: {job.video_files}",
                    f"Ignored: {job.ignored_files}",
                ]
            )
        elif job.status == JobStatus.COMPLETED:
            lines.extend(
                [
                    "",
                    f"Images: {job.image_files}",
                    f"Videos: {job.video_files}",
                    f"Ignored: {job.ignored_files}",
                ]
            )
        elif job.status == JobStatus.FAILED and job.error:
            lines.extend(["", f"Reason: {truncate(job.error, 300)}"])

        queued = self.job_manager.queue_size()
        if queued:
            lines.extend(["", f"Queued jobs: {queued}"])
        return "\n".join(lines)

    # ------------------------------------------------------------------ #
    # Message I/O adapter used by ProgressReporter
    # ------------------------------------------------------------------ #
    async def send_message(self, text: str) -> int:
        message = await self.client.send_message("me", text)
        return message.id

    async def edit_message(self, message_id: int, text: str) -> None:
        await self.client.edit_message("me", message_id, text)

    async def send_text(self, text: str) -> int:
        return await self.send_message(text)

    async def run_until_disconnected(self) -> None:
        await self.client.run_until_disconnected()


class TelegramMessageEditor:
    """Adapter turning the userbot into the ``MessageEditor`` protocol.

    The progress message lives in Saved Messages ("me") so it is always the
    owner's own chat and survives chat scrolling.
    """

    def __init__(self, event, client: TelegramClient) -> None:
        self._event = event
        self._client = client

    async def send(self, text: str) -> int:
        try:
            message = await self._event.reply(text)
            return message.id
        except Exception:  # pragma: no cover - fallback to Saved Messages
            message = await self._client.send_message("me", text)
            return message.id

    async def edit(self, message_id: int, text: str) -> None:
        try:
            await self._client.edit_message(
                getattr(self._event, "chat_id", None) or "me", message_id, text
            )
        except Exception:
            await self._client.edit_message("me", message_id, text)


def build_session_description(settings: Settings) -> str:
    """Human-readable (secret-free) description of the session location."""
    session_file = f"{settings.session_name}.session"
    return os.path.join(str(settings.session_dir), session_file)
