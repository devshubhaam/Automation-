"""Telegram userbot integration.

This module owns the Telethon userbot client, authentication state,
message handlers, and Telegram-facing job submission.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from telethon import TelegramClient, events
from telethon.tl.custom import Message

from .config import Settings
from .job_manager import JobManager

logger = logging.getLogger("app.telegram_client")

__all__ = ["TelegramUserbot"]


class TelegramUserbot:
    """Telegram userbot wrapper used by the media-processing pipeline."""

    def __init__(
        self,
        settings: Settings,
        job_manager: JobManager,
        pipeline,
    ) -> None:
        self.settings = settings
        self.job_manager = job_manager
        self.pipeline = pipeline

        self.client = TelegramClient(
            str(settings.session_path),
            settings.api_id,
            settings.api_hash,
            device_model="Media Processor",
            system_version="PART-1",
            app_version="0.1.0",
        )

        self.owner_id: Optional[int] = None
        self.owner_username: Optional[str] = None

        self._handlers_registered = False

    # ------------------------------------------------------------------ #
    # Authentication
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Connect the userbot.

        Authentication is intentionally non-interactive.

        If the Telethon session is already authorized, the authenticated
        account is initialized immediately.

        If it is not authorized, the QR login bot is responsible for
        completing authentication.
        """

        logger.info("Telegram client starting")

        await self.client.connect()

        if not await self.client.is_user_authorized():
            logger.info(
                "Telegram session is not authorized; "
                "waiting for QR login bot"
            )
            return

        me = await self.client.get_me()

        if me is None:
            raise RuntimeError(
                "Telegram session is authorized but get_me() returned None"
            )

        self.finish_authenticated_account(me)

    def finish_authenticated_account(self, me) -> None:
        """Finalize userbot setup after successful authentication."""

        self.owner_id = int(me.id)
        self.owner_username = getattr(me, "username", None)

        logger.info(
            "Telegram authentication successful "
            "(owner_id=%s, username=%s)",
            self.owner_id,
            self.owner_username or "none",
        )

        configured_owner_id = self.settings.owner_id

        if (
            configured_owner_id is not None
            and int(configured_owner_id) != self.owner_id
        ):
            logger.warning(
                "OWNER_ID does not match authenticated Telegram account: "
                "configured=%s authenticated=%s",
                configured_owner_id,
                self.owner_id,
            )

        self.register_handlers()

    async def stop(self) -> None:
        """Disconnect the Telegram client."""

        if self.client.is_connected():
            await self.client.disconnect()

        logger.info("Telegram client stopped")

    # ------------------------------------------------------------------ #
    # Event registration
    # ------------------------------------------------------------------ #

    def register_handlers(self) -> None:
        """Register Telegram message handlers exactly once."""

        if self._handlers_registered:
            return

        self._handlers_registered = True

        @self.client.on(events.NewMessage(outgoing=True))
        async def outgoing_handler(event: events.NewMessage.Event) -> None:
            await self._handle_message(event)

        @self.client.on(
            events.NewMessage(
                incoming=True,
                func=lambda event: event.is_private,
            )
        )
        async def incoming_handler(event: events.NewMessage.Event) -> None:
            await self._handle_message(event)

        logger.info("Telegram message handlers registered")

    async def _handle_message(
        self,
        event: events.NewMessage.Event,
    ) -> None:
        """Route an incoming/outgoing Telegram message."""

        if not self.is_owner_message(event):
            return

        message = event.message

        if not isinstance(message, Message):
            return

        text = (message.raw_text or "").strip()

        if not text:
            if message.document:
                await self.handle_zip(message)
            return

        command = text.split(maxsplit=1)[0].lower()

        if command == "/ping":
            await event.reply("🏓 pong")
            logger.info("Handled /ping")
            return

        if command == "/start":
            await event.reply(
                "🤖 Media Processor\n\n"
                "Send a ZIP file to start processing.\n"
                "Use /status to check jobs.\n"
                "Use /cancel <job_id> to cancel a queued job."
            )
            logger.info("Handled /start")
            return

        if command == "/status":
            await self.handle_status(event)
            return

        if command == "/cancel":
            await self.handle_cancel(event, text)
            return

        # A ZIP may arrive with a caption. If the message contains a
        # document, let the ZIP handler inspect the filename/MIME type.
        if message.document:
            await self.handle_zip(message)

    # ------------------------------------------------------------------ #
    # Authorization
    # ------------------------------------------------------------------ #

    def is_owner_message(
        self,
        event: events.NewMessage.Event,
    ) -> bool:
        """Return True only for the authenticated owner account."""

        if self.owner_id is None:
            return False

        # Outgoing messages are generated by the authenticated account.
        if event.out:
            return True

        sender_id = event.sender_id

        if sender_id is None:
            return False

        return int(sender_id) == int(self.owner_id)

    # ------------------------------------------------------------------ #
    # Commands
    # ------------------------------------------------------------------ #

    async def handle_status(
        self,
        event: events.NewMessage.Event,
    ) -> None:
        """Show current processor status."""

        try:
            status = self.job_manager.status()
        except AttributeError:
            # Compatibility fallback if JobManager exposes a different
            # status API in an older revision.
            await event.reply(
                "📊 Status unavailable right now."
            )
            return

        queued = status.get("queued", 0)
        running = status.get("running", 0)
        completed = status.get("completed", 0)
        failed = status.get("failed", 0)
        cancelled = status.get("cancelled", 0)

        await event.reply(
            "📊 Media Processor Status\n\n"
            f"Queued: {queued}\n"
            f"Running: {running}\n"
            f"Completed: {completed}\n"
            f"Failed: {failed}\n"
            f"Cancelled: {cancelled}"
        )

        logger.info("Handled /status")

    async def handle_cancel(
        self,
        event: events.NewMessage.Event,
        text: str,
    ) -> None:
        """Cancel a job by ID."""

        parts = text.split(maxsplit=1)

        if len(parts) != 2 or not parts[1].strip():
            await event.reply(
                "Usage:\n"
                "`/cancel <job_id>`"
            )
            return

        job_id = parts[1].strip()

        try:
            cancelled = self.job_manager.cancel(job_id)
        except AttributeError:
            await event.reply(
                "❌ Job cancellation is unavailable right now."
            )
            return

        if cancelled:
            await event.reply(
                f"🛑 Job `{job_id}` cancellation requested."
            )
            logger.info(
                "Job cancellation requested: %s",
                job_id,
            )
        else:
            await event.reply(
                f"❌ Job `{job_id}` was not found or cannot be cancelled."
            )

    # ------------------------------------------------------------------ #
    # ZIP handling
    # ------------------------------------------------------------------ #

    async def handle_zip(
        self,
        message: Message,
    ) -> None:
        """Validate a Telegram document and submit it as a processing job."""

        if not message.document:
            return

        filename = self._document_filename(message)

        mime_type = getattr(
            message.document,
            "mime_type",
            None,
        )

        is_zip = (
            filename.lower().endswith(".zip")
            or mime_type == "application/zip"
            or mime_type == "application/x-zip-compressed"
        )

        if not is_zip:
            await message.reply(
                "❌ Please send a ZIP archive."
            )
            logger.info(
                "Rejected non-ZIP document: filename=%s mime=%s",
                filename,
                mime_type,
            )
            return

        try:
            job = await self.job_manager.create_job(
                telegram_message_id=message.id,
                filename=filename,
            )
        except Exception:
            logger.exception("Failed to create processing job")

            await message.reply(
                "❌ Could not create a processing job."
            )
            return

        logger.info(
            "ZIP accepted: job_id=%s filename=%s",
            job.job_id,
            filename,
        )

        try:
            await self.job_manager.start_progress(job.job_id)

            # Keep the JobManager's queue/state in sync with the
            # pipeline submission.
            await self.job_manager.enqueue(job.job_id)

            await self.pipeline.submit(job)

        except Exception:
            logger.exception(
                "Failed to submit job: %s",
                job.job_id,
            )

            try:
                await self.job_manager.mark_failed(
                    job.job_id,
                    "Failed to submit processing job",
                )
            except Exception:
                logger.exception(
                    "Failed to mark submission failure: %s",
                    job.job_id,
                )

            await message.reply(
                f"❌ Failed to start job `{job.job_id}`."
            )
            return

        await message.reply(
            "📦 ZIP received.\n\n"
            f"Job ID: `{job.job_id}`\n"
            f"File: `{filename}`\n\n"
            "Processing started."
        )

    @staticmethod
    def _document_filename(
        message: Message,
    ) -> str:
        """Extract a safe display filename from a Telegram document."""

        document = message.document

        if document is None:
            return "archive.zip"

        for attribute in getattr(
            document,
            "attributes",
            [],
        ):
            filename = getattr(
                attribute,
                "file_name",
                None,
            )

            if filename:
                return Path(filename).name

        return "archive.zip"
