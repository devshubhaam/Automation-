"""Telegram userbot integration.

Handles Telegram authentication, owner-only message processing,
ZIP job creation, and communication with the processing pipeline.
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
    """Telegram userbot wrapper."""

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
        """Connect without interactive phone/OTP login.

        If the session is already authorized, initialize the owner
        immediately.

        If not authorized, the QR login bot will complete authentication.
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
                "Telegram session is authorized but account information "
                "could not be loaded"
            )

        self.finish_authenticated_account(me)

    def finish_authenticated_account(self, me) -> None:
        """Finalize the userbot after successful authentication."""

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
                "Configured OWNER_ID (%s) does not match "
                "authenticated account (%s)",
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
    # Telegram handlers
    # ------------------------------------------------------------------ #

    def register_handlers(self) -> None:
        """Register Telegram handlers once."""

        if self._handlers_registered:
            return

        self._handlers_registered = True

        @self.client.on(events.NewMessage(outgoing=True))
        async def outgoing_handler(event) -> None:
            await self._handle_message(event)

        @self.client.on(
            events.NewMessage(
                incoming=True,
                func=lambda event: event.is_private,
            )
        )
        async def incoming_handler(event) -> None:
            await self._handle_message(event)

        logger.info("Telegram message handlers registered")

    async def _handle_message(self, event) -> None:
        """Handle owner Telegram messages."""

        if not self.is_owner_message(event):
            return

        message = event.message

        if not isinstance(message, Message):
            return

        text = (message.raw_text or "").strip()

        # ZIP/document without useful text.
        if not text and message.document:
            await self.handle_zip(message)
            return

        if text:
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
                    "Use /cancel <job_id> to cancel a job."
                )
                logger.info("Handled /start")
                return

            if command == "/status":
                await self.handle_status(event)
                return

            if command == "/cancel":
                await self.handle_cancel(event, text)
                return

        # ZIP with caption or other text.
        if message.document:
            await self.handle_zip(message)

    # ------------------------------------------------------------------ #
    # Owner check
    # ------------------------------------------------------------------ #

    def is_owner_message(self, event) -> bool:
        """Return True only for messages belonging to the owner."""

        if self.owner_id is None:
            return False

        # Outgoing messages originate from the authenticated account.
        if event.out:
            return True

        sender_id = event.sender_id

        if sender_id is None:
            return False

        return int(sender_id) == int(self.owner_id)

    # ------------------------------------------------------------------ #
    # Commands
    # ------------------------------------------------------------------ #

    async def handle_status(self, event) -> None:
        """Show current job status."""

        try:
            status = self.job_manager.status()
        except AttributeError:
            logger.exception("JobManager status() is unavailable")

            await event.reply(
                "📊 Status is currently unavailable."
            )
            return

        if isinstance(status, dict):
            lines = [
                "📊 Media Processor Status",
                "",
            ]

            for key, value in status.items():
                lines.append(
                    f"{str(key).replace('_', ' ').title()}: {value}"
                )

            await event.reply("\n".join(lines))

        else:
            await event.reply(
                "📊 Media Processor Status\n\n"
                f"{status}"
            )

        logger.info("Handled /status")

    async def handle_cancel(
        self,
        event,
        text: str,
    ) -> None:
        """Cancel a job using its job ID."""

        parts = text.split(maxsplit=1)

        if len(parts) != 2:
            await event.reply(
                "Usage:\n"
                "/cancel <job_id>"
            )
            return

        job_id = parts[1].strip()

        if not job_id:
            await event.reply(
                "Usage:\n"
                "/cancel <job_id>"
            )
            return

        try:
            result = self.job_manager.cancel(job_id)
        except Exception:
            logger.exception(
                "Failed to cancel job %s",
                job_id,
            )

            await event.reply(
                f"❌ Could not cancel job `{job_id}`."
            )
            return

        if result:
            await event.reply(
                f"🛑 Cancellation requested for job `{job_id}`."
            )

            logger.info(
                "Job cancellation requested: %s",
                job_id,
            )
        else:
            await event.reply(
                f"❌ Job `{job_id}` was not found "
                "or cannot be cancelled."
            )

    # ------------------------------------------------------------------ #
    # ZIP processing
    # ------------------------------------------------------------------ #

    async def handle_zip(self, message: Message) -> None:
        """Validate and submit a ZIP document."""

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
            # JobManager.create_job() is synchronous in the existing
            # Part 1 architecture.
            job = self.job_manager.create_job(
                telegram_message_id=message.id,
                filename=filename,
            )

        except Exception:
            logger.exception(
                "Failed to create job for Telegram message %s",
                message.id,
            )

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
            # Existing JobManager owns its state/queue.
            self.job_manager.enqueue(
                job.job_id
            )

            # PipelineWorker owns the actual asyncio processing queue.
            await self.pipeline.submit(job)

        except Exception:
            logger.exception(
                "Failed to submit job %s",
                job.job_id,
            )

            try:
                self.job_manager.fail(
                    job.job_id,
                    "Failed to submit processing job",
                )
            except Exception:
                logger.exception(
                    "Failed to mark job %s as failed",
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
    def _document_filename(message: Message) -> str:
        """Get a safe filename from a Telegram document."""

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
