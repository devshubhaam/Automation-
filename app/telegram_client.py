"""Telegram userbot integration."""

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

    async def start(self) -> None:
        """Connect without interactive OTP login."""
        logger.info("Telegram client starting")

        await self.client.connect()

        if not await self.client.is_user_authorized():
            logger.info(
                "Telegram session is not authorized; "
                "waiting for QR login"
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
        """Finalize the userbot after authentication."""
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
        """Disconnect Telegram."""
        if self.client.is_connected():
            await self.client.disconnect()

        logger.info("Telegram client stopped")

    def register_handlers(self) -> None:
        """Register Telegram message handlers once."""
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

        if text:
            command = text.split(maxsplit=1)[0].lower()

            if command == "/ping":
                await event.reply("🏓 pong")
                return

            if command == "/start":
                await event.reply(
                    "🤖 Media Processor\n\n"
                    "Send a ZIP file to start processing.\n"
                    "Use /status to check jobs.\n"
                    "Use /cancel <job_id> to cancel a job."
                )
                return

            if command == "/status":
                await self.handle_status(event)
                return

            if command == "/cancel":
                await self.handle_cancel(event, text)
                return

        if message.document:
            await self.handle_zip(message)

    def is_owner_message(self, event) -> bool:
        """Return True only for owner messages."""
        if self.owner_id is None:
            return False

        if event.out:
            return True

        sender_id = event.sender_id

        if sender_id is None:
            return False

        return int(sender_id) == int(self.owner_id)

    async def handle_status(self, event) -> None:
        """Show current jobs."""
        jobs = self.job_manager.all_jobs()

        if not jobs:
            await event.reply(
                "📊 Media Processor Status\n\n"
                "No jobs yet."
            )
            return

        lines = [
            "📊 Media Processor Status",
            "",
            f"Total jobs: {len(jobs)}",
            f"Active jobs: {len(self.job_manager.active_jobs())}",
            f"Queued jobs: {self.job_manager.queue_size()}",
            "",
        ]

        for job in jobs[-10:]:
            lines.append(
                f"{job.job_id} — {job.status.value}"
            )

        await event.reply("\n".join(lines))

    async def handle_cancel(
        self,
        event,
        text: str,
    ) -> None:
        """Request cancellation of a job."""
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
            result = self.job_manager.request_cancel(job_id)
        except Exception:
            logger.exception(
                "Failed to request cancellation for job %s",
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
        else:
            await event.reply(
                f"❌ Job `{job_id}` was not found "
                "or is already finished."
            )

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
            return

        document_size = int(
            getattr(message.document, "size", 0) or 0
        )

        try:
            job = self.job_manager.create_job(
                archive_name=filename,
                source_message_id=message.id,
                chat_id=message.chat_id,
                sender_id=message.sender_id,
                archive_size_bytes=document_size,
            )

            # PipelineWorker needs the actual Telethon message
            # to download the document later.
            job._telegram_message = message

            self.job_manager.enqueue(job.job_id)

            await self.pipeline.submit(job)

        except Exception:
            logger.exception(
                "Failed to create/submit job for Telegram "
                "message %s",
                message.id,
            )

            try:
                if "job" in locals():
                    self.job_manager.fail(
                        job.job_id,
                        "Failed to submit processing job",
                    )
            except Exception:
                logger.exception(
                    "Failed to mark job as failed"
                )

            await message.reply(
                "❌ Failed to start processing."
            )
            return

        logger.info(
            "ZIP accepted: job_id=%s filename=%s size=%s",
            job.job_id,
            filename,
            document_size,
        )

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
