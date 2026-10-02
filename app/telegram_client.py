"""Telegram userbot integration."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.custom import Message

from .config import Settings
from .job_manager import JobManager, JobStatus
from .session_store import MongoSessionStore

logger = logging.getLogger("app.telegram_client")

__all__ = ["TelegramUserbot"]


class TelegramUserbot:
    """Telegram userbot wrapper.

    Responsibilities:
    - Maintain the Telethon user session.
    - Restrict commands/media processing to the owner.
    - Accept ZIP archives.
    - Create jobs using the current JobManager API.
    - Submit jobs to PipelineWorker.
    - Provide /ping, /start, /status and /cancel.
    - Persist the Telegram StringSession to MongoDB.
    """

    def __init__(
        self,
        settings: Settings,
        job_manager: JobManager,
        pipeline,
    ) -> None:
        self.settings = settings
        self.job_manager = job_manager
        self.pipeline = pipeline

        self.session_store: Optional[MongoSessionStore] = None

        # ------------------------------------------------------------------
        # Telegram session source
        # ------------------------------------------------------------------

        session_source = str(settings.session_path)

        if settings.mongodb_uri:
            logger.info(
                "MongoDB session storage is configured"
            )

            self.session_store = MongoSessionStore(
                settings.mongodb_uri,
                database=settings.mongodb_database,
                collection=settings.mongodb_collection,
            )

            try:
                self.session_store.ping()

                logger.info(
                    "MongoDB connection successful"
                )

            except Exception:
                logger.exception(
                    "MongoDB connection failed"
                )
                raise

            stored_session = self.session_store.load(
                settings.session_name
            )

            if stored_session:
                session_source = StringSession(
                    stored_session
                )

                logger.info(
                    "Loaded Telegram session from MongoDB"
                )

            else:
                logger.info(
                    "No Telegram session found in MongoDB; "
                    "QR login will create the first session"
                )

        else:
            logger.info(
                "MONGODB_URI not configured; "
                "using local Telegram session"
            )

        # ------------------------------------------------------------------
        # Telethon client
        # ------------------------------------------------------------------

        self.client = TelegramClient(
            session_source,
            settings.api_id,
            settings.api_hash,
            device_model="Media Processor",
            system_version="PART-2",
            app_version="0.2.0",
        )

        self.owner_id: Optional[int] = None
        self.owner_username: Optional[str] = None

        self._handlers_registered = False

    # ======================================================================
    # START / STOP
    # ======================================================================

    async def start(self) -> None:
        """Connect to Telegram.

        This method does not perform interactive login.
        First-time authentication is handled by LoginBot/QR flow.
        """

        logger.info(
            "Telegram client starting"
        )

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
                "Telegram session is authorized but account "
                "information could not be loaded"
            )

        self.finish_authenticated_account(me)

    def finish_authenticated_account(
        self,
        me,
    ) -> None:
        """Finalize the userbot after successful authentication."""

        self.owner_id = int(me.id)

        self.owner_username = getattr(
            me,
            "username",
            None,
        )

        logger.info(
            "Telegram authentication successful "
            "(owner_id=%s, username=%s)",
            self.owner_id,
            self.owner_username or "none",
        )

        configured_owner_id = getattr(
            self.settings,
            "owner_id",
            None,
        )

        if (
            configured_owner_id is not None
            and int(configured_owner_id) != self.owner_id
        ):
            raise RuntimeError(
                "Authenticated Telegram account does not match "
                "configured BOT_OWNER_ID/OWNER_ID"
            )

        self.register_handlers()

    async def persist_session(self) -> None:
        """Persist current authorized Telegram session to MongoDB."""

        if self.session_store is None:
            logger.debug(
                "MongoDB session storage is disabled"
            )
            return

        if not await self.client.is_user_authorized():
            logger.warning(
                "Cannot persist Telegram session: "
                "client is not authorized"
            )
            return

        try:
            session_string = StringSession.save(
                self.client.session
            )

            await asyncio.to_thread(
                self.session_store.save,
                self.settings.session_name,
                session_string,
            )

            logger.info(
                "Telegram session persisted to MongoDB"
            )

        except Exception:
            logger.exception(
                "Failed to persist Telegram session to MongoDB"
            )
            raise

    async def stop(self) -> None:
        """Persist the session and disconnect Telegram."""

        if self.session_store is not None:
            try:
                await self.persist_session()

            except Exception:
                logger.exception(
                    "Failed to persist Telegram session "
                    "before shutdown"
                )

        if self.client.is_connected():
            await self.client.disconnect()

        if self.session_store is not None:
            try:
                self.session_store.close()

            except Exception:
                logger.exception(
                    "Failed to close MongoDB session store"
                )

        logger.info(
            "Telegram client stopped"
        )

    # ======================================================================
    # EVENT HANDLERS
    # ======================================================================

    def register_handlers(self) -> None:
        """Register Telegram handlers exactly once."""

        if self._handlers_registered:
            return

        self._handlers_registered = True

        @self.client.on(
            events.NewMessage(
                outgoing=True,
            )
        )
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

        logger.info(
            "Telegram message handlers registered"
        )

    async def _handle_message(
        self,
        event,
    ) -> None:
        """Handle owner Telegram messages."""

        if not self.is_owner_message(event):
            return

        message = event.message

        if not isinstance(message, Message):
            return

        text = (
            message.raw_text or ""
        ).strip()

        # ------------------------------------------------------------------
        # Commands
        # ------------------------------------------------------------------

        if text:
            command = (
                text.split(
                    maxsplit=1
                )[0]
                .lower()
            )

            if command == "/ping":
                await event.reply(
                    "🏓 pong"
                )
                return

            if command == "/start":
                await event.reply(
                    "🤖 <b>Media Processor</b>\n\n"
                    "Send a ZIP file to start processing.\n\n"
                    "/status — show jobs\n"
                    "/cancel &lt;job_id&gt; — cancel a job\n"
                    "/ping — health check",
                    parse_mode="html",
                )
                return

            if command == "/status":
                await self.handle_status(event)
                return

            if command == "/cancel":
                await self.handle_cancel(
                    event,
                    text,
                )
                return

        # ------------------------------------------------------------------
        # ZIP document
        # ------------------------------------------------------------------

        if message.document:
            await self.handle_zip(message)

    # ======================================================================
    # OWNER CHECK
    # ======================================================================

    def is_owner_message(
        self,
        event,
    ) -> bool:
        """Return True only for messages belonging to the owner."""

        if self.owner_id is None:
            return False

        # Outgoing messages are generated by the authenticated account.
        if event.out:
            return True

        sender_id = event.sender_id

        if sender_id is None:
            return False

        return int(sender_id) == int(
            self.owner_id
        )

    # ======================================================================
    # STATUS
    # ======================================================================

    async def handle_status(
        self,
        event,
    ) -> None:
        """Show current jobs."""

        jobs = self.job_manager.list_jobs()

        if not jobs:
            await event.reply(
                "📊 <b>Media Processor Status</b>\n\n"
                "No jobs yet.",
                parse_mode="html",
            )
            return

        active_jobs = self.job_manager.active_jobs()

        lines = [
            "📊 <b>Media Processor Status</b>",
            "",
            f"Total jobs: {len(jobs)}",
            f"Active jobs: {len(active_jobs)}",
            f"Queued jobs: {self.job_manager.queue_size()}",
            "",
        ]

        # Show latest 10 jobs.
        for job in jobs[-10:]:
            lines.append(
                f"<code>{self._escape(job.job_id)}</code> "
                f"— {self._escape(job.status.value)}"
            )

        await event.reply(
            "\n".join(lines),
            parse_mode="html",
        )

    # ======================================================================
    # CANCEL
    # ======================================================================

    async def handle_cancel(
        self,
        event,
        text: str,
    ) -> None:
        """Cancel a job using the current JobManager API."""

        parts = text.split(
            maxsplit=1
        )

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

        job = self.job_manager.get_job(
            job_id
        )

        if job is None:
            await event.reply(
                f"❌ Job `{self._escape(job_id)}` was not found.",
                parse_mode="html",
            )
            return

        if job.status in {
            JobStatus.COMPLETED,
            JobStatus.COMPLETED_WITH_ERRORS,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        }:
            await event.reply(
                f"ℹ️ Job `{self._escape(job_id)}` is already "
                f"`{self._escape(job.status.value)}`.",
                parse_mode="html",
            )
            return

        try:
            self.job_manager.cancel(
                job_id,
                reason="Cancelled by owner",
            )

        except Exception:
            logger.exception(
                "Failed to cancel job %s",
                job_id,
            )

            await event.reply(
                f"❌ Could not cancel job "
                f"`{self._escape(job_id)}`.",
                parse_mode="html",
            )
            return

        await event.reply(
            f"🛑 Cancellation requested for job "
            f"`{self._escape(job_id)}`.",
            parse_mode="html",
        )

    # ======================================================================
    # ZIP HANDLING
    # ======================================================================

    async def handle_zip(
        self,
        message: Message,
    ) -> None:
        """Validate and submit a ZIP document."""

        if not message.document:
            return

        filename = self._document_filename(
            message
        )

        mime_type = getattr(
            message.document,
            "mime_type",
            None,
        )

        is_zip = (
            filename.lower().endswith(".zip")
            or mime_type == "application/zip"
            or mime_type
            == "application/x-zip-compressed"
        )

        if not is_zip:
            await message.reply(
                "❌ Please send a ZIP archive."
            )
            return

        document_size = int(
            getattr(
                message.document,
                "size",
                0,
            )
            or 0
        )

        # ------------------------------------------------------------------
        # Owner/user/chat IDs
        # ------------------------------------------------------------------

        sender_id = message.sender_id

        if sender_id is None:
            sender_id = self.owner_id

        if sender_id is None:
            await message.reply(
                "❌ Could not determine Telegram user ID."
            )
            return

        chat_id = message.chat_id

        if chat_id is None:
            await message.reply(
                "❌ Could not determine Telegram chat ID."
            )
            return

        # ------------------------------------------------------------------
        # Generate job ID
        # ------------------------------------------------------------------

        job_id = self.job_manager.generate_job_id()

        try:
            job = self.job_manager.create_job(
                job_id=job_id,
                user_id=int(sender_id),
                chat_id=int(chat_id),
                archive_name=filename,
                message_id=int(message.id),
            )

            # PipelineWorker uses this object to download
            # the Telegram document.
            #
            # This is runtime-only data and is intentionally not
            # serialized into JobManager state.
            job._telegram_message = message

            # ----------------------------------------------------------------
            # Create status message
            # ----------------------------------------------------------------

            status_message = await message.reply(
                "📥 <b>ZIP received.</b>\n\n"
                f"<b>Job:</b> <code>{self._escape(job.job_id)}</code>\n"
                f"<b>File:</b> {self._escape(filename)}\n\n"
                "⏳ Queuing job...",
                parse_mode="html",
            )

            self.job_manager.set_status_message(
                job.job_id,
                int(status_message.id),
            )

            # ----------------------------------------------------------------
            # Queue job
            # ----------------------------------------------------------------

            await self.job_manager.enqueue(
                job.job_id
            )

            # PipelineWorker owns actual processing.
            await self.pipeline.submit(
                job
            )

            logger.info(
                "ZIP accepted: job_id=%s filename=%s size=%s",
                job.job_id,
                filename,
                document_size,
            )

        except Exception as exc:
            logger.exception(
                "Failed to create/submit job for Telegram "
                "message %s",
                message.id,
            )

            # If a Job was created, mark it failed.
            if "job" in locals():
                try:
                    self.job_manager.set_error(
                        job.job_id,
                        f"Failed to submit processing job: {exc}",
                        failed=True,
                    )

                except Exception:
                    logger.exception(
                        "Failed to mark job %s as failed",
                        job.job_id,
                    )

            else:
                await message.reply(
                    "❌ Failed to start processing."
                )

    # ======================================================================
    # HELPERS
    # ======================================================================

    @staticmethod
    def _document_filename(
        message: Message,
    ) -> str:
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
                # Keep only the basename.
                safe_name = Path(
                    filename
                ).name

                if safe_name:
                    return safe_name

        return "archive.zip"

    @staticmethod
    def _escape(
        value: object,
    ) -> str:
        """Escape text for Telegram HTML parse mode."""

        return (
            str(value)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
