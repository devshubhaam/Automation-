"""Telegram userbot integration."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional

from telethon import TelegramClient, events
from telethon.errors import AuthKeyDuplicatedError
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
    - Maintain Telethon user session.
    - Restrict processing to the owner account.
    - Accept ZIP archives.
    - Create jobs.
    - Submit jobs to PipelineWorker.
    - Provide /ping, /start, /status and /cancel.
    - Persist StringSession to MongoDB.
    - Recover automatically from a revoked (AuthKeyDuplicated) session.
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

        # --------------------------------------------------------------
        # Telegram session source
        # --------------------------------------------------------------

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
                session_source = StringSession()

                logger.info(
                    "No Telegram session found in MongoDB; "
                    "QR login will create the first session"
                )

        else:
            logger.info(
                "MONGODB_URI not configured; "
                "using local Telegram session"
            )

        # --------------------------------------------------------------
        # Telethon client
        # --------------------------------------------------------------

        self.client = self._build_client(
            session_source
        )

        self.owner_id: Optional[int] = None
        self.owner_username: Optional[str] = None

        self._handlers_registered = False

    # ==================================================================
    # CLIENT FACTORY
    # ==================================================================

    def _build_client(
        self,
        session_source,
    ) -> TelegramClient:
        """Create a Telethon client for the given session source."""

        return TelegramClient(
            session_source,
            self.settings.api_id,
            self.settings.api_hash,
            device_model="Media Processor",
            system_version="PART-2",
            app_version="0.2.0",
        )

    # ==================================================================
    # START / STOP
    # ==================================================================

    async def start(self) -> None:
        """Connect to Telegram.

        Interactive first-time login is handled by LoginBot.

        If Telegram revoked the saved session because it was used from
        two IP addresses at once (AuthKeyDuplicatedError), the dead
        session is discarded and a fresh unauthorized client is created
        so the LoginBot can perform a new QR login instead of the whole
        application crash-looping.
        """

        logger.info(
            "Telegram client starting"
        )

        try:
            await self._connect_and_check()

        except AuthKeyDuplicatedError:
            logger.error(
                "Telegram session was revoked "
                "(AuthKeyDuplicatedError: used from two IPs at once). "
                "Discarding it; use /login on the login bot "
                "to authorize again. Make sure only ONE instance "
                "of this app uses the session."
            )

            await self._reset_dead_session()

            # A brand-new session cannot be duplicated, so any error
            # from here on is a real failure.
            await self._connect_and_check()

    async def _connect_and_check(self) -> None:
        """Connect and, if already authorized, finish setup."""

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

    async def _reset_dead_session(self) -> None:
        """Delete the revoked session and build a fresh client."""

        try:
            await self.client.disconnect()

        except Exception:
            logger.debug(
                "Ignoring error while disconnecting dead client",
                exc_info=True,
            )

        if self.session_store is not None:
            try:
                await asyncio.to_thread(
                    self.session_store.delete,
                    self.settings.session_name,
                )

                logger.info(
                    "Deleted revoked Telegram session from MongoDB"
                )

            except Exception:
                logger.exception(
                    "Failed to delete revoked session from MongoDB"
                )

            fresh_source = StringSession()

        else:
            base = str(self.settings.session_path)

            candidates = {
                Path(base),
                Path(base + ".session"),
            }

            for path in candidates:
                try:
                    if path.is_file():
                        path.unlink()

                        logger.info(
                            "Deleted revoked local session file %s",
                            path,
                        )

                except Exception:
                    logger.exception(
                        "Failed to delete local session file %s",
                        path,
                    )

            fresh_source = base

        self.owner_id = None
        self.owner_username = None
        self._handlers_registered = False

        self.client = self._build_client(
            fresh_source
        )

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
        """Persist session and disconnect Telegram."""

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

    # ==================================================================
    # EVENT HANDLERS
    # ==================================================================

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

        # --------------------------------------------------------------
        # Commands
        # --------------------------------------------------------------

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

        # --------------------------------------------------------------
        # ZIP document
        # --------------------------------------------------------------

        if message.document:
            await self.handle_zip(message)

    # ==================================================================
    # OWNER CHECK
    # ==================================================================

    def is_owner_message(
        self,
        event,
    ) -> bool:
        """Allow only messages belonging to authenticated owner."""

        if self.owner_id is None:
            return False

        # Outgoing messages originate from the authenticated account.
        if event.out:
            return True

        sender_id = event.sender_id

        if sender_id is None:
            return False

        return int(sender_id) == int(
            self.owner_id
        )

    # ==================================================================
    # STATUS
    # ==================================================================

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
            f"Queued jobs: {self.pipeline.queue_size()}",
            "",
        ]

        # Latest 10 jobs only.
        for job in jobs[-10:]:
            lines.append(
                f"<code>{self._escape(job.job_id)}</code> "
                f"— {self._escape(job.status)}"
            )

        await event.reply(
            "\n".join(lines),
            parse_mode="html",
        )

    # ==================================================================
    # CANCEL
    # ==================================================================

    async def handle_cancel(
        self,
        event,
        text: str,
    ) -> None:
        """Cancel a job."""

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
                f"❌ Job "
                f"`{self._escape(job_id)}` "
                f"was not found.",
                parse_mode="html",
            )
            return

        terminal_states = {
            JobStatus.COMPLETED,
            JobStatus.COMPLETED_WITH_ERRORS,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        }

        if job.status in terminal_states:
            await event.reply(
                f"ℹ️ Job "
                f"`{self._escape(job_id)}` "
                f"is already "
                f"`{self._escape(job.status)}`.",
                parse_mode="html",
            )
            return

        try:
            self.job_manager.cancel(
                job_id
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
            f"<code>{self._escape(job_id)}</code>. "
            f"It will stop at the next safe point.",
            parse_mode="html",
        )

    # ==================================================================
    # ZIP HANDLING
    # ==================================================================

    async def handle_zip(
        self,
        message: Message,
    ) -> None:
        """Validate and submit a ZIP archive."""

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

        job = None

        try:
            # ----------------------------------------------------------
            # Create job
            # ----------------------------------------------------------

            job_id = self.job_manager.generate_job_id()

            job = self.job_manager.create_job(
                job_id=job_id,
                user_id=int(sender_id),
                chat_id=int(chat_id),
                archive_name=filename,
                message_id=int(message.id),
            )

            # Runtime-only Telegram Message object.
            # PipelineWorker uses this for download_media().
            job._telegram_message = message

            # ----------------------------------------------------------
            # Create editable status message
            # ----------------------------------------------------------

            status_message = await message.reply(
                "📥 <b>ZIP received.</b>\n\n"
                f"<b>Job:</b> "
                f"<code>{self._escape(job.job_id)}</code>\n"
                f"<b>File:</b> "
                f"{self._escape(filename)}\n\n"
                "⏳ Queuing job...",
                parse_mode="html",
            )

            self.job_manager.set_status_message(
                job.job_id,
                int(status_message.id),
            )

            # Keep runtime reference so PipelineWorker can edit it.
            job._status_message = status_message

            # ----------------------------------------------------------
            # State: QUEUED (JobManager keeps state only, no queue)
            # ----------------------------------------------------------

            self.job_manager.mark_queued(
                job.job_id
            )

            # ----------------------------------------------------------
            # Submit to the one real processing queue (PipelineWorker)
            # ----------------------------------------------------------

            await self.pipeline.submit(
                job
            )

            logger.info(
                "ZIP accepted: "
                "job_id=%s filename=%s size=%s",
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

            if job is not None:
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

                try:
                    await self._safe_edit(
                        job,
                        "❌ <b>Job failed to start.</b>\n\n"
                        f"<code>{self._escape(job.job_id)}</code>\n"
                        f"{self._escape(str(exc))}",
                    )

                except Exception:
                    logger.exception(
                        "Failed to update failed-job message"
                    )

            else:
                await message.reply(
                    "❌ Failed to start processing."
                )

    # ==================================================================
    # HELPERS
    # ==================================================================

    async def _safe_edit(
        self,
        job,
        text: str,
    ) -> None:
        """Safely edit the job's status message."""

        status_message = getattr(
            job,
            "_status_message",
            None,
        )

        if status_message is None:
            status_message_id = (
                job.status_message_id
            )

            if status_message_id is None:
                return

            status_message = await self.client.get_messages(
                job.chat_id,
                ids=status_message_id,
            )

        if status_message is None:
            return

        try:
            await status_message.edit(
                text,
                parse_mode="html",
            )

        except Exception:
            logger.exception(
                "Failed to edit status message for job %s",
                job.job_id,
            )

    @staticmethod
    def _document_filename(
        message: Message,
    ) -> str:
        """Get safe filename from Telegram document."""

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
