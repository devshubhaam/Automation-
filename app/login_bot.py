"""Owner-only Telegram bot used to authorize the userbot via QR login."""

from __future__ import annotations

import asyncio
import io
import logging
import os
from typing import Optional

import qrcode
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError

from .config import Settings

logger = logging.getLogger("app.login_bot")

__all__ = ["LoginBot"]


class LoginBot:
    """Owner-only bot for QR-based Telethon authentication."""

    def __init__(
        self,
        settings: Settings,
        userbot,
    ) -> None:
        self.settings = settings
        self.userbot = userbot

        self._client: Optional[TelegramClient] = None

        self._authorized_event = asyncio.Event()

        # Prevent multiple /login commands from starting
        # multiple QR login flows at the same time.
        self._login_lock = asyncio.Lock()

        self._running = False
        self._handlers_registered = False

    # ------------------------------------------------------------------ #
    # Login bot client
    # ------------------------------------------------------------------ #

    @property
    def client(self) -> TelegramClient:
        """Return the Telegram login-bot client."""

        if self._client is None:
            self.settings.session_dir.mkdir(
                parents=True,
                exist_ok=True,
            )

            self._client = TelegramClient(
                str(
                    self.settings.session_dir
                    / "login_bot"
                ),
                self.settings.api_id,
                self.settings.api_hash,
                device_model="Media Processor Login Bot",
                system_version="PART-1",
                app_version="0.1.0",
            )

        return self._client

    # ------------------------------------------------------------------ #
    # Owner verification
    # ------------------------------------------------------------------ #

    def is_owner(
        self,
        event: events.NewMessage.Event,
    ) -> bool:
        """Allow commands only from BOT_OWNER_ID."""

        if self.settings.bot_owner_id is None:
            return False

        try:
            return (
                int(event.sender_id)
                == int(self.settings.bot_owner_id)
            )

        except (
            TypeError,
            ValueError,
        ):
            return False

    # ------------------------------------------------------------------ #
    # Start / stop
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Start the owner-only login bot."""

        if not self.settings.bot_token:
            logger.info(
                "BOT_TOKEN not configured; "
                "login bot disabled"
            )
            return

        if self.settings.bot_owner_id is None:
            raise RuntimeError(
                "BOT_OWNER_ID is required when "
                "BOT_TOKEN is configured"
            )

        logger.info(
            "Starting Telegram login bot"
        )

        await self.client.start(
            bot_token=self.settings.bot_token
        )

        self._register_handlers()

        self._running = True

        me = await self.client.get_me()

        logger.info(
            "Login bot started as @%s",
            getattr(
                me,
                "username",
                None,
            )
            or "none",
        )

    async def stop(self) -> None:
        """Stop the login bot."""

        self._running = False

        if self._client is not None:
            try:
                await self._client.disconnect()

            except Exception:
                logger.exception(
                    "Failed to disconnect login bot"
                )

        logger.info(
            "Telegram login bot stopped"
        )

    # ------------------------------------------------------------------ #
    # Event handlers
    # ------------------------------------------------------------------ #

    def _register_handlers(self) -> None:
        """Register login-bot handlers exactly once."""

        if self._handlers_registered:
            return

        self._handlers_registered = True

        @self.client.on(
            events.NewMessage(
                incoming=True
            )
        )
        async def handler(
            event: events.NewMessage.Event,
        ) -> None:
            await self._handle_message(event)

        logger.info(
            "Telegram login-bot handlers registered"
        )

    async def _handle_message(
        self,
        event: events.NewMessage.Event,
    ) -> None:
        """Handle commands sent to the login bot."""

        if not self.is_owner(event):
            return

        text = (
            event.raw_text or ""
        ).strip()

        command = (
            text.split(
                maxsplit=1
            )[0].lower()
            if text
            else ""
        )

        if command == "/start":
            await event.reply(
                "🤖 Media Processor Login Bot\n\n"
                "Use /login to authorize the userbot via QR.\n"
                "Use /status to check login status."
            )
            return

        if command == "/status":
            await self._handle_status(event)
            return

        if command == "/login":
            await self._run_qr_login(event)
            return

    # ------------------------------------------------------------------ #
    # Status
    # ------------------------------------------------------------------ #

    async def _handle_status(
        self,
        event: events.NewMessage.Event,
    ) -> None:
        """Show current userbot authentication status."""

        try:
            client = self.userbot.client

            if not client.is_connected():
                await client.connect()

            if not await client.is_user_authorized():
                await event.reply(
                    "❌ Userbot is not authorized.\n\n"
                    "Use /login to start QR login."
                )
                return

            me = await client.get_me()

            if me is None:
                await event.reply(
                    "❌ Userbot session is authorized, "
                    "but account information is unavailable."
                )
                return

            await event.reply(
                "✅ Userbot is authorized.\n\n"
                f"ID: {me.id}\n"
                f"Username: "
                f"@{getattr(me, 'username', None) or 'none'}"
            )

        except Exception:
            logger.exception(
                "Failed to check userbot login status"
            )

            await event.reply(
                "❌ Could not check login status."
            )

    # ------------------------------------------------------------------ #
    # QR login entry
    # ------------------------------------------------------------------ #

    async def _run_qr_login(
        self,
        event: events.NewMessage.Event,
    ) -> None:
        """Run one QR login attempt at a time."""

        if self._login_lock.locked():
            await event.reply(
                "⏳ A login attempt is already running.\n"
                "Please wait for it to finish."
            )
            return

        async with self._login_lock:
            await self._perform_qr_login(event)

    # ------------------------------------------------------------------ #
    # QR image
    # ------------------------------------------------------------------ #

    async def _send_qr(
        self,
        event: events.NewMessage.Event,
        qr_url: str,
    ) -> None:
        """Generate and send a QR image."""

        qr = qrcode.make(qr_url)

        buffer = io.BytesIO()

        qr.save(
            buffer,
            format="PNG",
        )

        buffer.seek(0)

        buffer.name = (
            "telegram_login_qr.png"
        )

        await self.client.send_file(
            event.chat_id,
            buffer,
            force_document=False,
            caption=(
                "📱 Scan this QR in Telegram.\n\n"
                "Telegram → Settings → Devices → "
                "Link Desktop Device"
            ),
        )

    # ------------------------------------------------------------------ #
    # Finalize authentication
    # ------------------------------------------------------------------ #

    async def _finish_login(
        self,
        event: events.NewMessage.Event,
    ) -> bool:
        """
        Verify the logged-in account, register the userbot,
        and persist the session to MongoDB.
        """

        client = self.userbot.client

        me = await client.get_me()

        if me is None:
            await event.reply(
                "❌ Login failed: Telegram account "
                "information unavailable."
            )
            return False

        # -------------------------------------------------------------- #
        # Make sure the QR was scanned by our owner account.
        # -------------------------------------------------------------- #

        expected_owner = (
            self.settings.bot_owner_id
        )

        if (
            expected_owner is not None
            and int(me.id)
            != int(expected_owner)
        ):
            logger.warning(
                "QR login completed by unexpected "
                "Telegram account: %s",
                me.id,
            )

            try:
                await client.log_out()

            except Exception:
                logger.exception(
                    "Failed to log out unexpected "
                    "Telegram account"
                )

            await event.reply(
                "❌ QR was scanned by a different "
                "Telegram account.\n\n"
                "That session has been logged out."
            )

            return False

        # -------------------------------------------------------------- #
        # Tell the userbot that authentication succeeded.
        # -------------------------------------------------------------- #

        self.userbot.finish_authenticated_account(
            me
        )

        # -------------------------------------------------------------- #
        # Persist session immediately.
        # -------------------------------------------------------------- #

        if self.userbot.session_store is not None:

            try:
                await self.userbot.persist_session()

                logger.info(
                    "Telegram session persisted to MongoDB "
                    "after successful QR login"
                )

            except Exception:
                logger.exception(
                    "Telegram authentication succeeded, "
                    "but MongoDB session persistence failed"
                )

                await event.reply(
                    "⚠️ Telegram login successful, "
                    "but the session could not be saved "
                    "to MongoDB.\n\n"
                    "Do NOT restart/redeploy yet. "
                    "Check MongoDB configuration."
                )

                return False

        # -------------------------------------------------------------- #
        # Signal main.py that authentication is complete.
        # -------------------------------------------------------------- #

        self._authorized_event.set()

        logger.info(
            "Userbot authenticated successfully "
            "(owner_id=%s, username=%s)",
            me.id,
            getattr(
                me,
                "username",
                None,
            )
            or "none",
        )

        await event.reply(
            "✅ Telegram userbot login successful!\n\n"
            f"Account ID: {me.id}\n"
            f"Username: "
            f"@{getattr(me, 'username', None) or 'none'}\n\n"
            "Userbot is now ready.\n"
            "Session saved securely."
        )

        return True

    # ------------------------------------------------------------------ #
    # QR authentication
    # ------------------------------------------------------------------ #

    async def _perform_qr_login(
        self,
        event: events.NewMessage.Event,
    ) -> None:
        """
        Perform QR authentication.

        IMPORTANT:
        qr_login.wait() is started BEFORE the QR is sent to Telegram.
        This prevents the QR from being scanned before Telethon starts
        listening for the login-token update.
        """

        client = self.userbot.client

        try:
            if not client.is_connected():
                await client.connect()

            # ---------------------------------------------------------- #
            # Existing authorized session
            # ---------------------------------------------------------- #

            if await client.is_user_authorized():

                logger.info(
                    "Userbot is already authorized; "
                    "QR login is not required"
                )

                # IMPORTANT:
                # The main application is already using this
                # authorized Telegram session.
                #
                # Do NOT call _finish_login() here.
                #
                # Calling _finish_login() again performs get_me()
                # on the already-active authorization key and can
                # cause AuthKeyDuplicatedError when the same
                # authorization key is active from another process
                # or instance.
                await event.reply(
                    "✅ Userbot is already authorized.\n\n"
                    "QR login is not required.\n"
                    "Use /status to check the current session."
                )

                return

            # ---------------------------------------------------------- #
            # 2FA password
            # ---------------------------------------------------------- #

            password = os.getenv(
                "TELEGRAM_2FA_PASSWORD"
            )

            if not password:
                logger.warning(
                    "TELEGRAM_2FA_PASSWORD is not configured"
                )

                await event.reply(
                    "🔒 Telegram 2FA password is required.\n\n"
                    "Add `TELEGRAM_2FA_PASSWORD` "
                    "to Koyeb Environment Variables, "
                    "then use /login again."
                )

                return

            await event.reply(
                "🔐 QR login starting...\n\n"
                "Telegram app me:\n"
                "Settings → Devices → "
                "Link Desktop Device\n\n"
                "A fresh QR will be sent."
            )

            # ---------------------------------------------------------- #
            # Create QR login object.
            # ---------------------------------------------------------- #

            qr_login = await client.qr_login()

            logger.info(
                "Created Telegram QR login token"
            )

            while True:

                # ------------------------------------------------------ #
                # CRITICAL:
                #
                # Start waiting BEFORE sending the QR.
                # ------------------------------------------------------ #

                wait_task = asyncio.create_task(
                    qr_login.wait()
                )

                try:
                    # -------------------------------------------------- #
                    # Send QR after wait() is already listening.
                    # -------------------------------------------------- #

                    await self._send_qr(
                        event,
                        qr_login.url,
                    )

                    logger.info(
                        "QR login code sent to owner"
                    )

                    # -------------------------------------------------- #
                    # Wait for scan/login result.
                    # -------------------------------------------------- #

                    try:
                        await wait_task

                    except SessionPasswordNeededError:
                        logger.info(
                            "QR scan successful; "
                            "Telegram requires 2FA password"
                        )

                        try:
                            await client.sign_in(
                                password=password
                            )

                        except Exception:
                            logger.exception(
                                "Telegram 2FA authentication failed"
                            )

                            await event.reply(
                                "❌ Telegram 2FA authentication failed.\n\n"
                                "Check `TELEGRAM_2FA_PASSWORD` "
                                "in Koyeb and try /login again."
                            )

                            return

                    break

                except asyncio.TimeoutError:

                    logger.info(
                        "QR token expired; "
                        "recreating QR token"
                    )

                    # -------------------------------------------------- #
                    # Cancel old wait task.
                    # -------------------------------------------------- #

                    if not wait_task.done():
                        wait_task.cancel()

                        try:
                            await wait_task

                        except asyncio.CancelledError:
                            pass

                    # -------------------------------------------------- #
                    # Recreate expired QR token.
                    # -------------------------------------------------- #

                    await qr_login.recreate()

                    await event.reply(
                        "⏳ QR expire ho gaya.\n\n"
                        "Naya QR bhej raha hoon..."
                    )

                    continue

                except SessionPasswordNeededError:

                    logger.info(
                        "QR accepted; Telegram "
                        "requires 2FA password"
                    )

                    try:
                        await client.sign_in(
                            password=password
                        )

                    except Exception:
                        logger.exception(
                            "Telegram 2FA authentication failed"
                        )

                        await event.reply(
                            "❌ Telegram 2FA authentication failed.\n\n"
                            "Check `TELEGRAM_2FA_PASSWORD` "
                            "in Koyeb."
                        )

                        return

                    break

                except Exception:
                    logger.exception(
                        "Unexpected QR wait error"
                    )

                    if not wait_task.done():
                        wait_task.cancel()

                        try:
                            await wait_task

                        except asyncio.CancelledError:
                            pass

                    raise

                finally:
                    # -------------------------------------------------- #
                    # Always clean up wait task.
                    # -------------------------------------------------- #

                    if not wait_task.done():
                        wait_task.cancel()

                        try:
                            await wait_task

                        except asyncio.CancelledError:
                            pass

            # ---------------------------------------------------------- #
            # Authentication completed.
            # ---------------------------------------------------------- #

            if not await client.is_user_authorized():
                await event.reply(
                    "❌ QR flow finished, but Telegram "
                    "did not authorize the userbot."
                )

                logger.error(
                    "QR flow completed but "
                    "client is not authorized"
                )

                return

            await self._finish_login(
                event
            )

        except asyncio.CancelledError:
            logger.info(
                "QR login task cancelled"
            )
            raise

        except Exception as exc:
            logger.exception(
                "QR login failed"
            )

            await event.reply(
                "❌ QR login failed.\n\n"
                f"Reason: {type(exc).__name__}"
            )

    # ------------------------------------------------------------------ #
    # Wait for authorization
    # ------------------------------------------------------------------ #

    async def wait_until_authorized(self) -> None:
        """Wait until QR login successfully authorizes the userbot."""

        await self._authorized_event.wait()
