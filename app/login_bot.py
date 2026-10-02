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
        self._login_lock = asyncio.Lock()
        self._running = False
        self._handlers_registered = False

    @property
    def client(self) -> TelegramClient:
        """Return the Telegram login-bot client."""
        if self._client is None:
            self.settings.session_dir.mkdir(
                parents=True,
                exist_ok=True,
            )

            self._client = TelegramClient(
                str(self.settings.session_dir / "login_bot"),
                self.settings.api_id,
                self.settings.api_hash,
                device_model="Media Processor Login Bot",
                system_version="PART-1",
                app_version="0.1.0",
            )

        return self._client

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

    async def start(self) -> None:
        """Start the login bot."""
        if not self.settings.bot_token:
            logger.info(
                "BOT_TOKEN not configured; login bot disabled"
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
        if self._client is not None:
            await self._client.disconnect()

        self._running = False

        logger.info(
            "Telegram login bot stopped"
        )

    def _register_handlers(self) -> None:
        """Register login-bot handlers exactly once."""
        if self._handlers_registered:
            return

        self._handlers_registered = True

        @self.client.on(
            events.NewMessage(incoming=True)
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
        ).strip().lower()

        if text == "/start":
            await event.reply(
                "🤖 Media Processor Login Bot\n\n"
                "Use /login to authorize the userbot via QR.\n"
                "Use /status to check login status."
            )
            return

        if text == "/status":
            await self._handle_status(event)
            return

        if text == "/login":
            await self._run_qr_login(event)
            return

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

    async def _send_qr(
        self,
        event: events.NewMessage.Event,
        qr_url: str,
    ) -> None:
        """Generate and send QR image."""
        qr = qrcode.make(qr_url)

        buffer = io.BytesIO()

        qr.save(
            buffer,
            format="PNG",
        )

        buffer.seek(0)
        buffer.name = "telegram_login_qr.png"

        await self.client.send_file(
            event.chat_id,
            buffer,
            force_document=False,
            caption=(
                "📱 Is QR ko scan karo.\n\n"
                "Telegram → Settings → Devices → "
                "Link Desktop Device"
            ),
        )

    async def _finish_login(
        self,
        event: events.NewMessage.Event,
    ) -> bool:
        """Verify, finalize, and persist authenticated userbot."""
        client = self.userbot.client

        me = await client.get_me()

        if me is None:
            await event.reply(
                "❌ Login failed: Telegram account "
                "information unavailable."
            )
            return False

        expected_owner = self.settings.bot_owner_id

        if (
            expected_owner is not None
            and int(me.id) != int(expected_owner)
        ):
            logger.warning(
                "QR login completed by unexpected account: %s",
                me.id,
            )

            try:
                await client.log_out()

            except Exception:
                logger.exception(
                    "Failed to log out unexpected QR account"
                )

            await event.reply(
                "❌ QR was scanned by a different "
                "Telegram account.\n\n"
                "That session has been logged out."
            )

            return False

        # Finalize userbot first so owner information and
        # message handlers are registered.
        self.userbot.finish_authenticated_account(
            me
        )

        # IMPORTANT:
        # Save the authenticated Telegram session immediately.
        # This prevents QR login from being required again
        # after a Koyeb restart/redeploy.
        if self.userbot.session_store is not None:
            try:
                await self.userbot.persist_session()

                logger.info(
                    "Telegram session saved to MongoDB "
                    "after successful QR login"
                )

            except Exception:
                logger.exception(
                    "Telegram authentication succeeded, "
                    "but MongoDB session persistence failed"
                )

                await event.reply(
                    "⚠️ Telegram login successful, "
                    "but session could not be saved to MongoDB.\n\n"
                    "Do NOT redeploy/restart yet. "
                    "Check MongoDB configuration first."
                )

                return False

        self._authorized_event.set()

        logger.info(
            "Userbot authenticated successfully "
            "(owner_id=%s)",
            me.id,
        )

        await event.reply(
            "✅ Telegram userbot login successful!\n\n"
            f"Account ID: {me.id}\n"
            f"Username: "
            f"@{getattr(me, 'username', None) or 'none'}\n\n"
            "Userbot is now ready."
        )

        return True

    async def _perform_qr_login(
        self,
        event: events.NewMessage.Event,
    ) -> None:
        """Perform QR authentication with 2FA support."""
        client = self.userbot.client

        try:
            if not client.is_connected():
                await client.connect()

            # If an existing MongoDB/local session is already
            # authorized, no QR login is necessary.
            if await client.is_user_authorized():
                await self._finish_login(event)
                return

            password = os.getenv(
                "TELEGRAM_2FA_PASSWORD"
            )

            if not password:
                await event.reply(
                    "🔒 Telegram 2FA password is required.\n\n"
                    "Koyeb Environment Variables me "
                    "`TELEGRAM_2FA_PASSWORD` add karo, "
                    "phir /login dobara bhejo."
                )

                logger.warning(
                    "TELEGRAM_2FA_PASSWORD is not configured"
                )

                return

            await event.reply(
                "🔐 QR login starting...\n\n"
                "Telegram app me:\n"
                "Settings → Devices → "
                "Link Desktop Device\n\n"
                "Neeche aane wala QR scan karo."
            )

            while True:
                qr_login = await client.qr_login()

                await self._send_qr(
                    event,
                    qr_login.url,
                )

                logger.info(
                    "QR login code sent to owner"
                )

                try:
                    await qr_login.wait()

                except asyncio.TimeoutError:
                    logger.info(
                        "QR expired; generating a new QR"
                    )

                    await event.reply(
                        "⏳ QR expire ho gaya. "
                        "Naya QR bhej raha hoon..."
                    )

                    continue

                except SessionPasswordNeededError:
                    logger.info(
                        "QR scan successful; Telegram "
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
                            "in Koyeb and try /login again."
                        )

                        return

                break

            await self._finish_login(event)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            logger.exception(
                "QR login failed"
            )

            await event.reply(
                "❌ QR login failed.\n\n"
                f"Reason: {type(exc).__name__}"
            )

    async def wait_until_authorized(self) -> None:
        """Wait until QR login successfully authorizes the userbot."""
        await self._authorized_event.wait()
