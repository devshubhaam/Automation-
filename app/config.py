"""Centralised configuration.

All environment access lives here. The rest of the application only ever sees a
frozen Settings object, so os.getenv() is never scattered around.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

from dotenv import load_dotenv

__all__ = ["ConfigError", "Settings", "VALID_LOG_LEVELS", "parse_video_bots"]

DEFAULT_ENV_FILE = ".env"

VALID_LOG_LEVELS = (
    "DEBUG",
    "INFO",
    "WARNING",
    "ERROR",
    "CRITICAL",
)


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


# --------------------------------------------------------------------------- #
# Typed environment readers
# --------------------------------------------------------------------------- #

def _raw(name: str) -> Optional[str]:
    value = os.getenv(name)

    if value is None:
        return None

    value = value.strip()

    return value or None


def _get_str(
    name: str,
    *,
    default: Optional[str] = None,
    required: bool = False,
) -> Optional[str]:
    value = _raw(name)

    if value is None:
        if required:
            raise ConfigError(
                f"Missing required environment variable: {name}"
            )

        return default

    return value


def _get_int(
    name: str,
    *,
    default: Optional[int] = None,
    required: bool = False,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> Optional[int]:
    value = _raw(name)

    if value is None:
        if required:
            raise ConfigError(
                f"Missing required environment variable: {name}"
            )

        result = default

    else:
        try:
            result = int(value)

        except ValueError as exc:
            raise ConfigError(
                f"{name} must be an integer (got {value!r})"
            ) from exc

    if result is None:
        return None

    if minimum is not None and result < minimum:
        raise ConfigError(
            f"{name} must be >= {minimum} (got {result})"
        )

    if maximum is not None and result > maximum:
        raise ConfigError(
            f"{name} must be <= {maximum} (got {result})"
        )

    return result


def _get_float(
    name: str,
    *,
    default: Optional[float] = None,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> Optional[float]:
    value = _raw(name)

    if value is None:
        result = default

    else:
        try:
            result = float(value)

        except ValueError as exc:
            raise ConfigError(
                f"{name} must be a number (got {value!r})"
            ) from exc

    if result is None:
        return None

    if minimum is not None and result < minimum:
        raise ConfigError(
            f"{name} must be >= {minimum} (got {result})"
        )

    if maximum is not None and result > maximum:
        raise ConfigError(
            f"{name} must be <= {maximum} (got {result})"
        )

    return result


def _get_bool(
    name: str,
    *,
    default: bool = False,
) -> bool:
    value = _raw(name)

    if value is None:
        return default

    return value.lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def _get_path(
    name: str,
    *,
    default: str,
) -> Path:
    return Path(
        _get_str(
            name,
            default=default,
        )
        or default
    ).expanduser()


def _normalise_bot_username(value: str) -> Optional[str]:
    """``@Bot`` / ``Bot`` / ``t.me/Bot`` / ``https://t.me/Bot`` -> ``Bot``."""

    value = value.strip()
    value = re.sub(r"^(https?://)?(t\.me/)", "", value, flags=re.IGNORECASE)
    value = value.lstrip("@").strip("/").strip()

    return value or None


def parse_video_bots(value: Optional[str]) -> tuple[str, ...]:
    """Parse ``VIDEO_BOTS``.

    ``"@FirstBot, SecondBot"`` -> ``("FirstBot", "SecondBot")``.

    Splits on commas, trims whitespace, ignores empty values, accepts names
    with or without ``@`` and removes duplicates (case-insensitive) while
    keeping the configured order.
    """

    if not value:
        return ()

    bots: list[str] = []
    seen: set[str] = set()

    for part in value.split(","):
        name = _normalise_bot_username(part)

        if not name or name.lower() in seen:
            continue

        seen.add(name.lower())
        bots.append(name)

    return tuple(bots)


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Settings:
    """Immutable application settings."""

    # ----------------------------------------------------------------------- #
    # Telegram API
    # ----------------------------------------------------------------------- #

    api_id: int
    api_hash: str

    # ----------------------------------------------------------------------- #
    # Telegram userbot session
    # ----------------------------------------------------------------------- #

    session_name: str = "media_processor"
    session_dir: Path = Path("./data/sessions")

    # ----------------------------------------------------------------------- #
    # Runtime directories
    # ----------------------------------------------------------------------- #

    download_dir: Path = Path("./data/downloads")
    job_dir: Path = Path("./data/jobs")
    log_dir: Path = Path("./data/logs")

    # ----------------------------------------------------------------------- #
    # Archive limits
    # ----------------------------------------------------------------------- #

    max_archive_size_mb: int = 500
    max_extracted_size_mb: int = 2000
    max_files_per_archive: int = 10000

    # ----------------------------------------------------------------------- #
    # Logging
    # ----------------------------------------------------------------------- #

    log_level: str = "INFO"
    log_max_bytes: int = 5 * 1024 * 1024
    log_backup_count: int = 5

    # ----------------------------------------------------------------------- #
    # Job files
    # ----------------------------------------------------------------------- #

    keep_job_files: bool = False

    # ----------------------------------------------------------------------- #
    # Telegram account
    # ----------------------------------------------------------------------- #

    owner_id: Optional[int] = None
    phone: Optional[str] = None

    # ----------------------------------------------------------------------- #
    # Telegram login bot
    # ----------------------------------------------------------------------- #

    bot_token: Optional[str] = None
    bot_owner_id: Optional[int] = None

    # ----------------------------------------------------------------------- #
    # Pipeline
    # ----------------------------------------------------------------------- #

    worker_count: int = 1

    # ----------------------------------------------------------------------- #
    # MongoDB persistent Telegram session
    # ----------------------------------------------------------------------- #

    mongodb_uri: Optional[str] = None
    mongodb_database: str = "telegram_media_processor"
    mongodb_collection: str = "sessions"

    # ----------------------------------------------------------------------- #
    # Part 2 - Image hosting
    # ----------------------------------------------------------------------- #

    imgbb_api_key: Optional[str] = None

    # Telegraph is used ONLY to publish articles that embed ImgBB IMAGE links.
    # Video links are never sent to Telegraph.
    # Optional: if unset, an anonymous account is created on first use.
    telegraph_access_token: Optional[str] = None
    telegraph_author_name: Optional[str] = None

    # Part 3 - video uploader bots. Videos are sent to these Telegram bots
    # (in order) with the userbot session; the bot replies with a link.
    # ``video_bots`` holds normalised usernames (no "@"); it comes from
    # VIDEO_BOTS (preferred) or, for backwards compatibility, VIDEO_BOT_USERNAME.
    video_bots: tuple[str, ...] = ()
    # Deprecated single-bot setting (raw VIDEO_BOT_USERNAME value).
    video_bot_username: Optional[str] = None
    # "all": EVERY video is uploaded to EVERY configured bot (one link per
    # bot, e.g. a DiskWala link AND a Flezen link). "fallback": the bots are
    # tried in order and the first link wins.
    video_bot_mode: str = "all"
    # (fallback mode only) Try the next bot after a provider TIMEOUT. Off by default because the
    # first bot may still be processing the video (duplicate upload risk).
    video_bot_fallback_on_timeout: bool = False
    # Accept only bot messages that are Telegram replies to the sent video.
    video_bot_require_reply: bool = True
    # Per-video wait for the bot's link AFTER the file was delivered to it.
    video_bot_timeout_seconds: int = 1800
    # Attempts to *deliver* one video to the bot. A video is never re-sent
    # once the bot has received it (timeouts are not retried).
    video_bot_send_attempts: int = 2
    video_url_pattern: Optional[str] = None
    # Safety limit for EACH individual video (not for the ZIP archive).
    video_max_size_gb: float = 1.5

    # Optional custom final Telegram post (FINAL_POST_TEMPLATE). Empty/None
    # keeps the built-in clean final post. May contain escaped ``\\n``.
    final_post_template: Optional[str] = None

    # ----------------------------------------------------------------------- #
    # Derived values
    # ----------------------------------------------------------------------- #

    @property
    def max_archive_size_bytes(self) -> int:
        return self.max_archive_size_mb * 1024 * 1024

    @property
    def max_extracted_size_bytes(self) -> int:
        return self.max_extracted_size_mb * 1024 * 1024

    @property
    def video_max_size_bytes(self) -> int:
        """Per-video size limit in bytes (default 1.5 GB = 1536 MiB)."""
        return int(self.video_max_size_gb * 1024 * 1024 * 1024)

    @property
    def session_path(self) -> Path:
        """Path of the local Telethon session."""
        return self.session_dir / self.session_name

    @property
    def log_file(self) -> Path:
        return self.log_dir / "app.log"

    # ----------------------------------------------------------------------- #
    # Validation
    # ----------------------------------------------------------------------- #

    def __post_init__(self) -> None:
        level = str(self.log_level).upper()

        if level not in VALID_LOG_LEVELS:
            raise ConfigError(
                f"LOG_LEVEL must be one of "
                f"{', '.join(VALID_LOG_LEVELS)} "
                f"(got {self.log_level!r})"
            )

        object.__setattr__(
            self,
            "log_level",
            level,
        )

        # Normalise the video bots and keep the legacy single-bot setting
        # working: VIDEO_BOTS wins, VIDEO_BOT_USERNAME is the fallback.
        bots = parse_video_bots(",".join(self.video_bots)) if self.video_bots else ()

        if not bots and self.video_bot_username:
            bots = parse_video_bots(self.video_bot_username)

        object.__setattr__(self, "video_bots", bots)

        mode = str(self.video_bot_mode or "all").strip().lower()
        if mode not in ("all", "fallback"):
            raise ConfigError(
                f"VIDEO_BOT_MODE must be 'all' or 'fallback' (got {self.video_bot_mode!r})"
            )
        object.__setattr__(self, "video_bot_mode", mode)

        if float(self.video_max_size_gb) <= 0:
            raise ConfigError(
                f"VIDEO_MAX_SIZE_GB must be > 0 (got {self.video_max_size_gb})"
            )

    def __repr__(self) -> str:
        """Never leak secrets through repr/logging."""

        return (
            f"Settings("
            f"api_id={self.api_id}, "
            f"api_hash='***redacted***', "
            f"session_name={self.session_name!r}, "
            f"session_dir={str(self.session_dir)!r}, "
            f"job_dir={str(self.job_dir)!r}, "
            f"download_dir={str(self.download_dir)!r}, "
            f"log_dir={str(self.log_dir)!r}, "
            f"max_archive_size_mb={self.max_archive_size_mb}, "
            f"max_extracted_size_mb={self.max_extracted_size_mb}, "
            f"max_files_per_archive={self.max_files_per_archive}, "
            f"log_level={self.log_level!r}, "
            f"keep_job_files={self.keep_job_files}, "
            f"owner_id={self.owner_id}, "
            f"bot_owner_id={self.bot_owner_id}, "
            f"worker_count={self.worker_count}, "
            f"mongodb_database={self.mongodb_database!r}, "
            f"mongodb_collection={self.mongodb_collection!r}, "
            f"imgbb_api_key='***redacted***', "
            f"telegraph_access_token='***redacted***', "
            f"video_bots={list(self.video_bots)!r}, "
            f"video_bot_mode={self.video_bot_mode!r}, "
            f"video_bot_timeout_seconds={self.video_bot_timeout_seconds}, "
            f"video_max_size_gb={self.video_max_size_gb}"
            f")"
        )

    # ----------------------------------------------------------------------- #
    # Directories
    # ----------------------------------------------------------------------- #

    def ensure_directories(self) -> None:
        """Create every runtime directory this application needs."""

        for directory in (
            self.session_dir,
            self.download_dir,
            self.job_dir,
            self.log_dir,
        ):
            Path(directory).mkdir(
                parents=True,
                exist_ok=True,
            )

    # ----------------------------------------------------------------------- #
    # Environment loading
    # ----------------------------------------------------------------------- #

    @classmethod
    def from_env(
        cls,
        env_file: Optional[Union[str, os.PathLike]] = DEFAULT_ENV_FILE,
    ) -> "Settings":
        """Load .env if present and build a validated Settings object."""

        if env_file is not None and Path(env_file).is_file():
            load_dotenv(
                env_file,
                override=False,
            )

        # ------------------------------------------------------------------- #
        # Telegram API
        # ------------------------------------------------------------------- #

        api_id = _get_int(
            "API_ID",
            required=True,
            minimum=1,
        )

        api_hash = _get_str(
            "API_HASH",
            required=True,
        )

        assert api_id is not None
        assert api_hash is not None

        if len(api_hash) < 8:
            raise ConfigError(
                "API_HASH looks invalid "
                "(shorter than 8 characters)"
            )

        # ------------------------------------------------------------------- #
        # Settings object
        # ------------------------------------------------------------------- #

        return cls(
            # Telegram API
            api_id=api_id,
            api_hash=api_hash,

            # Userbot session
            session_name=_get_str(
                "SESSION_NAME",
                default="media_processor",
            ) or "media_processor",

            session_dir=_get_path(
                "SESSION_DIR",
                default="./data/sessions",
            ),

            # Runtime directories
            download_dir=_get_path(
                "DOWNLOAD_DIR",
                default="./data/downloads",
            ),

            job_dir=_get_path(
                "JOB_DIR",
                default="./data/jobs",
            ),

            log_dir=_get_path(
                "LOG_DIR",
                default="./data/logs",
            ),

            # Archive limits
            max_archive_size_mb=_get_int(
                "MAX_ARCHIVE_SIZE_MB",
                default=500,
                minimum=1,
            ) or 500,

            max_extracted_size_mb=_get_int(
                "MAX_EXTRACTED_SIZE_MB",
                default=2000,
                minimum=1,
            ) or 2000,

            max_files_per_archive=_get_int(
                "MAX_FILES_PER_ARCHIVE",
                default=10000,
                minimum=1,
            ) or 10000,

            # Logging
            log_level=_get_str(
                "LOG_LEVEL",
                default="INFO",
            ) or "INFO",

            log_max_bytes=_get_int(
                "LOG_MAX_BYTES",
                default=5 * 1024 * 1024,
                minimum=1024,
            ) or 5 * 1024 * 1024,

            log_backup_count=_get_int(
                "LOG_BACKUP_COUNT",
                default=5,
                minimum=1,
            ) or 5,

            # Job files
            keep_job_files=_get_bool(
                "KEEP_JOB_FILES",
                default=False,
            ),

            # Telegram account
            owner_id=_get_int(
                "OWNER_ID",
                default=None,
            ),

            phone=_get_str(
                "TELEGRAM_PHONE",
                default=None,
            ),

            # Telegram login bot
            bot_token=_get_str(
                "BOT_TOKEN",
                default=None,
            ),

            bot_owner_id=_get_int(
                "BOT_OWNER_ID",
                default=None,
            ),

            # Pipeline
            worker_count=_get_int(
                "WORKER_COUNT",
                default=1,
                minimum=1,
                maximum=4,
            ) or 1,

            # MongoDB
            mongodb_uri=_get_str(
                "MONGODB_URI",
                default=None,
            ),

            mongodb_database=_get_str(
                "MONGODB_DATABASE",
                default="telegram_media_processor",
            ) or "telegram_media_processor",

            mongodb_collection=_get_str(
                "MONGODB_COLLECTION",
                default="sessions",
            ) or "sessions",

            # Part 2 - ImgBB
            #
            # Optional here so the application can still start without
            # ImgBB configured. Uploading an image will report a clear
            # upload error instead of failing application startup.
            imgbb_api_key=_get_str(
                "IMGBB_API_KEY",
                default=None,
            ),

            # Part 2 - Telegraph article (embeds ImgBB image links only)
            telegraph_access_token=_get_str(
                "TELEGRAPH_ACCESS_TOKEN",
                default=None,
            ),
            telegraph_author_name=_get_str(
                "TELEGRAPH_AUTHOR_NAME",
                default=None,
            ),

            # Part 3 - video bots, e.g.
            #   VIDEO_BOTS=@FirstUploaderBot,@SecondUploaderBot
            # VIDEO_BOT_USERNAME is still read as a fallback (deprecated).
            video_bots=parse_video_bots(
                _get_str("VIDEO_BOTS", default=None)
                or _get_str("VIDEO_BOT_USERNAME", default=None)
            ),
            video_bot_username=_get_str(
                "VIDEO_BOT_USERNAME",
                default=None,
            ),
            video_bot_mode=(
                _get_str("VIDEO_BOT_MODE", default="all") or "all"
            ).strip().lower(),
            video_bot_fallback_on_timeout=_get_bool(
                "VIDEO_BOT_FALLBACK_ON_TIMEOUT",
                default=False,
            ),
            video_bot_require_reply=_get_bool(
                "VIDEO_BOT_REQUIRE_REPLY",
                default=True,
            ),
            video_bot_timeout_seconds=_get_int(
                "VIDEO_BOT_TIMEOUT_SECONDS",
                default=1800,
                minimum=10,
                maximum=7200,
            ) or 1800,
            video_bot_send_attempts=_get_int(
                "VIDEO_BOT_SEND_ATTEMPTS",
                default=2,
                minimum=1,
                maximum=5,
            ) or 2,
            video_max_size_gb=_get_float(
                "VIDEO_MAX_SIZE_GB",
                default=1.5,
                minimum=0.01,
                maximum=4.0,
            ) or 1.5,
            video_url_pattern=_get_str(
                "VIDEO_URL_PATTERN",
                default=None,
            ),
            final_post_template=_get_str(
                "FINAL_POST_TEMPLATE",
                default=None,
            ),
        )
