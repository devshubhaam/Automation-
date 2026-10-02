"""Centralised configuration.

All environment access lives here. The rest of the application only ever sees a
frozen Settings object, so os.getenv() is never scattered around.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

from dotenv import load_dotenv

__all__ = ["ConfigError", "Settings", "VALID_LOG_LEVELS"]

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


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Settings:
    """Immutable application settings."""

    # Telegram API
    api_id: int
    api_hash: str

    # Telegram userbot session
    session_name: str = "media_processor"
    session_dir: Path = Path("./data/sessions")

    # Runtime directories
    download_dir: Path = Path("./data/downloads")
    job_dir: Path = Path("./data/jobs")
    log_dir: Path = Path("./data/logs")

    # Archive limits
    max_archive_size_mb: int = 500
    max_extracted_size_mb: int = 2000
    max_files_per_archive: int = 10000

    # Logging
    log_level: str = "INFO"
    log_max_bytes: int = 5 * 1024 * 1024
    log_backup_count: int = 5

    # Job files
    keep_job_files: bool = False

    # Telegram account
    owner_id: Optional[int] = None
    phone: Optional[str] = None

    # Telegram login bot
    bot_token: Optional[str] = None
    bot_owner_id: Optional[int] = None

    # Pipeline
    worker_count: int = 1

    # MongoDB persistent Telegram session
    mongodb_uri: Optional[str] = None
    mongodb_database: str = "telegram_media_processor"
    mongodb_collection: str = "sessions"

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
    def session_path(self) -> Path:
        """Path of the Telethon local session."""
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
            f"mongodb_collection={self.mongodb_collection!r}"
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
        )
