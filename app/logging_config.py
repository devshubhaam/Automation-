"""Logging setup: console + rotating file handler.

Secrets (API_HASH, session contents, .env values, login codes, 2FA passwords) are
never logged anywhere in this application.
"""

from __future__ import annotations

import logging
import logging.handlers
from typing import Optional

from .config import Settings

__all__ = ["setup_logging", "get_logger"]

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
ROOT_LOGGER_NAME = "app"


def setup_logging(settings: Settings) -> logging.Logger:
    """Configure and return the application logger."""
    settings.log_dir.mkdir(parents=True, exist_ok=True)
    level = getattr(logging, settings.log_level.upper(), logging.INFO)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    file_handler = logging.handlers.RotatingFileHandler(
        filename=settings.log_file,
        maxBytes=settings.log_max_bytes,
        backupCount=settings.log_backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        try:
            handler.close()
        except Exception:  # pragma: no cover - defensive only
            pass
    root.addHandler(file_handler)
    root.addHandler(console_handler)
    root.setLevel(level)

    # Telethon is chatty at INFO; keep it at WARNING or above.
    logging.getLogger("telethon").setLevel(max(level, logging.WARNING))

    logger = logging.getLogger(ROOT_LOGGER_NAME)
    logger.debug("Logging configured (level=%s, file=%s)", settings.log_level, settings.log_file)
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a namespaced logger under the ``app`` root logger."""
    if not name or name == ROOT_LOGGER_NAME:
        return logging.getLogger(ROOT_LOGGER_NAME)
    if name.startswith(f"{ROOT_LOGGER_NAME}."):
        return logging.getLogger(name)
    return logging.getLogger(f"{ROOT_LOGGER_NAME}.{name}")
