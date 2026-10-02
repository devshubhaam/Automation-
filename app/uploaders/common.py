"""Shared constants, errors and validation for Part 2 image uploaders.

Part 2 handles *images only*. Videos are reserved for Part 3 and must never be
sent to ImgBB or Telegraph, so every uploader validates its input through
:func:`validate_image_file` before touching the network.
"""
from __future__ import annotations

import mimetypes
from pathlib import Path

#: Single source of truth for the Part 2 image size limit (2 MiB).
PART2_IMAGE_MAX_BYTES = 2 * 1024 * 1024


class UploadError(RuntimeError):
    """Raised when an image upload cannot be completed (permanent failure)."""


def redact(text: str, *secrets: str | None) -> str:
    """Remove every non-empty secret from ``text``."""
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***redacted***")
    return text


def validate_image_file(path: Path, *, provider: str, max_bytes: int) -> int:
    """Validate that ``path`` is an existing image within ``max_bytes``.

    Returns the file size in bytes. Raises :class:`UploadError` for missing
    files, non-image files (including every video) and oversized images.
    """
    path = Path(path)
    if not path.is_file():
        raise UploadError(f"Image file does not exist: {path.name}")
    content_type = mimetypes.guess_type(path.name)[0] or ""
    if not content_type.startswith("image/"):
        raise UploadError(f"{provider} Part 2 accepts images only: {path.name}")
    size = path.stat().st_size
    if size > max_bytes:
        raise UploadError(
            f"Image exceeds {provider} Part 2 limit ({size} bytes > {max_bytes} bytes)"
        )
    return size
