"""Dependency-free helpers shared across the application.

Everything in here is pure (no I/O, no Telegram, no globals) so it can be unit
tested without any credentials or network access.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, List, Union

__all__ = [
    "UnsafePathError",
    "natural_sort_key",
    "natural_sorted",
    "is_within_directory",
    "sanitize_member_name",
    "resolve_archive_member",
    "relative_posix_path",
    "format_bytes",
    "truncate",
]

_NUMBER_RE = re.compile(r"(\d+)")
_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:")


class UnsafePathError(ValueError):
    """Raised when an archive member would escape the extraction root."""


# --------------------------------------------------------------------------- #
# Natural sorting
# --------------------------------------------------------------------------- #
def natural_sort_key(value: Union[str, Path]) -> tuple:
    """Return a sort key implementing human/natural ordering.

    ``image1.jpg`` -> ``image2.jpg`` -> ``image10.jpg``
    """
    text = str(value)
    key: List[tuple] = []
    for part in _NUMBER_RE.split(text):
        if not part:
            continue
        if part.isdigit():
            key.append((1, int(part), ""))
        else:
            key.append((0, 0, part.lower()))
    return tuple(key)


def natural_sorted(values: Iterable[Union[str, Path]]) -> List:
    """Return ``values`` sorted naturally (case-insensitive)."""
    return sorted(values, key=natural_sort_key)


# --------------------------------------------------------------------------- #
# Path safety
# --------------------------------------------------------------------------- #
def is_within_directory(directory: Union[str, Path], target: Union[str, Path]) -> bool:
    """True when ``target`` resolves to ``directory`` or something inside it."""
    directory_path = Path(directory).resolve()
    target_path = Path(target).resolve()
    if target_path == directory_path:
        return True
    return directory_path in target_path.parents


def sanitize_member_name(member_name: str) -> str:
    """Normalise a ZIP member name and reject anything unsafe.

    Rejects:
      * absolute POSIX paths (``/etc/passwd``, ``//host/share``)
      * Windows drive-qualified paths (``C:\\evil``)
      * parent traversal (``../../evil``)
      * empty / dot-only names
    """
    if member_name is None:
        raise UnsafePathError("member has no name")
    normalized = str(member_name).replace("\\", "/").strip()
    if not normalized:
        raise UnsafePathError("empty member name")
    if normalized.startswith("/"):
        raise UnsafePathError(f"absolute member path: {member_name!r}")
    if _WINDOWS_DRIVE_RE.match(normalized):
        raise UnsafePathError(f"drive-qualified member path: {member_name!r}")
    # UNC-style / protocol-relative
    if normalized.startswith("//"):
        raise UnsafePathError(f"absolute member path: {member_name!r}")

    parts = [part for part in normalized.split("/") if part not in ("", ".")]
    if not parts:
        raise UnsafePathError(f"empty member path: {member_name!r}")
    if any(part == ".." for part in parts):
        raise UnsafePathError(f"parent traversal in member path: {member_name!r}")
    if any("\x00" in part for part in parts):
        raise UnsafePathError(f"NUL byte in member path: {member_name!r}")
    return "/".join(parts)


def resolve_archive_member(extract_root: Union[str, Path], member_name: str) -> Path:
    """Resolve an archive member to an absolute path guaranteed to stay inside root."""
    safe_name = sanitize_member_name(member_name)
    root = Path(extract_root).resolve()
    target = (root / safe_name).resolve()
    if not is_within_directory(root, target):
        raise UnsafePathError(f"member escapes extraction root: {member_name!r}")
    return target


def relative_posix_path(path: Union[str, Path], root: Union[str, Path]) -> str:
    """Return a POSIX-style path relative to ``root`` (never an absolute path)."""
    path_obj = Path(path)
    try:
        return path_obj.resolve().relative_to(Path(root).resolve()).as_posix()
    except (ValueError, OSError):
        return path_obj.name


# --------------------------------------------------------------------------- #
# Formatting
# --------------------------------------------------------------------------- #
def format_bytes(num_bytes: Union[int, float, None]) -> str:
    """Human readable byte size, e.g. ``1.5 MB``."""
    if num_bytes is None:
        return "unknown"
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size) < 1024.0 or unit == "TB":
            if unit == "B":
                return f"{int(size)} B"
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"


def truncate(text: str, limit: int = 200) -> str:
    """Shorten ``text`` to ``limit`` characters with an ellipsis."""
    text = str(text)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "…"
