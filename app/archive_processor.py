"""Safe ZIP validation and extraction.

Security properties enforced here:

* archive size limit (``MAX_ARCHIVE_SIZE_MB``)
* file count limit (``MAX_FILES_PER_ARCHIVE``)
* total *actual* uncompressed byte limit (``MAX_EXTRACTED_SIZE_MB``) - checked
  against bytes really read from the stream, not only the declared header size
* path traversal / absolute path / drive-letter rejection
* symlink and special-file rejection
* extraction is streamed in chunks (the archive is never fully loaded into RAM)
* every write is confined to the job's own ``extracted/`` directory
"""

from __future__ import annotations

import logging
import stat
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional

from .config import Settings
from .utils import (
    UnsafePathError,
    format_bytes,
    is_within_directory,
    resolve_archive_member,
)

__all__ = [
    "ArchiveError",
    "InvalidArchiveError",
    "UnsafeArchiveError",
    "ArchiveTooLargeError",
    "TooManyFilesError",
    "ExtractionCancelledError",
    "ArchiveInfo",
    "ExtractionResult",
    "validate_archive",
    "safe_extract",
    "iter_member_names",
]

logger = logging.getLogger("app.archive_processor")

CHUNK_SIZE = 1024 * 1024  # 1 MiB


class ArchiveError(Exception):
    """Base class for every archive problem (safe to show to the user)."""

    stage = "ZIP processing"


class InvalidArchiveError(ArchiveError):
    stage = "ZIP validation"


class UnsafeArchiveError(ArchiveError):
    stage = "ZIP extraction"


class ArchiveTooLargeError(ArchiveError):
    stage = "ZIP extraction"


class TooManyFilesError(ArchiveError):
    stage = "ZIP validation"


class ExtractionCancelledError(ArchiveError):
    stage = "ZIP extraction"


@dataclass
class ArchiveInfo:
    """Result of a pre-extraction validation pass."""

    path: Path
    archive_size_bytes: int
    member_count: int
    file_count: int
    total_uncompressed_bytes: int
    compression_ratio: float
    rejected_members: List[str] = field(default_factory=list)


@dataclass
class ExtractionResult:
    """Result of a successful extraction."""

    extract_root: Path
    extracted_files: int
    extracted_directories: int
    skipped_members: List[str] = field(default_factory=list)
    total_bytes: int = 0


def _member_mode(member: zipfile.ZipInfo) -> int:
    """Return the POSIX mode bits stored in the central directory (0 if absent)."""
    return member.external_attr >> 16


def _is_symlink_member(member: zipfile.ZipInfo) -> bool:
    mode = _member_mode(member)
    return bool(mode) and stat.S_ISLNK(mode)


def _is_regular_or_unknown(member: zipfile.ZipInfo) -> bool:
    """True for regular files and for members with no *file type* information.

    ZIP writers frequently store only permission bits (e.g. Python's own
    ``writestr`` writes ``0o600 << 16``), leaving ``S_IFMT`` empty. Those members
    are ordinary files and must be accepted; only entries that explicitly declare
    a non-regular type (fifo, socket, device, symlink) are rejected.
    """
    mode = _member_mode(member)
    if mode == 0:
        return True
    filetype = stat.S_IFMT(mode)
    if filetype == 0:
        return True
    return stat.S_ISREG(mode)


def iter_member_names(archive_path: Path) -> List[str]:
    """Return every member name in the archive (names only, nothing extracted)."""
    with zipfile.ZipFile(archive_path) as archive:
        return [info.filename for info in archive.infolist()]


def validate_archive(archive_path: Path, settings: Settings) -> ArchiveInfo:
    """Validate a ZIP before any byte is written to disk.

    Raises one of the :class:`ArchiveError` subclasses on rejection.
    """
    archive_path = Path(archive_path)
    if not archive_path.exists():
        raise InvalidArchiveError(f"archive not found: {archive_path.name}")
    if not archive_path.is_file():
        raise InvalidArchiveError("archive is not a regular file")

    archive_size = archive_path.stat().st_size
    if archive_size > settings.max_archive_size_bytes:
        raise ArchiveTooLargeError(
            f"ZIP file is too large ({format_bytes(archive_size)}). "
            f"Maximum allowed size: {settings.max_archive_size_mb} MB"
        )

    if not zipfile.is_zipfile(archive_path):
        raise InvalidArchiveError("file is not a valid ZIP archive")

    rejected: List[str] = []
    file_count = 0
    total_uncompressed = 0

    try:
        with zipfile.ZipFile(archive_path) as archive:
            infos = archive.infolist()
            if not infos:
                return ArchiveInfo(
                    path=archive_path,
                    archive_size_bytes=archive_size,
                    member_count=0,
                    file_count=0,
                    total_uncompressed_bytes=0,
                    compression_ratio=0.0,
                )

            for info in infos:
                if info.is_dir():
                    continue
                file_count += 1
                total_uncompressed += max(0, int(info.file_size or 0))

                if _is_symlink_member(info):
                    rejected.append(f"{info.filename} (symlink)")
                    continue
                if not _is_regular_or_unknown(info):
                    rejected.append(f"{info.filename} (special file)")
                    continue
                try:
                    resolve_archive_member(Path("."), info.filename)
                except UnsafePathError:
                    rejected.append(info.filename)
    except zipfile.BadZipFile as exc:
        raise InvalidArchiveError(f"corrupted ZIP archive: {exc}") from exc

    if rejected:
        preview = ", ".join(rejected[:3])
        raise UnsafeArchiveError(
            f"archive contains unsafe member(s): {preview}"
            + (" ..." if len(rejected) > 3 else "")
        )

    if file_count > settings.max_files_per_archive:
        raise TooManyFilesError(
            f"archive contains too many files ({file_count}). "
            f"Maximum allowed: {settings.max_files_per_archive}"
        )

    if total_uncompressed > settings.max_extracted_size_bytes:
        raise ArchiveTooLargeError(
            f"archive expands to {format_bytes(total_uncompressed)} "
            f"(maximum extracted size: {settings.max_extracted_size_mb} MB)"
        )

    ratio = (total_uncompressed / archive_size) if archive_size else 0.0
    return ArchiveInfo(
        path=archive_path,
        archive_size_bytes=archive_size,
        member_count=len(infos),
        file_count=file_count,
        total_uncompressed_bytes=total_uncompressed,
        compression_ratio=ratio,
    )


def _safe_remove(path: Path, extract_root: Path) -> None:
    """Remove a partially written file, but only inside the extraction root."""
    try:
        if is_within_directory(extract_root, path) and path.is_file():
            path.unlink()
    except OSError:  # pragma: no cover - defensive
        logger.warning("Could not remove partial file %s", path.name)


def safe_extract(
    archive_path: Path,
    extract_root: Path,
    settings: Settings,
    *,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> ExtractionResult:
    """Extract ``archive_path`` into ``extract_root`` safely.

    The declared header sizes are never trusted on their own: the cumulative
    number of bytes actually written is enforced against
    ``settings.max_extracted_size_bytes``.
    """
    archive_path = Path(archive_path)
    extract_root = Path(extract_root)
    extract_root.mkdir(parents=True, exist_ok=True)
    root_resolved = extract_root.resolve()

    if not zipfile.is_zipfile(archive_path):
        raise InvalidArchiveError("file is not a valid ZIP archive")

    max_bytes = settings.max_extracted_size_bytes
    max_files = settings.max_files_per_archive

    extracted_files = 0
    extracted_directories = 0
    skipped: List[str] = []
    total_bytes = 0

    try:
        with zipfile.ZipFile(archive_path) as archive:
            for member in archive.infolist():
                if should_cancel is not None and should_cancel():
                    raise ExtractionCancelledError("extraction cancelled by user")

                try:
                    target = resolve_archive_member(root_resolved, member.filename)
                except UnsafePathError as exc:
                    raise UnsafeArchiveError(f"rejected unsafe archive member: {exc}") from exc

                if member.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                    extracted_directories += 1
                    continue

                if _is_symlink_member(member):
                    skipped.append(f"{member.filename} (symlink)")
                    continue
                if not _is_regular_or_unknown(member):
                    skipped.append(f"{member.filename} (special file)")
                    continue

                extracted_files += 1
                if extracted_files > max_files:
                    raise TooManyFilesError(
                        f"archive contains too many files. Maximum allowed: {max_files}"
                    )

                target.parent.mkdir(parents=True, exist_ok=True)
                try:
                    with archive.open(member) as source, open(target, "wb") as destination:
                        while True:
                            if should_cancel is not None and should_cancel():
                                destination.close()
                                _safe_remove(target, root_resolved)
                                raise ExtractionCancelledError("extraction cancelled by user")
                            chunk = source.read(CHUNK_SIZE)
                            if not chunk:
                                break
                            total_bytes += len(chunk)
                            if total_bytes > max_bytes:
                                destination.close()
                                _safe_remove(target, root_resolved)
                                raise ArchiveTooLargeError(
                                    "Archive extraction stopped.\n"
                                    f"Reason: Maximum extracted size exceeded "
                                    f"({settings.max_extracted_size_mb} MB)."
                                )
                            destination.write(chunk)
                except ArchiveError:
                    raise
                except OSError as exc:
                    _safe_remove(target, root_resolved)
                    raise ArchiveError(f"failed to write extracted file: {exc}") from exc
    except zipfile.BadZipFile as exc:
        raise InvalidArchiveError(f"corrupted ZIP archive: {exc}") from exc

    return ExtractionResult(
        extract_root=extract_root,
        extracted_files=extracted_files,
        extracted_directories=extracted_directories,
        skipped_members=skipped,
        total_bytes=total_bytes,
    )
