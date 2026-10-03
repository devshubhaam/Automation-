"""Recursive media scanner.

Walks an extracted job directory and classifies every regular file as
``image``, ``video`` or ``ignored``.

Detection is extension based for PART 1 (case-insensitive). The public API takes
an optional ``detector`` hook so stronger MIME/signature detection can be added
later without touching callers.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence

from .utils import natural_sort_key, relative_posix_path

__all__ = [
    "IMAGE_EXTENSIONS",
    "VIDEO_EXTENSIONS",
    "SUPPORTED_EXTENSIONS",
    "MediaFile",
    "MediaScanResult",
    "classify_extension",
    "scan_directory",
]

IMAGE_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"})
VIDEO_EXTENSIONS = frozenset({".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v"})
SUPPORTED_EXTENSIONS = IMAGE_EXTENSIONS | VIDEO_EXTENSIONS

IMAGE_TYPE = "image"
VIDEO_TYPE = "video"
IGNORED_TYPE = "ignored"

# Type alias for an optional future MIME/signature detector.
Detector = Callable[[Path], Optional[str]]


def classify_extension(filename: str) -> Optional[str]:
    """Return ``"image"``, ``"video"`` or ``None`` for an unsupported extension."""
    suffix = Path(filename).suffix.lower()
    if suffix in IMAGE_EXTENSIONS:
        return IMAGE_TYPE
    if suffix in VIDEO_EXTENSIONS:
        return VIDEO_TYPE
    return None


@dataclass(frozen=True)
class MediaFile:
    """A single detected media file."""

    path: Path
    relative_path: str
    filename: str
    media_type: str
    index: int
    size_bytes: int = 0

    def to_dict(self) -> Dict[str, object]:
        return {
            "filename": self.filename,
            "relative_path": self.relative_path,
            "absolute_path": str(self.path),
            "type": self.media_type,
            "index": self.index,
            "size_bytes": self.size_bytes,
        }


@dataclass
class MediaScanResult:
    """Result of scanning an extracted job directory."""

    images: List[MediaFile] = field(default_factory=list)
    videos: List[MediaFile] = field(default_factory=list)
    ignored: List[Path] = field(default_factory=list)
    scan_root: Optional[Path] = None

    @property
    def image_count(self) -> int:
        return len(self.images)

    @property
    def video_count(self) -> int:
        return len(self.videos)

    @property
    def ignored_count(self) -> int:
        return len(self.ignored)

    @property
    def total_files(self) -> int:
        return self.image_count + self.video_count + self.ignored_count

    def relative_ignored(self) -> List[str]:
        root = self.scan_root
        if root is None:
            return [Path(p).name for p in self.ignored]
        return [relative_posix_path(p, root) for p in self.ignored]

    def counts(self) -> Dict[str, int]:
        return {
            "total_files": self.total_files,
            "image_files": self.image_count,
            "video_files": self.video_count,
            "ignored_files": self.ignored_count,
        }

    def to_dict(self) -> Dict[str, object]:
        return {
            **self.counts(),
            "images": [f.to_dict() for f in self.images],
            "videos": [f.to_dict() for f in self.videos],
            "ignored": self.relative_ignored(),
        }

    def summary(self) -> str:
        return (
            f"Images: {self.image_count}\n"
            f"Videos: {self.video_count}\n"
            f"Ignored: {self.ignored_count}"
        )


def _iter_regular_files(root: Path):
    """Yield ``(path, is_symlink)`` for every entry under root.

    ``os.walk(followlinks=False)`` is used deliberately: it never descends into
    symlinked directories, so a crafted archive cannot make the scanner escape
    the job directory.
    """
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames[:] = sorted(
            [d for d in dirnames if not (Path(dirpath) / d).is_symlink()],
            key=natural_sort_key,
        )
        for name in sorted(filenames, key=natural_sort_key):
            candidate = Path(dirpath) / name
            if candidate.is_symlink():
                yield candidate, True
            elif candidate.is_file():
                yield candidate, False


def scan_directory(
    root: Path,
    *,
    scan_root: Optional[Path] = None,
    detector: Optional[Detector] = None,
) -> MediaScanResult:
    """Recursively classify every file under ``root``.

    ``scan_root`` controls the base used for ``relative_path`` (defaults to
    ``root``). ``detector`` is an optional hook that may override the
    extension-based classification.
    """
    root = Path(root)
    if not root.exists():
        raise FileNotFoundError(f"scan root does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"scan root is not a directory: {root}")
    scan_root = Path(scan_root) if scan_root is not None else root

    image_paths: List[Path] = []
    video_paths: List[Path] = []
    ignored: List[Path] = []

    for path, is_symlink in _iter_regular_files(root):
        if is_symlink:
            # Never trust symlinks inside an extracted archive.
            ignored.append(path)
            continue

        media_type = detector(path) if detector is not None else None
        if media_type not in (IMAGE_TYPE, VIDEO_TYPE):
            media_type = classify_extension(path.name)

        if media_type == IMAGE_TYPE:
            image_paths.append(path)
        elif media_type == VIDEO_TYPE:
            video_paths.append(path)
        else:
            ignored.append(path)

    image_paths.sort(key=natural_sort_key)
    video_paths.sort(key=natural_sort_key)
    ignored.sort(key=natural_sort_key)

    def build(paths: Sequence[Path], media_type: str) -> List[MediaFile]:
        built: List[MediaFile] = []
        for index, path in enumerate(paths, start=1):
            try:
                size = path.stat().st_size
            except OSError:  # pragma: no cover - defensive
                size = 0
            built.append(
                MediaFile(
                    path=path,
                    relative_path=relative_posix_path(path, scan_root),
                    filename=path.name,
                    media_type=media_type,
                    index=index,
                    size_bytes=size,
                )
            )
        return built

    return MediaScanResult(
        images=build(image_paths, IMAGE_TYPE),
        videos=build(video_paths, VIDEO_TYPE),
        ignored=ignored,
        scan_root=scan_root,
    )
