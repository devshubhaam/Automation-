"""Shared pytest fixtures and ZIP builders (no Telegram, no network)."""

from __future__ import annotations

import stat
import zipfile
from pathlib import Path

import pytest

from app.config import Settings


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    """A fully isolated Settings object pointing at tmp_path."""
    return Settings(
        api_id=12345,
        api_hash="0123456789abcdef0123456789abcdef",
        session_name="test_session",
        session_dir=tmp_path / "sessions",
        download_dir=tmp_path / "downloads",
        job_dir=tmp_path / "jobs",
        log_dir=tmp_path / "logs",
        max_archive_size_mb=1,
        max_extracted_size_mb=1,
        max_files_per_archive=100,
        log_level="DEBUG",
        keep_job_files=False,
    )


def build_zip(
    path: Path,
    entries: dict[str, bytes],
    *,
    compression: int = zipfile.ZIP_DEFLATED,
) -> Path:
    """Create a ZIP at ``path`` from ``{member_name: payload}``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=compression) as archive:
        for name, payload in entries.items():
            archive.writestr(name, payload)
    return path


def build_symlink_zip(path: Path, *, link_name: str = "evil_link") -> Path:
    """Create a ZIP containing a symlink member (external_attr S_IFLNK)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        info = zipfile.ZipInfo(link_name)
        info.create_system = 3  # UNIX
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(info, "/etc/passwd")
    return path


def make_media_tree(root: Path) -> Path:
    """Build the canonical fixture tree used by scanner tests.

    folder/
      image1.jpg, image2.png, image10.jpg, video1.mp4, video2.mkv, note.txt
    """
    folder = root / "folder"
    folder.mkdir(parents=True, exist_ok=True)
    for name in ("image1.jpg", "image2.png", "image10.jpg"):
        (folder / name).write_bytes(b"\xff\xd8\xff\xe0fake-image")
    for name in ("video1.mp4", "video2.mkv"):
        (folder / name).write_bytes(b"\x00\x00\x00\x18ftypmp42")
    (folder / "note.txt").write_text("hello", encoding="utf-8")
    return folder


def make_acceptance_tree(root: Path) -> Path:
    """Build the §52 acceptance tree: Sample/{Images,Videos,Other}."""
    sample = root / "Sample"
    (sample / "Images").mkdir(parents=True, exist_ok=True)
    (sample / "Videos").mkdir(parents=True, exist_ok=True)
    (sample / "Other").mkdir(parents=True, exist_ok=True)
    for name in ("1.jpg", "2.png", "10.webp"):
        (sample / "Images" / name).write_bytes(b"\xff\xd8\xff\xe0fake")
    for name in ("1.mp4", "2.mkv"):
        (sample / "Videos" / name).write_bytes(b"\x00\x00\x00\x18ftyp")
    (sample / "Other" / "readme.txt").write_text("hi", encoding="utf-8")
    (sample / "Other" / "document.pdf").write_bytes(b"%PDF-1.4 fake")
    return sample
