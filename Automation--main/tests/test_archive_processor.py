"""Unit tests for app.archive_processor (validation, traversal, bombs, symlinks)."""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from app.archive_processor import (
    ArchiveTooLargeError,
    InvalidArchiveError,
    TooManyFilesError,
    UnsafeArchiveError,
    safe_extract,
    validate_archive,
)
from tests.conftest import build_symlink_zip, build_zip


class TestValidateArchive:
    def test_valid_zip(self, tmp_path, settings):
        archive = build_zip(tmp_path / "ok.zip", {"a.txt": b"hello", "b/c.jpg": b"img"})
        info = validate_archive(archive, settings)
        assert info.file_count == 2
        assert info.total_uncompressed_bytes == len(b"hello") + len(b"img")

    def test_empty_zip_is_valid_but_empty(self, tmp_path, settings):
        archive = build_zip(tmp_path / "empty.zip", {})
        info = validate_archive(archive, settings)
        assert info.file_count == 0
        assert info.member_count == 0

    def test_corrupted_zip_rejected(self, tmp_path, settings):
        broken = tmp_path / "broken.zip"
        broken.write_bytes(b"PK\x03\x04this-is-not-a-real-zip")
        with pytest.raises(InvalidArchiveError):
            validate_archive(broken, settings)

    def test_non_zip_file_rejected(self, tmp_path, settings):
        plain = tmp_path / "plain.txt"
        plain.write_text("not a zip")
        with pytest.raises(InvalidArchiveError):
            validate_archive(plain, settings)

    def test_missing_file_rejected(self, tmp_path, settings):
        with pytest.raises(InvalidArchiveError):
            validate_archive(tmp_path / "nope.zip", settings)

    def test_path_traversal_rejected(self, tmp_path, settings):
        archive = build_zip(tmp_path / "evil.zip", {"../evil.txt": b"x"})
        with pytest.raises(UnsafeArchiveError):
            validate_archive(archive, settings)

    def test_absolute_path_rejected(self, tmp_path, settings):
        archive = build_zip(tmp_path / "abs.zip", {"/etc/passwd": b"x"})
        with pytest.raises(UnsafeArchiveError):
            validate_archive(archive, settings)

    def test_drive_letter_path_rejected(self, tmp_path, settings):
        archive = build_zip(tmp_path / "drive.zip", {"C:/Windows/evil.txt": b"x"})
        with pytest.raises(UnsafeArchiveError):
            validate_archive(archive, settings)

    def test_archive_size_limit_enforced(self, tmp_path, settings):
        # settings.max_archive_size_mb == 1 -> the ZIP itself must exceed 1 MiB,
        # so it is stored uncompressed (deflate would shrink zeros to a few KB).
        archive = build_zip(
            tmp_path / "big.zip",
            {"a.bin": b"0" * (2 * 1024 * 1024)},
            compression=zipfile.ZIP_STORED,
        )
        assert archive.stat().st_size > settings.max_archive_size_bytes
        with pytest.raises(ArchiveTooLargeError):
            validate_archive(archive, settings)

    def test_excessive_file_count_rejected(self, tmp_path, settings):
        small_settings = settings
        object.__setattr__(small_settings, "max_files_per_archive", 5)
        archive = build_zip(
            tmp_path / "many.zip", {f"f{i}.txt": b"x" for i in range(10)}
        )
        with pytest.raises(TooManyFilesError):
            validate_archive(archive, small_settings)

    def test_declared_expansion_limit_rejected(self, tmp_path, settings):
        # settings.max_extracted_size_mb == 1 -> 2 MiB of zeros compresses tiny
        archive = build_zip(tmp_path / "bomb.zip", {"bomb.bin": b"\x00" * (2 * 1024 * 1024)})
        assert archive.stat().st_size < settings.max_archive_size_bytes  # small on disk
        with pytest.raises(ArchiveTooLargeError):
            validate_archive(archive, settings)


class TestSafeExtract:
    def test_extracts_nested_tree(self, tmp_path, settings):
        archive = build_zip(
            tmp_path / "tree.zip",
            {"MyFolder/images/001.jpg": b"img", "MyFolder/other/doc.pdf": b"pdf"},
        )
        result = safe_extract(archive, tmp_path / "out", settings)
        assert result.extracted_files == 2
        assert (tmp_path / "out" / "MyFolder" / "images" / "001.jpg").exists()
        assert (tmp_path / "out" / "MyFolder" / "other" / "doc.pdf").exists()

    def test_nothing_written_outside_root(self, tmp_path, settings):
        archive = build_zip(tmp_path / "evil.zip", {"../escape.txt": b"boom"})
        root = tmp_path / "out"
        with pytest.raises(UnsafeArchiveError):
            safe_extract(archive, root, settings)
        assert not (tmp_path / "escape.txt").exists()

    def test_actual_stream_size_limit_enforced(self, tmp_path, settings, monkeypatch):
        """A lying header must not bypass the limit: real streamed bytes are counted.

        The archive itself is tiny and its declared size passes validation, but the
        member stream delivers more bytes than the configured maximum. Only the
        byte counter in the extraction loop can catch this.
        """
        archive = build_zip(tmp_path / "liar.zip", {"liar.bin": b"A"})
        over_limit = settings.max_extracted_size_bytes + 1

        class LiarStream:
            def __init__(self) -> None:
                self._sent = False

            def read(self, _n: int = -1) -> bytes:
                if self._sent:
                    return b""
                self._sent = True
                return b"A" * over_limit

            def __enter__(self):
                return self

            def __exit__(self, *_exc) -> bool:
                return False

        def fake_open(self, *_args, **_kwargs):
            return LiarStream()

        monkeypatch.setattr(zipfile.ZipFile, "open", fake_open)

        root = tmp_path / "out"
        with pytest.raises(ArchiveTooLargeError):
            safe_extract(archive, root, settings)
        # partial file must have been removed
        assert not (root / "liar.bin").exists()

    def test_symlink_member_skipped(self, tmp_path, settings):
        archive = build_symlink_zip(tmp_path / "link.zip")
        result = safe_extract(archive, tmp_path / "out", settings)
        assert result.extracted_files == 0
        assert result.skipped_members

    def test_symlink_rejected_by_validation(self, tmp_path, settings):
        archive = build_symlink_zip(tmp_path / "link2.zip")
        with pytest.raises(UnsafeArchiveError):
            validate_archive(archive, settings)

    def test_empty_zip_extracts_zero_files(self, tmp_path, settings):
        archive = build_zip(tmp_path / "empty.zip", {})
        result = safe_extract(archive, tmp_path / "out", settings)
        assert result.extracted_files == 0

    def test_directory_entries_are_created(self, tmp_path, settings):
        archive = tmp_path / "dirs.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("folder/", "")
            zf.writestr("folder/file.jpg", b"img")
        result = safe_extract(archive, tmp_path / "out", settings)
        assert result.extracted_files == 1
        assert (tmp_path / "out" / "folder" / "file.jpg").exists()

    def test_cancellation_stops_extraction(self, tmp_path, settings):
        archive = build_zip(
            tmp_path / "cancel.zip", {f"f{i}.bin": b"x" * 1024 for i in range(20)}
        )
        calls = {"n": 0}

        def should_cancel() -> bool:
            calls["n"] += 1
            return calls["n"] > 2

        from app.archive_processor import ExtractionCancelledError

        with pytest.raises(ExtractionCancelledError):
            safe_extract(archive, tmp_path / "out", settings, should_cancel=should_cancel)
