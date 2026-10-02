"""Unit tests for app.utils (natural sorting + path safety)."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.utils import (
    UnsafePathError,
    format_bytes,
    is_within_directory,
    natural_sort_key,
    natural_sorted,
    relative_posix_path,
    resolve_archive_member,
    sanitize_member_name,
    truncate,
)


class TestNaturalSorting:
    def test_numbers_ordered_naturally(self):
        names = ["image1.jpg", "image10.jpg", "image2.jpg"]
        assert natural_sorted(names) == ["image1.jpg", "image2.jpg", "image10.jpg"]

    def test_zero_padding_and_large_numbers(self):
        names = ["frame100.png", "frame20.png", "frame3.png"]
        assert natural_sorted(names) == ["frame3.png", "frame20.png", "frame100.png"]

    def test_case_insensitive(self):
        names = ["B.mp4", "a.mp4", "C.mp4"]
        assert natural_sorted(names) == ["a.mp4", "B.mp4", "C.mp4"]

    def test_key_accepts_paths(self):
        paths = [Path("v10.mkv"), Path("v2.mkv")]
        assert natural_sorted(paths) == [Path("v2.mkv"), Path("v10.mkv")]

    def test_sorting_is_deterministic(self):
        names = ["2.png", "10.webp", "1.jpg"]
        assert natural_sorted(names) == natural_sorted(list(reversed(names)))

    def test_mixed_names(self):
        # numbers are compared numerically inside the same textual prefix
        names = ["b2", "a10", "a2"]
        assert natural_sorted(names) == ["a2", "a10", "b2"]

    def test_prefix_is_compared_before_number(self):
        assert natural_sorted(["img10.png", "img2.png"]) == ["img2.png", "img10.png"]


class TestPathSafety:
    @pytest.mark.parametrize(
        "bad",
        [
            "../evil.txt",
            "../../evil.txt",
            "folder/../../evil.txt",
            "/etc/passwd",
            "//host/share/file",
            "C:\\Windows\\evil.txt",
            "c:/windows/evil.txt",
            "",
            "   ",
            "./",
        ],
    )
    def test_rejects_unsafe_member_names(self, bad):
        with pytest.raises(UnsafePathError):
            sanitize_member_name(bad)

    @pytest.mark.parametrize(
        "good,expected",
        [
            ("a.txt", "a.txt"),
            ("folder/a.txt", "folder/a.txt"),
            ("./folder/a.txt", "folder/a.txt"),
            ("folder\\a.txt", "folder/a.txt"),
            ("a/./b/c.jpg", "a/b/c.jpg"),
        ],
    )
    def test_normalises_safe_member_names(self, good, expected):
        assert sanitize_member_name(good) == expected

    def test_resolve_archive_member_stays_inside_root(self, tmp_path):
        resolved = resolve_archive_member(tmp_path, "a/b/c.jpg")
        assert str(resolved).startswith(str(tmp_path.resolve()))

    def test_resolve_archive_member_rejects_traversal(self, tmp_path):
        with pytest.raises(UnsafePathError):
            resolve_archive_member(tmp_path, "../outside.txt")

    def test_is_within_directory(self, tmp_path):
        inside = tmp_path / "a" / "b.txt"
        assert is_within_directory(tmp_path, inside) is True
        assert is_within_directory(tmp_path, tmp_path) is True
        assert is_within_directory(tmp_path / "a", tmp_path / "b.txt") is False
        assert is_within_directory(tmp_path, tmp_path.parent / "elsewhere.txt") is False

    def test_relative_posix_path(self, tmp_path):
        target = tmp_path / "Sample" / "Images" / "1.jpg"
        target.parent.mkdir(parents=True)
        target.write_bytes(b"x")
        assert relative_posix_path(target, tmp_path) == "Sample/Images/1.jpg"


class TestFormatting:
    def test_format_bytes(self):
        assert format_bytes(0) == "0 B"
        assert format_bytes(1024) == "1.0 KB"
        assert format_bytes(1024 * 1024) == "1.0 MB"
        assert format_bytes(None) == "unknown"

    def test_truncate(self):
        assert truncate("abc", 10) == "abc"
        assert len(truncate("abcdefghij", 5)) == 5
        assert truncate("abcdefghij", 5).endswith("…")
