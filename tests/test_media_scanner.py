"""Unit tests for app.media_scanner (detection, recursion, ordering, ignores)."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.media_scanner import (
    IMAGE_EXTENSIONS,
    VIDEO_EXTENSIONS,
    classify_extension,
    scan_directory,
)
from tests.conftest import make_acceptance_tree, make_media_tree


class TestExtensionClassification:
    @pytest.mark.parametrize("ext", sorted(IMAGE_EXTENSIONS))
    def test_image_extensions(self, ext):
        assert classify_extension(f"file{ext}") == "image"

    @pytest.mark.parametrize("ext", sorted(VIDEO_EXTENSIONS))
    def test_video_extensions(self, ext):
        assert classify_extension(f"file{ext}") == "video"

    @pytest.mark.parametrize("ext", sorted(IMAGE_EXTENSIONS))
    def test_uppercase_image_extensions(self, ext):
        assert classify_extension(f"file{ext.upper()}") == "image"

    @pytest.mark.parametrize("ext", sorted(VIDEO_EXTENSIONS))
    def test_uppercase_video_extensions(self, ext):
        assert classify_extension(f"file{ext.upper()}") == "video"

    def test_mixed_case(self):
        assert classify_extension("image.JpG") == "image"
        assert classify_extension("clip.MkV") == "video"

    def test_unsupported(self):
        assert classify_extension("document.pdf") is None
        assert classify_extension("readme.txt") is None
        assert classify_extension("noextension") is None


class TestScanDirectory:
    def test_media_fixture_counts(self, tmp_path):
        root = make_media_tree(tmp_path)
        result = scan_directory(root)
        assert result.image_count == 3
        assert result.video_count == 2
        assert result.ignored_count == 1

    def test_natural_ordering_of_images(self, tmp_path):
        root = make_media_tree(tmp_path)
        result = scan_directory(root)
        names = [f.filename for f in result.images]
        assert names == ["image1.jpg", "image2.png", "image10.jpg"]

    def test_indexes_are_sequential(self, tmp_path):
        root = make_media_tree(tmp_path)
        result = scan_directory(root)
        assert [f.index for f in result.images] == [1, 2, 3]
        assert [f.index for f in result.videos] == [1, 2]

    def test_relative_paths_not_absolute(self, tmp_path):
        root = make_media_tree(tmp_path)
        result = scan_directory(root, scan_root=tmp_path)
        for media in result.images + result.videos:
            assert not media.relative_path.startswith("/")
            assert str(tmp_path) not in media.relative_path
            assert media.relative_path.startswith("folder/")

    def test_scan_root_makes_paths_relative_to_extracted(self, tmp_path):
        root = make_media_tree(tmp_path)
        result = scan_directory(root, scan_root=tmp_path)
        assert result.images[0].relative_path == "folder/image1.jpg"

    def test_nested_directories_are_recursed(self, tmp_path):
        deep = tmp_path / "a" / "b" / "c" / "d"
        deep.mkdir(parents=True)
        (deep / "deep.jpg").write_bytes(b"img")
        result = scan_directory(tmp_path)
        assert result.image_count == 1
        assert result.images[0].relative_path == "a/b/c/d/deep.jpg"

    def test_ignored_files_recorded(self, tmp_path):
        root = make_media_tree(tmp_path)
        result = scan_directory(root)
        ignored = [Path(p).name for p in result.ignored]
        assert ignored == ["note.txt"]

    def test_acceptance_tree(self, tmp_path):
        root = make_acceptance_tree(tmp_path)
        result = scan_directory(root)
        assert result.image_count == 3
        assert result.video_count == 2
        assert result.ignored_count == 2
        assert [f.filename for f in result.images] == ["1.jpg", "2.png", "10.webp"]
        assert [f.filename for f in result.videos] == ["1.mp4", "2.mkv"]

    def test_empty_directory(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        result = scan_directory(empty)
        assert result.total_files == 0

    def test_missing_directory_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            scan_directory(tmp_path / "missing")

    def test_scan_result_serialisation(self, tmp_path):
        root = make_media_tree(tmp_path)
        payload = scan_directory(root).to_dict()
        assert payload["image_files"] == 3
        assert payload["video_files"] == 2
        assert payload["ignored_files"] == 1
        assert payload["images"][0]["type"] == "image"

    def test_custom_detector_hook_overrides_extension(self, tmp_path):
        root = tmp_path / "media"
        root.mkdir()
        (root / "mystery.dat").write_bytes(b"img")
        result = scan_directory(root, detector=lambda p: "image" if p.suffix == ".dat" else None)
        assert result.image_count == 1

    def test_symlinks_are_not_treated_as_media(self, tmp_path):
        root = tmp_path / "media"
        root.mkdir()
        real = tmp_path / "real.jpg"
        real.write_bytes(b"img")
        link = root / "linked.jpg"
        try:
            link.symlink_to(real)
        except (OSError, NotImplementedError):  # pragma: no cover
            pytest.skip("symlinks not supported on this platform")
        result = scan_directory(root)
        assert result.image_count == 0
