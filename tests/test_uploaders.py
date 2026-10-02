"""Uploader tests (offline; httpx is mocked)."""

from pathlib import Path

import pytest

from app.uploaders import ImgBBUploader, PART2_IMAGE_MAX_BYTES, TelegraphUploader, UploadError
from app.uploaders import imgbb as imgbb_module
from app.uploaders import telegraph as telegraph_module

SECRET = "SUPERSECRETKEY123"


def test_single_shared_limit():
    assert PART2_IMAGE_MAX_BYTES == 2 * 1024 * 1024
    assert telegraph_module.PART2_IMAGE_MAX_BYTES == PART2_IMAGE_MAX_BYTES
    assert TelegraphUploader().max_bytes == PART2_IMAGE_MAX_BYTES
    assert ImgBBUploader(SECRET).max_bytes == PART2_IMAGE_MAX_BYTES


@pytest.mark.asyncio
async def test_telegraph_rejects_non_image(tmp_path: Path):
    path = tmp_path / "video.mp4"
    path.write_bytes(b"not-a-real-video")
    with pytest.raises(UploadError, match="images only"):
        await TelegraphUploader().upload(path)


@pytest.mark.asyncio
async def test_telegraph_rejects_large_image(tmp_path: Path):
    path = tmp_path / "large.jpg"
    path.write_bytes(b"x" * (PART2_IMAGE_MAX_BYTES + 1))
    with pytest.raises(UploadError, match="exceeds"):
        await TelegraphUploader().upload(path)


@pytest.mark.asyncio
async def test_imgbb_rejects_video_without_network(tmp_path: Path):
    path = tmp_path / "clip.mp4"
    path.write_bytes(b"video")
    with pytest.raises(UploadError, match="images only"):
        await ImgBBUploader(SECRET).upload(path)


@pytest.mark.asyncio
async def test_imgbb_rejects_large_image(tmp_path: Path):
    path = tmp_path / "large.png"
    path.write_bytes(b"x" * (PART2_IMAGE_MAX_BYTES + 1))
    with pytest.raises(UploadError, match="exceeds"):
        await ImgBBUploader(SECRET).upload(path)


@pytest.mark.asyncio
async def test_imgbb_requires_api_key(tmp_path: Path):
    path = tmp_path / "a.jpg"
    path.write_bytes(b"img")
    with pytest.raises(UploadError, match="IMGBB_API_KEY"):
        await ImgBBUploader(None).upload(path)


class _FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


def _patch_client(monkeypatch, module, response):
    class FakeClient:
        def __init__(self, *a, **k): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def post(self, url, **kwargs): return response

    monkeypatch.setattr(module.httpx, "AsyncClient", FakeClient)


@pytest.mark.asyncio
async def test_imgbb_error_message_never_contains_api_key(tmp_path, monkeypatch):
    path = tmp_path / "a.jpg"
    path.write_bytes(b"img")
    _patch_client(monkeypatch, imgbb_module,
                  _FakeResponse(200, {"success": False, "error": {"message": f"Invalid key {SECRET}"}}))
    with pytest.raises(UploadError) as info:
        await ImgBBUploader(SECRET, max_retries=0).upload(path)
    assert SECRET not in str(info.value)


@pytest.mark.asyncio
async def test_imgbb_http_error_never_contains_api_key(tmp_path, monkeypatch):
    path = tmp_path / "a.jpg"
    path.write_bytes(b"img")
    _patch_client(monkeypatch, imgbb_module, _FakeResponse(400, {}))
    with pytest.raises(UploadError) as info:
        await ImgBBUploader(SECRET, max_retries=0).upload(path)
    assert SECRET not in str(info.value) and SECRET not in repr(info.value)


@pytest.mark.asyncio
async def test_imgbb_success_normalised(tmp_path, monkeypatch):
    path = tmp_path / "a.jpg"
    path.write_bytes(b"img")
    _patch_client(monkeypatch, imgbb_module,
                  _FakeResponse(200, {"success": True, "data": {"url": "https://i.ibb.co/x/a.jpg", "id": "x"}}))
    result = await ImgBBUploader(SECRET).upload(path)
    assert result["provider"] == "imgbb" and result["url"] == "https://i.ibb.co/x/a.jpg"


@pytest.mark.asyncio
async def test_telegraph_success_normalises_relative_src(tmp_path, monkeypatch):
    path = tmp_path / "a.png"
    path.write_bytes(b"img")
    _patch_client(monkeypatch, telegraph_module, _FakeResponse(200, [{"src": "/file/abc.png"}]))
    result = await TelegraphUploader().upload(path)
    assert result == {"provider": "telegraph", "url": "https://telegra.ph/file/abc.png", "src": "/file/abc.png"}


@pytest.mark.asyncio
async def test_telegraph_error_payload_is_permanent_failure(tmp_path, monkeypatch):
    path = tmp_path / "a.png"
    path.write_bytes(b"img")
    _patch_client(monkeypatch, telegraph_module, _FakeResponse(200, {"error": "File type invalid"}))
    with pytest.raises(UploadError):
        await TelegraphUploader().upload(path)
