"""Offline tests for Part 2 media uploaders."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from app.uploaders.imgbb import ImgBBUploader, UploadError as ImgBBUploadError
from app.uploaders.telegraph import (
    TELEGRAPH_MAX_BYTES,
    TelegraphUploader,
    UploadError as TelegraphUploadError,
)


class FakeResponse:
    """Minimal httpx.Response-like object for uploader tests."""

    def __init__(
        self,
        status_code: int = 200,
        json_data: Any = None,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self._json_data = json_data
        self.text = text

    def json(self) -> Any:
        return self._json_data

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(
                f"HTTP {self.status_code}: {self.text}"
            )


class FakeAsyncClient:
    """Fake httpx.AsyncClient."""

    def __init__(
        self,
        response: FakeResponse,
        calls: list[dict[str, Any]],
    ) -> None:
        self.response = response
        self.calls = calls

    async def __aenter__(self) -> "FakeAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def post(self, *args, **kwargs) -> FakeResponse:
        self.calls.append(
            {
                "args": args,
                "kwargs": kwargs,
            }
        )
        return self.response


class FakeAsyncClientFactory:
    """Factory compatible with httpx.AsyncClient(...)."""

    def __init__(
        self,
        response: FakeResponse,
        calls: list[dict[str, Any]],
    ) -> None:
        self.response = response
        self.calls = calls

    def __call__(self, *args, **kwargs) -> FakeAsyncClient:
        return FakeAsyncClient(
            response=self.response,
            calls=self.calls,
        )


@pytest.fixture
def image_file(tmp_path: Path) -> Path:
    """Create a small fake image file."""

    path = tmp_path / "test-image.jpg"
    path.write_bytes(b"\xff\xd8\xff\xe0fake-image-data\xff\xd9")
    return path


@pytest.fixture
def video_file(tmp_path: Path) -> Path:
    """Create a small fake video file."""

    path = tmp_path / "test-video.mp4"
    path.write_bytes(b"fake-video-data")
    return path


# ---------------------------------------------------------------------------
# ImgBB tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_imgbb_upload_success(
    image_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ImgBB should return the uploaded image URL."""

    calls: list[dict[str, Any]] = []

    response = FakeResponse(
        status_code=200,
        json_data={
            "success": True,
            "data": {
                "id": "abc123",
                "url": "https://i.ibb.co/abc123/test-image.jpg",
                "display_url": "https://ibb.co/abc123",
                "width": 1280,
                "height": 720,
                "size": 12345,
            },
        },
    )

    monkeypatch.setattr(
        "app.uploaders.imgbb.httpx.AsyncClient",
        FakeAsyncClientFactory(response, calls),
    )

    uploader = ImgBBUploader(
        api_key="test-api-key",
    )

    result = await uploader.upload(image_file)

    assert result["provider"] == "imgbb"
    assert result["id"] == "abc123"
    assert result["url"] == (
        "https://i.ibb.co/abc123/test-image.jpg"
    )
    assert result["display_url"] == "https://ibb.co/abc123"
    assert result["width"] == 1280
    assert result["height"] == 720
    assert result["size"] == 12345

    assert len(calls) == 1

    request = calls[0]

    assert request["args"][0] == (
        "https://api.imgbb.com/1/upload"
    )

    assert request["kwargs"]["params"]["key"] == "test-api-key"
    assert "files" in request["kwargs"]

    files = request["kwargs"]["files"]
    assert "image" in files


@pytest.mark.asyncio
async def test_imgbb_requires_api_key(
    image_file: Path,
) -> None:
    """ImgBB should fail when no API key is configured."""

    uploader = ImgBBUploader(
        api_key=None,
    )

    with pytest.raises(ImgBBUploadError, match="IMGBB_API_KEY"):
        await uploader.upload(image_file)


@pytest.mark.asyncio
async def test_imgbb_missing_file(
    tmp_path: Path,
) -> None:
    """ImgBB should reject a missing file."""

    missing = tmp_path / "does-not-exist.jpg"

    uploader = ImgBBUploader(
        api_key="test-api-key",
    )

    with pytest.raises(ImgBBUploadError):
        await uploader.upload(missing)


@pytest.mark.asyncio
async def test_imgbb_rejects_failed_response(
    image_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ImgBB should raise UploadError when the API reports failure."""

    calls: list[dict[str, Any]] = []

    response = FakeResponse(
        status_code=400,
        json_data={
            "success": False,
            "error": {
                "message": "Invalid API key",
            },
        },
        text="Invalid API key",
    )

    monkeypatch.setattr(
        "app.uploaders.imgbb.httpx.AsyncClient",
        FakeAsyncClientFactory(response, calls),
    )

    uploader = ImgBBUploader(
        api_key="bad-api-key",
        max_retries=0,
    )

    with pytest.raises(ImgBBUploadError):
        await uploader.upload(image_file)


@pytest.mark.asyncio
async def test_imgbb_rejects_unsuccessful_json(
    image_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HTTP 200 with success=false should still fail."""

    calls: list[dict[str, Any]] = []

    response = FakeResponse(
        status_code=200,
        json_data={
            "success": False,
            "error": {
                "message": "Upload failed",
            },
        },
    )

    monkeypatch.setattr(
        "app.uploaders.imgbb.httpx.AsyncClient",
        FakeAsyncClientFactory(response, calls),
    )

    uploader = ImgBBUploader(
        api_key="test-api-key",
        max_retries=0,
    )

    with pytest.raises(ImgBBUploadError):
        await uploader.upload(image_file)


@pytest.mark.asyncio
async def test_imgbb_handles_missing_url(
    image_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ImgBB should fail if a successful response has no URL."""

    calls: list[dict[str, Any]] = []

    response = FakeResponse(
        status_code=200,
        json_data={
            "success": True,
            "data": {
                "id": "abc123",
            },
        },
    )

    monkeypatch.setattr(
        "app.uploaders.imgbb.httpx.AsyncClient",
        FakeAsyncClientFactory(response, calls),
    )

    uploader = ImgBBUploader(
        api_key="test-api-key",
        max_retries=0,
    )

    with pytest.raises(ImgBBUploadError):
        await uploader.upload(image_file)


# ---------------------------------------------------------------------------
# Telegraph tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_telegraph_upload_success(
    video_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Telegraph should convert /file/... into a full URL."""

    calls: list[dict[str, Any]] = []

    response = FakeResponse(
        status_code=200,
        json_data=[
            {
                "src": "/file/test-video.mp4",
            }
        ],
    )

    monkeypatch.setattr(
        "app.uploaders.telegraph.httpx.AsyncClient",
        FakeAsyncClientFactory(response, calls),
    )

    uploader = TelegraphUploader(
        max_retries=0,
    )

    result = await uploader.upload(video_file)

    assert result["provider"] == "telegraph"
    assert result["url"] == (
        "https://telegra.ph/file/test-video.mp4"
    )

    assert len(calls) == 1

    request = calls[0]

    assert request["args"][0] == (
        "https://telegra.ph/upload"
    )

    assert "files" in request["kwargs"]
    assert "file" in request["kwargs"]["files"]


@pytest.mark.asyncio
async def test_telegraph_accepts_absolute_url(
    video_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Telegraph should preserve an already absolute URL."""

    calls: list[dict[str, Any]] = []

    response = FakeResponse(
        status_code=200,
        json_data=[
            {
                "src": "https://telegra.ph/file/abc123.mp4",
            }
        ],
    )

    monkeypatch.setattr(
        "app.uploaders.telegraph.httpx.AsyncClient",
        FakeAsyncClientFactory(response, calls),
    )

    uploader = TelegraphUploader(
        max_retries=0,
    )

    result = await uploader.upload(video_file)

    assert result["provider"] == "telegraph"
    assert result["url"] == (
        "https://telegra.ph/file/abc123.mp4"
    )


@pytest.mark.asyncio
async def test_telegraph_missing_file(
    tmp_path: Path,
) -> None:
    """Telegraph should reject a missing video."""

    missing = tmp_path / "missing.mp4"

    uploader = TelegraphUploader(
        max_retries=0,
    )

    with pytest.raises(TelegraphUploadError):
        await uploader.upload(missing)


@pytest.mark.asyncio
async def test_telegraph_rejects_large_file(
    tmp_path: Path,
) -> None:
    """Telegraph should reject files over its direct-upload limit."""

    large_file = tmp_path / "large-video.mp4"

    # Sparse file keeps the test fast and avoids allocating 5+ MB in memory.
    with large_file.open("wb") as file:
        file.truncate(TELEGRAPH_MAX_BYTES + 1)

    uploader = TelegraphUploader(
        max_retries=0,
    )

    with pytest.raises(
        TelegraphUploadError,
        match="limit",
    ):
        await uploader.upload(large_file)


@pytest.mark.asyncio
async def test_telegraph_rejects_failed_response(
    video_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Telegraph should raise UploadError on HTTP failure."""

    calls: list[dict[str, Any]] = []

    response = FakeResponse(
        status_code=500,
        json_data=None,
        text="Internal Server Error",
    )

    monkeypatch.setattr(
        "app.uploaders.telegraph.httpx.AsyncClient",
        FakeAsyncClientFactory(response, calls),
    )

    uploader = TelegraphUploader(
        max_retries=0,
    )

    with pytest.raises(TelegraphUploadError):
        await uploader.upload(video_file)


@pytest.mark.asyncio
async def test_telegraph_rejects_empty_response(
    video_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Telegraph should fail when the response contains no uploaded file."""

    calls: list[dict[str, Any]] = []

    response = FakeResponse(
        status_code=200,
        json_data=[],
    )

    monkeypatch.setattr(
        "app.uploaders.telegraph.httpx.AsyncClient",
        FakeAsyncClientFactory(response, calls),
    )

    uploader = TelegraphUploader(
        max_retries=0,
    )

    with pytest.raises(TelegraphUploadError):
        await uploader.upload(video_file)


@pytest.mark.asyncio
async def test_telegraph_rejects_invalid_response(
    video_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Telegraph should fail when src is missing."""

    calls: list[dict[str, Any]] = []

    response = FakeResponse(
        status_code=200,
        json_data=[
            {
                "error": "upload failed",
            }
        ],
    )

    monkeypatch.setattr(
        "app.uploaders.telegraph.httpx.AsyncClient",
        FakeAsyncClientFactory(response, calls),
    )

    uploader = TelegraphUploader(
        max_retries=0,
    )

    with pytest.raises(TelegraphUploadError):
        await uploader.upload(video_file)


# ---------------------------------------------------------------------------
# Cross-uploader sanity tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_uploaders_do_not_modify_original_files(
    image_file: Path,
    video_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Uploaders must not modify the source media files."""

    image_before = image_file.read_bytes()
    video_before = video_file.read_bytes()

    imgbb_calls: list[dict[str, Any]] = []
    telegraph_calls: list[dict[str, Any]] = []

    imgbb_response = FakeResponse(
        status_code=200,
        json_data={
            "success": True,
            "data": {
                "id": "image-id",
                "url": "https://i.ibb.co/test/image.jpg",
                "display_url": "https://ibb.co/test",
            },
        },
    )

    telegraph_response = FakeResponse(
        status_code=200,
        json_data=[
            {
                "src": "/file/video.mp4",
            }
        ],
    )

    monkeypatch.setattr(
        "app.uploaders.imgbb.httpx.AsyncClient",
        FakeAsyncClientFactory(
            imgbb_response,
            imgbb_calls,
        ),
    )

    monkeypatch.setattr(
        "app.uploaders.telegraph.httpx.AsyncClient",
        FakeAsyncClientFactory(
            telegraph_response,
            telegraph_calls,
        ),
    )

    imgbb = ImgBBUploader(
        api_key="test-api-key",
        max_retries=0,
    )

    telegraph = TelegraphUploader(
        max_retries=0,
    )

    await imgbb.upload(image_file)
    await telegraph.upload(video_file)

    assert image_file.read_bytes() == image_before
    assert video_file.read_bytes() == video_before
