"""Offline Part 2 uploader tests."""
import pytest
from app.uploaders.imgbb import ImgBBUploader, UploadError as ImgBBError
from app.uploaders.telegraph import TelegraphUploader, UploadError as TelegraphError

class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload
    def json(self):
        return self._payload

class FakeClient:
    def __init__(self, response):
        self.response = response
        self.calls = 0
    async def __aenter__(self): return self
    async def __aexit__(self, *args): return None
    async def post(self, *args, **kwargs):
        self.calls += 1
        return self.response

@pytest.mark.asyncio
async def test_imgbb_success(tmp_path, monkeypatch):
    image = tmp_path / "a.jpg"; image.write_bytes(b"image")
    fake = FakeClient(FakeResponse(payload={"success": True, "data": {"url": "https://i.ibb.co/example/a.jpg"}}))
    monkeypatch.setattr("app.uploaders.imgbb.httpx.AsyncClient", lambda **kwargs: fake)
    result = await ImgBBUploader("secret").upload(image)
    assert result["provider"] == "imgbb"
    assert result["url"].startswith("https://")

@pytest.mark.asyncio
async def test_imgbb_missing_key(tmp_path):
    image = tmp_path / "a.jpg"; image.write_bytes(b"image")
    with pytest.raises(ImgBBError, match="IMGBB_API_KEY"):
        await ImgBBUploader(None).upload(image)

@pytest.mark.asyncio
async def test_imgbb_failure(tmp_path, monkeypatch):
    image = tmp_path / "a.jpg"; image.write_bytes(b"image")
    fake = FakeClient(FakeResponse(payload={"success": False, "error": {"message": "bad key"}}))
    monkeypatch.setattr("app.uploaders.imgbb.httpx.AsyncClient", lambda **kwargs: fake)
    with pytest.raises(ImgBBError, match="bad key"):
        await ImgBBUploader("secret").upload(image)

@pytest.mark.asyncio
async def test_telegraph_success(tmp_path, monkeypatch):
    video = tmp_path / "a.mp4"; video.write_bytes(b"video")
    fake = FakeClient(FakeResponse(payload=[{"src": "/file/example.mp4"}]))
    monkeypatch.setattr("app.uploaders.telegraph.httpx.AsyncClient", lambda **kwargs: fake)
    result = await TelegraphUploader().upload(video)
    assert result["url"] == "https://telegra.ph/file/example.mp4"

@pytest.mark.asyncio
async def test_telegraph_failure(tmp_path, monkeypatch):
    video = tmp_path / "a.mp4"; video.write_bytes(b"video")
    fake = FakeClient(FakeResponse(payload={"error": "bad"}))
    monkeypatch.setattr("app.uploaders.telegraph.httpx.AsyncClient", lambda **kwargs: fake)
    with pytest.raises(TelegraphError):
        await TelegraphUploader().upload(video)

@pytest.mark.asyncio
async def test_telegraph_size_limit(tmp_path):
    video = tmp_path / "a.mp4"; video.write_bytes(b"x" * 20)
    with pytest.raises(TelegraphError, match="limit"):
        await TelegraphUploader(max_bytes=10).upload(video)
