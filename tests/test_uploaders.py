from pathlib import Path
import pytest
from app.uploaders.telegraph import TelegraphUploader, UploadError

@pytest.mark.asyncio
async def test_telegraph_rejects_non_image(tmp_path: Path):
    path = tmp_path / "video.mp4"
    path.write_bytes(b"not-a-real-video")
    with pytest.raises(UploadError, match="images only"):
        await TelegraphUploader().upload(path)

@pytest.mark.asyncio
async def test_telegraph_rejects_large_image(tmp_path: Path):
    path = tmp_path / "large.jpg"
    path.write_bytes(b"x" * (2 * 1024 * 1024 + 1))
    with pytest.raises(UploadError, match="exceeds"):
        await TelegraphUploader().upload(path)
