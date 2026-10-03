"""ImgBB image uploader for Part 2."""
from __future__ import annotations
import asyncio
import mimetypes
from pathlib import Path
from typing import Any
import httpx

from .common import PART2_IMAGE_MAX_BYTES, UploadError, redact, validate_image_file

IMGBB_UPLOAD_URL = "https://api.imgbb.com/1/upload"

__all__ = ["ImgBBUploader", "UploadError", "IMGBB_UPLOAD_URL"]

class ImgBBUploader:
    """Upload images to ImgBB; videos are deliberately unsupported."""
    def __init__(self, api_key: str | None, *, timeout_seconds: float = 120.0, max_retries: int = 3, max_bytes: int = PART2_IMAGE_MAX_BYTES) -> None:
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.max_retries = max(0, int(max_retries))
        self.max_bytes = int(max_bytes)

    async def upload(self, path: Path) -> dict[str, Any]:
        path = Path(path)
        validate_image_file(path, provider="ImgBB", max_bytes=self.max_bytes)
        if not self.api_key:
            raise UploadError("IMGBB_API_KEY is not configured")
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        for attempt in range(self.max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
                    with path.open("rb") as handle:
                        response = await client.post(
                            IMGBB_UPLOAD_URL,
                            data={"key": self.api_key},
                            files={"image": (path.name, handle, content_type)},
                        )
                if response.status_code == 429 or response.status_code >= 500:
                    if attempt < self.max_retries:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    raise UploadError(f"ImgBB temporary HTTP error: {response.status_code}")
                if response.status_code >= 400:
                    raise UploadError(f"ImgBB rejected upload (HTTP {response.status_code})")
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise UploadError("ImgBB returned invalid JSON") from exc
                if not payload.get("success"):
                    error = payload.get("error") or {}
                    message = error.get("message") if isinstance(error, dict) else str(error)
                    raise UploadError(redact(message or "ImgBB upload failed", self.api_key))
                data = payload.get("data") or {}
                url = data.get("url")
                if not url:
                    raise UploadError("ImgBB response did not contain data.url")
                return {
                    "provider": "imgbb", "url": str(url),
                    "display_url": data.get("display_url"), "id": data.get("id"),
                    "size": data.get("size"), "width": data.get("width"), "height": data.get("height"),
                }
            except UploadError:
                raise
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < self.max_retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise UploadError("ImgBB network request failed after retries") from exc
            except OSError as exc:
                raise UploadError("Could not read image file") from exc
        raise UploadError("ImgBB upload failed")
