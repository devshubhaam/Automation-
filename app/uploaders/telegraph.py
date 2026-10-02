"""Telegraph image uploader for Part 2."""
from __future__ import annotations
import asyncio
import mimetypes
from pathlib import Path
from typing import Any
import httpx

TELEGRAPH_UPLOAD_URL = "https://telegra.ph/upload"
TELEGRAPH_MAX_BYTES = 5 * 1024 * 1024
PART2_IMAGE_MAX_BYTES = 2 * 1024 * 1024

class UploadError(RuntimeError):
    """Raised when a Telegraph upload cannot be completed."""

class TelegraphUploader:
    """Upload images to Telegraph; videos are deliberately unsupported."""
    def __init__(self, *, timeout_seconds: float = 120.0, max_retries: int = 3, max_bytes: int = PART2_IMAGE_MAX_BYTES) -> None:
        self.timeout_seconds = float(timeout_seconds)
        self.max_retries = max(0, int(max_retries))
        self.max_bytes = min(int(max_bytes), TELEGRAPH_MAX_BYTES)

    async def upload(self, path: Path) -> dict[str, Any]:
        path = Path(path)
        if not path.is_file():
            raise UploadError(f"Image file does not exist: {path.name}")
        content_type = mimetypes.guess_type(path.name)[0] or ""
        if not content_type.startswith("image/"):
            raise UploadError(f"Telegraph Part 2 accepts images only: {path.name}")
        size = path.stat().st_size
        if size > self.max_bytes:
            raise UploadError(f"Image exceeds Telegraph Part 2 limit ({size} bytes > {self.max_bytes} bytes)")
        for attempt in range(self.max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
                    with path.open("rb") as handle:
                        response = await client.post(TELEGRAPH_UPLOAD_URL, files={"file": (path.name, handle, content_type)})
                if response.status_code == 429 or response.status_code >= 500:
                    if attempt < self.max_retries:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    raise UploadError(f"Telegraph temporary HTTP error: {response.status_code}")
                if response.status_code >= 400:
                    raise UploadError(f"Telegraph rejected upload (HTTP {response.status_code})")
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise UploadError("Telegraph returned invalid JSON") from exc
                if not isinstance(payload, list) or not payload or not isinstance(payload[0], dict):
                    raise UploadError("Telegraph returned an unexpected upload response")
                src = payload[0].get("src")
                if not src:
                    raise UploadError("Telegraph response did not contain src")
                src = str(src)
                url = src if src.startswith(("http://", "https://")) else f"https://telegra.ph{src}"
                return {"provider": "telegraph", "url": url, "src": src}
            except UploadError:
                raise
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < self.max_retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise UploadError("Telegraph network request failed after retries") from exc
            except OSError as exc:
                raise UploadError("Could not read image file") from exc
        raise UploadError("Telegraph upload failed")
