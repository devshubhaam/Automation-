"""Telegraph article publisher for Part 2.

Telegraph no longer accepts image uploads, so files are **never** sent to
Telegraph. Images go to ImgBB, videos go to the video bot; this module takes
the resulting URLs and creates a Telegraph article that embeds the images as
``<img src="...">`` nodes and lists the videos as links, through the official
``createPage`` API (plain JSON, no file upload).
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Sequence

import httpx

from .common import UploadError, redact

TELEGRAPH_API_BASE = "https://api.telegra.ph"

#: Telegraph limits page content to 64 KB; ~200 <img> nodes stay far below it.
TELEGRAPH_MAX_IMAGES_PER_ARTICLE = 200
TELEGRAPH_TITLE_MAX = 256

__all__ = [
    "TelegraphPublisher",
    "UploadError",
    "TELEGRAPH_API_BASE",
    "TELEGRAPH_MAX_IMAGES_PER_ARTICLE",
]


def _body_hint(response: Any) -> str:
    """Short, whitespace-collapsed snippet of a provider error body."""
    text = getattr(response, "text", "") or ""
    text = " ".join(str(text).split())[:120]
    return f": {text}" if text else ""


VideoLink = tuple[str, str]  # (display name, url)


def build_content(
    image_urls: Sequence[str],
    video_links: Sequence[VideoLink] = (),
) -> list[dict[str, Any]]:
    """Build Telegraph ``Node`` content.

    One ``<img>`` per ImgBB URL, then (if any) a "Videos" heading followed by
    one linked paragraph per video.
    """
    nodes: list[dict[str, Any]] = []
    for url in image_urls:
        nodes.append({"tag": "img", "attrs": {"src": str(url)}})
    if video_links:
        nodes.append({"tag": "h4", "children": ["Videos"]})
        for name, url in video_links:
            nodes.append({
                "tag": "p",
                "children": [{"tag": "a", "attrs": {"href": str(url)}, "children": [str(name)]}],
            })
    return nodes


class TelegraphPublisher:
    """Create Telegraph articles that embed already-hosted image URLs."""

    def __init__(
        self,
        access_token: str | None = None,
        *,
        short_name: str = "MediaBot",
        author_name: str | None = None,
        timeout_seconds: float = 60.0,
        max_retries: int = 3,
        max_images_per_article: int = TELEGRAPH_MAX_IMAGES_PER_ARTICLE,
    ) -> None:
        self.access_token = access_token or None
        self.short_name = short_name
        self.author_name = author_name
        self.timeout_seconds = float(timeout_seconds)
        self.max_retries = max(0, int(max_retries))
        self.max_images_per_article = max(1, int(max_images_per_article))

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    async def publish(
        self,
        title: str,
        image_urls: Sequence[str],
        video_links: Sequence[VideoLink] = (),
    ) -> list[dict[str, Any]]:
        """Publish images + video links as one or more articles.

        An article holds at most ``max_images_per_article`` items; larger
        albums are split into "Part N" articles. Returns one
        ``{"provider", "title", "url", "path", "image_count", "video_count"}``
        dict per article. Raises :class:`UploadError` on failure.
        """
        images = [str(u) for u in image_urls if u]
        videos = [(str(n), str(u)) for n, u in video_links if u]
        if not images and not videos:
            raise UploadError("No image or video URLs to publish to Telegraph")
        for url in images + [u for _, u in videos]:
            if not url.startswith(("http://", "https://")):
                raise UploadError("Telegraph articles need absolute URLs")

        items: list[tuple[str, Any]] = [("img", u) for u in images] + [("vid", v) for v in videos]
        size = self.max_images_per_article
        chunks = [items[i : i + size] for i in range(0, len(items), size)]
        base_title = (title or "Media").strip() or "Media"

        results: list[dict[str, Any]] = []
        for index, chunk in enumerate(chunks, start=1):
            page_title = base_title if len(chunks) == 1 else f"{base_title} (Part {index})"
            results.append(
                await self.publish_article(
                    page_title[:TELEGRAPH_TITLE_MAX],
                    [v for kind, v in chunk if kind == "img"],
                    [v for kind, v in chunk if kind == "vid"],
                )
            )
        return results

    async def publish_article(
        self,
        title: str,
        image_urls: Sequence[str],
        video_links: Sequence[VideoLink] = (),
    ) -> dict[str, Any]:
        """Create exactly one Telegraph article."""
        token = await self._ensure_token()
        data: dict[str, str] = {
            "access_token": token,
            "title": title[:TELEGRAPH_TITLE_MAX],
            "content": json.dumps(build_content(image_urls, video_links), ensure_ascii=False),
            "return_content": "false",
        }
        if self.author_name:
            data["author_name"] = self.author_name[:128]

        result = await self._call("createPage", data)
        url = result.get("url")
        if not url:
            raise UploadError("Telegraph createPage response did not contain url")
        return {
            "provider": "telegraph_article",
            "title": title,
            "url": str(url),
            "path": result.get("path"),
            "image_count": len(image_urls),
            "video_count": len(video_links),
        }

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    async def _ensure_token(self) -> str:
        if self.access_token:
            return self.access_token
        data = {"short_name": self.short_name[:32]}
        if self.author_name:
            data["author_name"] = self.author_name[:128]
        result = await self._call("createAccount", data)
        token = result.get("access_token")
        if not token:
            raise UploadError("Telegraph createAccount did not return an access token")
        self.access_token = str(token)
        return self.access_token

    async def _call(self, method: str, data: dict[str, str]) -> dict[str, Any]:
        """POST one Telegraph API method and return its ``result`` object."""
        url = f"{TELEGRAPH_API_BASE}/{method}"
        secret = self.access_token or data.get("access_token")

        for attempt in range(self.max_retries + 1):
            try:
                async with httpx.AsyncClient(
                    timeout=self.timeout_seconds, follow_redirects=True
                ) as client:
                    response = await client.post(url, data=data)

                if response.status_code == 429 or response.status_code >= 500:
                    if attempt < self.max_retries:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    raise UploadError(f"Telegraph {method} temporary HTTP error: {response.status_code}")

                try:
                    payload = response.json()
                except ValueError as exc:
                    if response.status_code >= 400:
                        raise UploadError(
                            redact(f"Telegraph {method} failed (HTTP {response.status_code}{_body_hint(response)})", secret)
                        ) from exc
                    raise UploadError(f"Telegraph {method} returned invalid JSON") from exc

                if not isinstance(payload, dict):
                    raise UploadError(f"Telegraph {method} returned an unexpected response")

                if not payload.get("ok"):
                    error = str(payload.get("error") or f"HTTP {response.status_code}")
                    # FLOOD_WAIT_n is a temporary rate limit: wait and retry.
                    if error.startswith("FLOOD_WAIT_") and attempt < self.max_retries:
                        try:
                            wait = min(int(error.rsplit("_", 1)[1]), 30)
                        except ValueError:
                            wait = 2 ** attempt
                        await asyncio.sleep(wait)
                        continue
                    raise UploadError(redact(f"Telegraph {method} failed: {error}", secret))

                result = payload.get("result")
                if not isinstance(result, dict):
                    raise UploadError(f"Telegraph {method} response had no result")
                return result

            except UploadError:
                raise
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < self.max_retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise UploadError(f"Telegraph {method} network request failed after retries") from exc

        raise UploadError(f"Telegraph {method} failed")
