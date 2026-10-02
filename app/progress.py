"""Telegram progress-message rendering.

A single message per job is created and then *edited* in place, so the chat is
never flooded. Rendering is a pure function of :class:`Job`, which makes it
testable without Telegram.
"""

from __future__ import annotations

import logging
from typing import Optional, Protocol, Sequence

from .job_manager import Job, JobStatus
from .utils import format_bytes, truncate

__all__ = ["ProgressReporter", "MessageEditor", "render_progress", "STATUS_HEADLINES"]

logger = logging.getLogger("app.progress")

STATUS_HEADLINES = {
    JobStatus.RECEIVED: "⏳ New job",
    JobStatus.QUEUED: "🕒 Job queued",
    JobStatus.DOWNLOADING: "📥 Downloading ZIP...",
    JobStatus.EXTRACTING: "📦 Extracting archive...",
    JobStatus.SCANNING: "🔍 Scanning files...",
    JobStatus.COMPLETED: "✅ Scan complete",
    JobStatus.FAILED: "❌ Processing failed",
    JobStatus.CANCELLED: "🛑 Job cancelled",
}

TELEGRAM_MESSAGE_LIMIT = 4096


class MessageEditor(Protocol):
    """Minimal interface the reporter needs (implemented by the Telethon client)."""

    async def send(self, text: str) -> int:  # pragma: no cover - protocol
        ...

    async def edit(self, message_id: int, text: str) -> None:  # pragma: no cover - protocol
        ...


def _list_section(title: str, items: Sequence[str], limit: int = 20) -> str:
    if not items:
        return ""
    lines = [title]
    for position, item in enumerate(items[:limit], start=1):
        lines.append(f"{position}. {item}")
    if len(items) > limit:
        lines.append(f"... and {len(items) - limit} more")
    return "\n".join(lines)


def render_progress(
    job: Job,
    *,
    detail: Optional[str] = None,
    images: Optional[Sequence[str]] = None,
    videos: Optional[Sequence[str]] = None,
    ignored: Optional[Sequence[str]] = None,
    footer: Optional[str] = None,
) -> str:
    """Render the single progress/status message body for ``job``.

    ``detail`` is the current stage description for in-flight states and the
    failed stage for ``FAILED``; ``footer`` appends a trailing line (used for the
    "🧹 Cleaning up" state after the report has been sent).
    """
    headline = STATUS_HEADLINES.get(job.status, job.status.value)

    if job.status == JobStatus.COMPLETED:
        parts = [
            headline,
            "",
            f"Job: {job.job_id}",
            "",
            f"🖼 Images: {job.image_files}",
            f"🎬 Videos: {job.video_files}",
            f"📄 Ignored: {job.ignored_files}",
        ]
        images_block = _list_section("Images:", images or [])
        videos_block = _list_section("Videos:", videos or [])
        ignored_block = _list_section("Ignored:", ignored or [], limit=10)
        for block in (images_block, videos_block, ignored_block):
            if block:
                parts.extend(["", block])
        parts.extend(["", "PART 1 pipeline completed successfully."])
        if footer:
            parts.extend(["", footer])
        return truncate("\n".join(parts), TELEGRAM_MESSAGE_LIMIT)

    if job.status == JobStatus.FAILED:
        parts = [
            headline,
            "",
            f"Stage: {detail or 'processing'}",
            "",
            "Reason:",
            job.error or "Unknown error",
            "",
            f"Job: {job.job_id}",
        ]
        return truncate("\n".join(parts), TELEGRAM_MESSAGE_LIMIT)

    if job.status == JobStatus.CANCELLED:
        parts = [
            headline,
            "",
            f"Job: {job.job_id}",
            "Temporary files were cleaned up.",
        ]
        return truncate("\n".join(parts), TELEGRAM_MESSAGE_LIMIT)

    parts = [headline, "", f"Job: {job.job_id}", f"Status: {job.status.value}"]
    if detail:
        parts.extend(["", detail])
    if job.archive_name:
        size = format_bytes(job.archive_size_bytes) if job.archive_size_bytes else "unknown"
        parts.extend(["", f"Archive: {job.archive_name} ({size})"])
    if job.status == JobStatus.QUEUED and detail:
        pass
    return truncate("\n".join(parts), TELEGRAM_MESSAGE_LIMIT)


class ProgressReporter:
    """Create-once / edit-thereafter progress message for a single job."""

    def __init__(self, editor: MessageEditor) -> None:
        self._editor = editor
        self._message_id: Optional[int] = None
        self._last_text: Optional[str] = None

    @property
    def message_id(self) -> Optional[int]:
        return self._message_id

    async def start(self, job: Job, **kwargs) -> Optional[int]:
        text = render_progress(job, **kwargs)
        self._message_id = await self._editor.send(text)
        self._last_text = text
        return self._message_id

    async def update(self, job: Job, **kwargs) -> None:
        text = render_progress(job, **kwargs)
        if self._message_id is None:
            self._message_id = await self._editor.send(text)
            self._last_text = text
            return
        if text == self._last_text:
            return
        try:
            await self._editor.edit(self._message_id, text)
            self._last_text = text
        except Exception as exc:  # pragma: no cover - network/Telegram side
            # Message content unchanged or flood-wait etc. - never crash the job.
            logger.warning("Could not edit progress message: %s", exc)

    async def finish(self, job: Job, **kwargs) -> None:
        await self.update(job, **kwargs)
