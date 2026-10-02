"""Progress message formatting for the Telegram media processor."""

from __future__ import annotations

from typing import Any, Iterable, Optional

from .job_manager import Job, JobStatus


class ProgressRenderer:
    """Builds compact Telegram-friendly progress messages."""

    STATUS_ICONS = {
        JobStatus.RECEIVED: "📥",
        JobStatus.QUEUED: "⏳",
        JobStatus.DOWNLOADING: "⬇️",
        JobStatus.EXTRACTING: "📦",
        JobStatus.SCANNING: "🔎",
        JobStatus.UPLOADING: "☁️",
        JobStatus.COMPLETED: "✅",
        JobStatus.COMPLETED_WITH_ERRORS: "⚠️",
        JobStatus.FAILED: "❌",
        JobStatus.CANCELLED: "🚫",
    }

    STATUS_TEXT = {
        JobStatus.RECEIVED: "Received",
        JobStatus.QUEUED: "Queued",
        JobStatus.DOWNLOADING: "Downloading ZIP",
        JobStatus.EXTRACTING: "Extracting ZIP",
        JobStatus.SCANNING: "Scanning media",
        JobStatus.UPLOADING: "Uploading media",
        JobStatus.COMPLETED: "Completed",
        JobStatus.COMPLETED_WITH_ERRORS: "Completed with errors",
        JobStatus.FAILED: "Failed",
        JobStatus.CANCELLED: "Cancelled",
    }

    def render(self, job: Job) -> str:
        """Render the current state of a job."""

        icon = self.STATUS_ICONS.get(job.status, "ℹ️")
        status_text = self.STATUS_TEXT.get(
            job.status,
            str(job.status).replace("_", " ").title(),
        )

        lines: list[str] = [
            f"{icon} <b>Media Processor</b>",
            "",
            f"<b>Job:</b> <code>{self._escape(job.job_id)}</code>",
            f"<b>File:</b> {self._escape(job.archive_name)}",
            f"<b>Status:</b> {status_text}",
        ]

        if job.status == JobStatus.DOWNLOADING:
            lines.extend(
                [
                    "",
                    "⬇️ Downloading archive from Telegram...",
                ]
            )

        elif job.status == JobStatus.EXTRACTING:
            lines.extend(
                [
                    "",
                    "📦 Extracting ZIP safely...",
                ]
            )

        elif job.status == JobStatus.SCANNING:
            lines.extend(
                [
                    "",
                    "🔎 Scanning extracted files...",
                ]
            )

        elif job.status == JobStatus.UPLOADING:
            lines.extend(self._render_uploading(job))

        elif job.status == JobStatus.COMPLETED:
            lines.extend(self._render_completed(job))

        elif job.status == JobStatus.COMPLETED_WITH_ERRORS:
            lines.extend(self._render_completed_with_errors(job))

        elif job.status == JobStatus.FAILED:
            lines.extend(
                [
                    "",
                    f"❌ <b>Error:</b> {self._escape(job.error or 'Unknown error')}",
                ]
            )

        elif job.status == JobStatus.CANCELLED:
            lines.extend(
                [
                    "",
                    "🚫 Job cancelled.",
                ]
            )

        # Media summary is useful once scanning has happened.
        if self._has_media_stats(job):
            lines.extend(self._render_media_summary(job))

        return "\n".join(lines)

    def _render_uploading(self, job: Job) -> list[str]:
        lines: list[str] = [
            "",
            "☁️ <b>Uploading detected images...</b>",
        ]

        image_count = int(getattr(job, "image_count", 0) or 0)
        video_count = int(getattr(job, "video_count", 0) or 0)

        upload_results = list(getattr(job, "upload_results", []) or [])
        upload_failures = list(getattr(job, "upload_failures", []) or [])

        successful_images = self._count_successful_image_uploads(
            upload_results
        )

        if image_count:
            lines.append(
                f"🖼️ Images: {successful_images}/{image_count}"
            )

        if video_count:
            lines.append(
                f"🎬 Videos: {video_count} "
                "(queued for Part 3)"
            )

        if upload_failures:
            lines.append(
                f"⚠️ Upload failures: {len(upload_failures)}"
            )

        if image_count == 0 and video_count == 0:
            lines.append("No media detected.")

        return lines

    def _render_completed(self, job: Job) -> list[str]:
        lines: list[str] = [
            "",
            "🎉 <b>Processing completed successfully.</b>",
        ]

        upload_results = list(getattr(job, "upload_results", []) or [])

        if upload_results:
            lines.extend(
                [
                    "",
                    "🔗 <b>Generated links:</b>",
                ]
            )

            lines.extend(self._render_upload_results(upload_results))

        video_count = int(getattr(job, "video_count", 0) or 0)

        if video_count:
            lines.extend(
                [
                    "",
                    f"🎬 Videos detected: {video_count}",
                    "⏳ Video bot processing will be handled in Part 3.",
                ]
            )

        return lines

    def _render_completed_with_errors(self, job: Job) -> list[str]:
        lines: list[str] = [
            "",
            "⚠️ <b>Processing completed with some errors.</b>",
        ]

        upload_results = list(getattr(job, "upload_results", []) or [])
        upload_failures = list(getattr(job, "upload_failures", []) or [])

        if upload_results:
            lines.extend(
                [
                    "",
                    "🔗 <b>Successful uploads:</b>",
                ]
            )
            lines.extend(self._render_upload_results(upload_results))

        if upload_failures:
            lines.extend(
                [
                    "",
                    "❌ <b>Failed uploads:</b>",
                ]
            )
            lines.extend(self._render_upload_failures(upload_failures))

        return lines

    def _render_media_summary(self, job: Job) -> list[str]:
        image_count = int(getattr(job, "image_count", 0) or 0)
        video_count = int(getattr(job, "video_count", 0) or 0)
        ignored_count = int(getattr(job, "ignored_count", 0) or 0)

        lines = [
            "",
            "📊 <b>Media summary</b>",
            f"🖼️ Images: {image_count}",
            f"🎬 Videos: {video_count}",
        ]

        if ignored_count:
            lines.append(f"⏭️ Ignored: {ignored_count}")

        return lines

    def _render_upload_results(
        self,
        results: Iterable[Any],
    ) -> list[str]:
        lines: list[str] = []

        for result in results:
            provider = self._get_value(result, "provider", "unknown")
            url = self._get_value(result, "url", "")
            display_url = self._get_value(result, "display_url", "")
            filename = self._get_value(result, "filename", "")
            path = self._get_value(result, "path", "")

            label = filename or path or "Media"

            if url:
                lines.append(
                    f"• <b>{self._escape(str(label))}</b> "
                    f"({self._escape(str(provider))})\n"
                    f"  {self._escape(str(url))}"
                )
            elif display_url:
                lines.append(
                    f"• <b>{self._escape(str(label))}</b> "
                    f"({self._escape(str(provider))})\n"
                    f"  {self._escape(str(display_url))}"
                )
            else:
                lines.append(
                    f"• <b>{self._escape(str(label))}</b> "
                    f"({self._escape(str(provider))})"
                )

        return lines

    def _render_upload_failures(
        self,
        failures: Iterable[Any],
    ) -> list[str]:
        lines: list[str] = []

        for failure in failures:
            provider = self._get_value(
                failure,
                "provider",
                "unknown",
            )

            filename = self._get_value(
                failure,
                "filename",
                "",
            )

            path = self._get_value(
                failure,
                "path",
                "",
            )

            error = self._get_value(
                failure,
                "error",
                "Unknown upload error",
            )

            label = filename or path or "Media"

            lines.append(
                f"• <b>{self._escape(str(label))}</b> "
                f"({self._escape(str(provider))})"
            )
            lines.append(
                f"  ❌ {self._escape(str(error))}"
            )

        return lines

    @staticmethod
    def _count_successful_image_uploads(
        results: Iterable[Any],
    ) -> int:
        """Count unique image/provider uploads.

        An image can have two successful provider uploads
        (ImgBB + Telegraph), so this method counts the number
        of result entries conservatively rather than assuming
        one result equals one image.
        """

        return sum(
            1
            for result in results
            if str(
                ProgressRenderer._get_value(
                    result,
                    "provider",
                    "",
                )
            ).lower()
            in {"imgbb", "telegraph"}
        )

    @staticmethod
    def _has_media_stats(job: Job) -> bool:
        return any(
            int(getattr(job, field, 0) or 0) > 0
            for field in (
                "image_count",
                "video_count",
                "ignored_count",
            )
        )

    @staticmethod
    def _get_value(
        obj: Any,
        key: str,
        default: Any = None,
    ) -> Any:
        """Read a value from either a dataclass/object or dict."""

        if isinstance(obj, dict):
            return obj.get(key, default)

        return getattr(obj, key, default)

    @staticmethod
    def _escape(value: str) -> str:
        """Escape HTML-sensitive characters for Telegram HTML mode."""

        return (
            str(value)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )


# Backwards-compatible helper.
_default_renderer = ProgressRenderer()


def render_progress(job: Job) -> str:
    """Render a job using the default progress renderer."""

    return _default_renderer.render(job)


def format_progress(job: Job) -> str:
    """Alias kept for callers using the older function name."""

    return _default_renderer.render(job)


def build_progress_message(job: Job) -> str:
    """Alias for compatibility with older pipeline code."""

    return _default_renderer.render(job)
