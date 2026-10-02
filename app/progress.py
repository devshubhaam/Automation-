"""Progress message formatting for the Telegram media processor."""

from __future__ import annotations

from typing import Any, Iterable

from .job_manager import Job, JobStatus


class ProgressRenderer:
    """Build compact Telegram-friendly progress messages."""

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
            "📦 <b>Media Processor</b>",
            "",
            f"<b>Job:</b> <code>{self._escape(job.job_id)}</code>",
            f"<b>File:</b> {self._escape(job.archive_name)}",
            f"<b>Status:</b> {icon} {status_text}",
        ]

        if job.status == JobStatus.RECEIVED:
            lines.extend(
                [
                    "",
                    "📥 ZIP received.",
                    "Preparing job...",
                ]
            )

        elif job.status == JobStatus.QUEUED:
            lines.extend(
                [
                    "",
                    "⏳ Job added to processing queue...",
                ]
            )

        elif job.status == JobStatus.DOWNLOADING:
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
                    "❌ <b>Processing failed.</b>",
                    "",
                    f"<b>Error:</b> "
                    f"{self._escape(job.error or 'Unknown error')}",
                ]
            )

        elif job.status == JobStatus.CANCELLED:
            lines.extend(
                [
                    "",
                    "🚫 Job cancelled.",
                ]
            )

        if self._has_media_stats(job):
            lines.extend(self._render_media_summary(job))

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # UPLOADING
    # ------------------------------------------------------------------

    def _render_uploading(self, job: Job) -> list[str]:
        """Render image-upload progress.

        Videos are intentionally NOT uploaded to ImgBB or Telegraph.
        They are reserved for Part 3 video-bot processing.
        """

        lines: list[str] = [
            "",
            "☁️ <b>Uploading images...</b>",
        ]

        image_count = self._int_value(job, "image_count")
        video_count = self._int_value(job, "video_count")

        results = list(
            getattr(job, "upload_results", []) or []
        )

        failures = list(
            getattr(job, "upload_failures", []) or []
        )

        successful_images = self._count_successful_images(
            results
        )

        failed_images = self._count_failed_images(
            failures
        )

        skipped_images = self._skipped_large_count(job)

        if image_count:
            completed_images = min(
                successful_images + failed_images + skipped_images,
                image_count,
            )

            lines.append(
                f"🖼️ Images: "
                f"{completed_images}/{image_count}"
            )

            if successful_images:
                lines.append(
                    f"✅ Successful image uploads: "
                    f"{successful_images}"
                )

            if failed_images:
                lines.append(
                    f"❌ Failed image uploads: "
                    f"{failed_images}"
                )

            if skipped_images:
                lines.append(
                    f"⏭️ Skipped (over 2 MiB): "
                    f"{skipped_images}"
                )

        if video_count:
            lines.extend(
                [
                    "",
                    f"🎬 Videos detected: {video_count}",
                    "⏳ Videos are reserved for Part 3 "
                    "(target Telegram bot, not implemented yet).",
                ]
            )

        if image_count == 0 and video_count == 0:
            lines.append("No media detected.")

        return lines

    # ------------------------------------------------------------------
    # COMPLETED
    # ------------------------------------------------------------------

    def _render_completed(self, job: Job) -> list[str]:
        lines: list[str] = [
            "",
            "🎉 <b>Processing completed successfully.</b>",
        ]

        results = list(
            getattr(job, "upload_results", []) or []
        )

        articles, image_results = self._split_articles(results)

        if articles:
            lines.extend(
                [
                    "",
                    "📝 <b>Telegraph article:</b>",
                ]
            )

            lines.extend(
                self._render_upload_results(articles)
            )

        if image_results:
            lines.extend(
                [
                    "",
                    "🔗 <b>ImgBB image links:</b>",
                ]
            )

            lines.extend(
                self._render_upload_results(image_results)
            )

        skipped_images = self._skipped_large_count(job)

        if skipped_images:
            lines.extend(
                [
                    "",
                    f"⏭️ <b>Skipped (over 2 MiB):</b> {skipped_images}",
                ]
            )

        video_count = self._int_value(
            job,
            "video_count",
        )

        if video_count:
            lines.extend(
                [
                    "",
                    f"🎬 <b>Videos detected:</b> {video_count}",
                    "⏳ Videos are reserved for Part 3 "
                    "(not processed in Part 2).",
                ]
            )

        return lines

    # ------------------------------------------------------------------
    # COMPLETED WITH ERRORS
    # ------------------------------------------------------------------

    def _render_completed_with_errors(
        self,
        job: Job,
    ) -> list[str]:
        lines: list[str] = [
            "",
            "⚠️ <b>Processing completed with some errors.</b>",
        ]

        results = list(
            getattr(job, "upload_results", []) or []
        )

        failures = list(
            getattr(job, "upload_failures", []) or []
        )

        articles, image_results = self._split_articles(results)

        if articles:
            lines.extend(
                [
                    "",
                    "📝 <b>Telegraph article:</b>",
                ]
            )

            lines.extend(
                self._render_upload_results(articles)
            )

        if image_results:
            lines.extend(
                [
                    "",
                    "🔗 <b>Successful uploads:</b>",
                ]
            )

            lines.extend(
                self._render_upload_results(image_results)
            )

        if failures:
            lines.extend(
                [
                    "",
                    "❌ <b>Failed uploads:</b>",
                ]
            )

            lines.extend(
                self._render_upload_failures(failures)
            )

        return lines

    # ------------------------------------------------------------------
    # MEDIA SUMMARY
    # ------------------------------------------------------------------

    def _render_media_summary(
        self,
        job: Job,
    ) -> list[str]:
        image_count = self._int_value(
            job,
            "image_count",
        )

        video_count = self._int_value(
            job,
            "video_count",
        )

        ignored_count = self._int_value(
            job,
            "ignored_count",
        )

        lines = [
            "",
            "📊 <b>Media summary</b>",
            f"🖼️ Images: {image_count}",
            f"🎬 Videos: {video_count}",
        ]

        if ignored_count:
            lines.append(
                f"⏭️ Ignored: {ignored_count}"
            )

        return lines

    # ------------------------------------------------------------------
    # RESULTS
    # ------------------------------------------------------------------

    def _render_upload_results(
        self,
        results: Iterable[Any],
    ) -> list[str]:
        lines: list[str] = []

        for result in results:
            provider = self._get_value(
                result,
                "provider",
                "unknown",
            )

            url = self._get_value(
                result,
                "url",
                "",
            )

            display_url = self._get_value(
                result,
                "display_url",
                "",
            )

            filename = self._get_value(
                result,
                "filename",
                "",
            )

            path = self._get_value(
                result,
                "path",
                "",
            )

            label = filename or path or "Media"

            final_url = url or display_url

            if final_url:
                lines.append(
                    f"• <b>{self._escape(str(label))}</b> "
                    f"({self._escape(str(provider))})\n"
                    f"  {self._escape(str(final_url))}"
                )
            else:
                lines.append(
                    f"• <b>{self._escape(str(label))}</b> "
                    f"({self._escape(str(provider))})"
                )

        return lines

    # ------------------------------------------------------------------
    # FAILURES
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # COUNTING
    # ------------------------------------------------------------------

    @staticmethod
    def _split_articles(
        results: Iterable[Any],
    ) -> tuple[list[Any], list[Any]]:
        """Split results into (Telegraph articles, other results)."""

        articles: list[Any] = []
        others: list[Any] = []

        for result in results:
            provider = str(
                ProgressRenderer._get_value(result, "provider", "")
            ).lower()

            if provider == "telegraph_article":
                articles.append(result)
            else:
                others.append(result)

        return articles, others

    @staticmethod
    def _skipped_large_count(job: Job) -> int:
        """Images intentionally skipped for exceeding the Part 2 limit."""

        metadata = getattr(job, "metadata", None) or {}

        return len(metadata.get("part2_skipped_large_images") or [])

    @staticmethod
    def _count_successful_images(
        results: Iterable[Any],
    ) -> int:
        """Count unique images with at least one successful upload.

        Only ImgBB results are image uploads. Telegraph results are
        articles (provider ``telegraph_article``) and are never counted
        as images.
        """

        image_keys: set[str] = set()

        for result in results:
            provider = str(
                ProgressRenderer._get_value(
                    result,
                    "provider",
                    "",
                )
            ).lower()

            if provider != "imgbb":
                continue

            filename = str(
                ProgressRenderer._get_value(
                    result,
                    "filename",
                    "",
                )
                or ""
            )

            path = str(
                ProgressRenderer._get_value(
                    result,
                    "path",
                    "",
                )
                or ""
            )

            # Prefer filename/path as the identity.
            # Fall back to URL if needed.
            key = (
                filename
                or path
                or str(
                    ProgressRenderer._get_value(
                        result,
                        "url",
                        "",
                    )
                    or ""
                )
            )

            if key:
                image_keys.add(key)

        return len(image_keys)

    @staticmethod
    def _count_failed_images(
        failures: Iterable[Any],
    ) -> int:
        """Count unique failed image files."""

        image_keys: set[str] = set()

        for failure in failures:
            provider = str(
                ProgressRenderer._get_value(
                    failure,
                    "provider",
                    "",
                )
            ).lower()

            if provider != "imgbb":
                continue

            filename = str(
                ProgressRenderer._get_value(
                    failure,
                    "filename",
                    "",
                )
                or ""
            )

            path = str(
                ProgressRenderer._get_value(
                    failure,
                    "path",
                    "",
                )
                or ""
            )

            key = filename or path

            if key:
                image_keys.add(key)

        return len(image_keys)

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------

    @staticmethod
    def _int_value(
        job: Job,
        field: str,
    ) -> int:
        try:
            return int(
                getattr(job, field, 0) or 0
            )
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _has_media_stats(
        job: Job,
    ) -> bool:
        return any(
            ProgressRenderer._int_value(
                job,
                field,
            ) > 0
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
        """Read value from dataclass/object or dictionary."""

        if isinstance(obj, dict):
            return obj.get(key, default)

        return getattr(
            obj,
            key,
            default,
        )

    @staticmethod
    def _escape(
        value: str,
    ) -> str:
        """Escape HTML-sensitive characters."""

        return (
            str(value)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )


# ----------------------------------------------------------------------
# Backwards-compatible helpers
# ----------------------------------------------------------------------

_default_renderer = ProgressRenderer()


def render_progress(job: Job) -> str:
    """Render a job using the default renderer."""

    return _default_renderer.render(job)


def format_progress(job: Job) -> str:
    """Compatibility alias."""

    return _default_renderer.render(job)


def build_progress_message(job: Job) -> str:
    """Compatibility alias."""

    return _default_renderer.render(job)
