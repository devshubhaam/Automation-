"""Progress message formatting for the Telegram media processor."""

from __future__ import annotations

import logging
import re
import string

from pathlib import Path
from typing import Any, Iterable, Optional

from .job_manager import Job, JobStatus

logger = logging.getLogger(__name__)

#: Placeholders supported by ``FINAL_POST_TEMPLATE``.
FINAL_POST_VARIABLES = (
    "telegraph_url",
    "video_links",
    "title",
    "archive_name",
    "video_count",
    "image_count",
    "job_id",
)


def normalize_final_post_template(value: Optional[str]) -> Optional[str]:
    """Prepare a raw ``FINAL_POST_TEMPLATE`` value for rendering.

    Escaped ``\\n`` sequences (as typed into a RAW environment editor) become
    real newlines and CRLF is normalised. Empty / whitespace-only values mean
    "not configured" and return ``None``.
    """

    if value is None:
        return None

    text = str(value).replace("\\r\\n", "\n").replace("\\n", "\n")
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    return text if text.strip() else None


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

    def __init__(self, final_post_template: Optional[str] = None) -> None:
        # ``None`` / empty -> the built-in clean final post.
        self.final_post_template = normalize_final_post_template(
            final_post_template
        )

    def render(self, job: Job) -> str:
        """Render the current state of a job."""

        # Finished jobs get the clean final post (Telegraph + DiskWala links).
        clean = self._render_clean_final(job)
        if clean is not None:
            return clean

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
            lines.extend(self._render_cancelled(job))

        if self._has_media_stats(job):
            lines.extend(self._render_media_summary(job))

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # UPLOADING
    # ------------------------------------------------------------------

    def _render_uploading(self, job: Job) -> list[str]:
        """Render upload progress.

        Images go to ImgBB, videos go to the video bot. Telegraph only
        publishes the image article; videos never reach it.
        """

        lines: list[str] = [
            "",
            "☁️ <b>Uploading media...</b>",
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
            done_videos = self._count_videos(results)
            failed_videos = self._count_videos(
                [f for f in failures if not self._is_partial(f)]
            )

            lines.extend(
                [
                    "",
                    f"🎬 Videos: {min(done_videos + failed_videos, video_count)}/{video_count}",
                ]
            )

            if done_videos:
                lines.append(f"✅ Video links received: {done_videos}")

            if failed_videos:
                lines.append(f"❌ Failed videos: {failed_videos}")

        lines.extend(self._render_image_files(job, results, failures))
        lines.extend(self._render_video_files(job))

        if image_count == 0 and video_count == 0:
            lines.append("No media detected.")

        return lines

    # ------------------------------------------------------------------
    # PER-FILE LINES
    # ------------------------------------------------------------------

    MAX_FILE_LINES = 12

    def _render_image_files(
        self,
        job: Job,
        results: list[Any],
        failures: list[Any],
    ) -> list[str]:
        """``name → ImgBB`` / ``name → skipped`` lines (provider-accurate)."""

        entries: list[str] = []

        for result in results:
            if str(self._get_value(result, "provider", "")).lower() == "imgbb":
                entries.append(
                    f"{self._escape(self._get_value(result, 'filename', ''))} → ImgBB ✅"
                )

        for failure in failures:
            if str(self._get_value(failure, "provider", "")).lower() == "imgbb":
                entries.append(
                    f"{self._escape(self._get_value(failure, 'filename', ''))} → ❌ ImgBB failed"
                )

        metadata = getattr(job, "metadata", None) or {}
        for item in metadata.get("part2_skipped_large_images") or []:
            name = item.get("filename", "") if isinstance(item, dict) else str(item)
            entries.append(f"{self._escape(name)} → skipped (&gt;2 MiB)")

        if not entries:
            return []

        return ["", "🖼 <b>Images:</b>"] + self._limit_lines(entries)

    def _render_video_files(self, job: Job) -> list[str]:
        """Per-video status from ``metadata["video_status"]``.

        One short line per video (the single progress message is edited, no
        extra messages): ``1/3 video1.mp4 → ⏳ Processing (video bot) · @Bot``.
        The bot / provider are shown once they are known.
        """

        metadata = getattr(job, "metadata", None) or {}
        statuses = metadata.get("video_status") or []

        if not statuses:
            return []

        total = len(statuses)
        entries: list[str] = []

        for index, entry in enumerate(statuses, start=1):
            name = self._escape(entry.get("filename", ""))
            status = entry.get("status")
            bot = entry.get("bot")
            provider = entry.get("provider")

            links = entry.get("links") or []
            partial = entry.get("partial_errors") or []

            if status == "done":
                text = "✅ URL received"

                names = []
                for link in links:
                    label = self._provider_label(link.get("provider") or link.get("bot") or "")
                    if label and label not in names:
                        names.append(label)
                if not names and provider:
                    names = [self._provider_label(provider)]
                if names:
                    text += f" · {self._escape(', '.join(names))}"
                if partial:
                    text += f" · ⚠️ {len(partial)} bot failed"

            elif status == "failed":
                text = f"❌ {self._escape(str(entry.get('error') or 'failed')[:120])}"

            elif status == "processing":
                text = "⏳ Processing (video bot)"

                if bot:
                    text += f" · @{self._escape(bot)}"
                if links:
                    text += f" · {len(links)} link(s) ready"

            elif status == "merging":
                text = "🔀 Merging videos (no re-encoding)"

            elif status == "cancelled":
                text = "🛑 Cancelled"
            else:
                text = "🕓 Waiting"

            entries.append(f"{index}/{total} {name} → {text}")

        return ["", "🎬 <b>Videos (video bot):</b>"] + self._limit_lines(entries)

    @staticmethod
    def _provider_label(provider: Any) -> str:
        """``diskwala`` -> ``DiskWala`` (unknown providers are title-cased)."""

        labels = {
            "diskwala": "DiskWala",
            "flezen": "Flezen",
            "custom": "Custom",
        }

        key = str(provider or "").lower()

        return labels.get(key, str(provider).title() if provider else "unknown")

    def _limit_lines(self, entries: list[str]) -> list[str]:
        """Keep the message well below Telegram's 4096 character limit."""

        shown = entries[: self.MAX_FILE_LINES]

        if len(entries) > len(shown):
            shown.append(f"… and {len(entries) - len(shown)} more")

        return shown

    # ------------------------------------------------------------------
    # FINAL MESSAGE (COMPLETED / COMPLETED_WITH_ERRORS / CANCELLED)
    # ------------------------------------------------------------------

    #: Telegram rejects messages above 4096 characters; stay well below it so
    #: the final edit can never fail because of its length.
    MAX_MESSAGE_CHARS = 3800
    MAX_REASON_CHARS = 140
    MAX_NAME_CHARS = 90

    #: Items shown per section, reduced step by step until the message fits.
    _CAP_STEPS = (
        {"video": 40, "failed": 20, "skipped": 15, "imgbb": 10},
        {"video": 30, "failed": 15, "skipped": 10, "imgbb": 0},
        {"video": 20, "failed": 10, "skipped": 6, "imgbb": 0},
        {"video": 12, "failed": 6, "skipped": 3, "imgbb": 0},
        {"video": 6, "failed": 4, "skipped": 2, "imgbb": 0},
        {"video": 3, "failed": 2, "skipped": 1, "imgbb": 0},
        {"video": 0, "failed": 0, "skipped": 0, "imgbb": 0},
    )

    # ------------------------------------------------------------------
    # CLEAN FINAL POST
    # ------------------------------------------------------------------

    @staticmethod
    def _is_diskwala_url(url: str) -> bool:
        return bool(re.match(r"https?://(?:www\.)?diskwala\.com/", url.strip(), re.IGNORECASE))

    def _render_clean_final(self, job: Job) -> str | None:
        """The final post of a finished job.

            📝 Telegraph
            <article url>

            🎬 Videos

            <DiskWala link, biggest video first>
            ...

        No header, filenames, ImgBB links, counts or error details. Returns
        ``None`` when the job is not finished or there is nothing to show (the
        detailed report is used then). Links are cut from the END (smallest
        videos) so the post always fits into one Telegram message.
        """

        if job.status not in (JobStatus.COMPLETED, JobStatus.COMPLETED_WITH_ERRORS):
            return None

        results = list(getattr(job, "upload_results", []) or [])
        articles, _images, video_results = self._split_results(results)

        telegraph_url = ""
        for article in articles:
            url = str(self._get_value(article, "url", "") or "").strip()
            if url:
                telegraph_url = url
                break

        # DiskWala links only, biggest original video first (stable for ties).
        indexed: list[tuple[int, int, str]] = []
        seen: set[str] = set()
        for position, result in enumerate(video_results):
            url = str(self._get_value(result, "url", "") or "").strip()
            if not url or url in seen or not self._is_diskwala_url(url):
                continue
            seen.add(url)
            try:
                size = int(self._get_value(result, "size_bytes", 0) or 0)
            except (TypeError, ValueError):
                size = 0
            indexed.append((-size, position, url))
        indexed.sort()
        video_urls = [url for _, _, url in indexed]

        if not telegraph_url and not video_urls:
            return None

        if self.final_post_template:
            custom = self._render_custom_final(
                job, results, telegraph_url, video_urls
            )
            if custom is not None:
                return custom

        return self._build_default_clean(telegraph_url, video_urls)

    def _build_default_clean(
        self, telegraph_url: str, video_urls: list[str]
    ) -> str:
        """The built-in clean final post (used when no template is set)."""

        head: list[str] = []
        if telegraph_url:
            head.extend(["📝 Telegraph", self._escape(telegraph_url)])

        def build(count: int) -> str:
            lines = list(head)
            if video_urls:
                if lines:
                    lines.append("")
                lines.extend(["🎬 Videos", ""])
                shown = [self._escape(u) for u in video_urls[:count]]
                lines.append("\n\n".join(shown))
                if count < len(video_urls):
                    lines.extend(["", f"… +{len(video_urls) - count} more"])
            return "\n".join(lines)

        count = len(video_urls)
        text = build(count)
        while count > 0 and len(text) > self.MAX_MESSAGE_CHARS:
            count -= 1
            text = build(count)
        return text

    def _render_custom_final(
        self,
        job: Job,
        results: list[Any],
        telegraph_url: str,
        video_urls: list[str],
    ) -> str | None:
        """Render ``FINAL_POST_TEMPLATE``; ``None`` -> use the default post.

        ``video_urls`` is already DiskWala-only, de-duplicated and sorted by
        the original ``size_bytes`` (descending, stable), so substitution
        never reorders anything. When the post is too long the links are
        dropped from the END (smallest videos first). Falls back to the
        default post for unknown/invalid placeholders or when even the
        template without links does not fit.
        """

        template = self.final_post_template or ""

        try:
            fields = {
                name
                for _, name, _, _ in string.Formatter().parse(template)
                if name is not None
            }
        except ValueError as exc:
            logger.warning(
                "FINAL_POST_TEMPLATE is malformed (%s); using the default final post.",
                exc,
            )
            return None

        unknown = sorted(fields - set(FINAL_POST_VARIABLES))
        if unknown:
            logger.warning(
                "FINAL_POST_TEMPLATE uses unknown placeholder(s) %s; "
                "using the default final post. Supported: %s.",
                ", ".join("{%s}" % name for name in unknown),
                ", ".join("{%s}" % name for name in FINAL_POST_VARIABLES),
            )
            return None

        archive_name = str(getattr(job, "archive_name", "") or "")
        base_values = {
            "telegraph_url": self._escape(telegraph_url),
            "title": self._escape(Path(archive_name).stem or archive_name or "Media"),
            "archive_name": self._escape(archive_name),
            "image_count": str(
                self._count_successful_images(
                    [r for r in results if self._provider_of(r) == "imgbb"]
                )
            ),
            "job_id": self._escape(str(getattr(job, "job_id", "") or "")),
        }

        def build(count: int) -> str:
            values = dict(base_values)
            values["video_links"] = "\n\n".join(
                self._escape(u) for u in video_urls[:count]
            )
            values["video_count"] = str(count)
            return template.format_map(values)

        try:
            count = len(video_urls)
            text = build(count)
            while count > 0 and len(text) > self.MAX_MESSAGE_CHARS:
                count -= 1
                text = build(count)
        except (KeyError, IndexError, ValueError, AttributeError) as exc:
            logger.warning(
                "FINAL_POST_TEMPLATE could not be rendered (%s); "
                "using the default final post.",
                type(exc).__name__,
            )
            return None

        if len(text) > self.MAX_MESSAGE_CHARS:
            logger.warning(
                "FINAL_POST_TEMPLATE is longer than %d characters even without "
                "video links; using the default final post.",
                self.MAX_MESSAGE_CHARS,
            )
            return None

        return text

    def _render_completed(self, job: Job) -> list[str]:
        return self._render_final(
            job, "🎉 <b>Processing completed successfully.</b>"
        )

    def _render_completed_with_errors(self, job: Job) -> list[str]:
        return self._render_final(
            job, "⚠️ <b>Processing completed with some errors.</b>"
        )

    def _render_cancelled(self, job: Job) -> list[str]:
        return self._render_final(job, "🚫 <b>Job cancelled.</b>")

    def _render_final(self, job: Job, headline: str) -> list[str]:
        """The one final message of a job.

        Contains the Telegraph article URL, every successful video URL with
        its filename, every failed/skipped file with its reason and the overall
        status. It is cut down step by step so that it always fits into one
        Telegram message (the ImgBB link list is the first thing to go; the
        Telegraph article and the video links are the last).
        """

        last: list[str] = []
        for caps in self._CAP_STEPS:
            last = self._build_final(job, headline, caps)
            if self._length(job, last) <= self.MAX_MESSAGE_CHARS:
                return last
        return last

    def _length(self, job: Job, lines: list[str]) -> int:
        """Approximate size of the whole message (header + body + summary)."""

        header = 260 + len(str(job.archive_name))
        summary = 120 if self._has_media_stats(job) else 0
        return header + summary + len("\n".join(lines))

    def _short(self, value: Any, limit: int) -> str:
        text = " ".join(str(value).split())
        if len(text) > limit:
            text = text[: limit - 1] + "…"
        return self._escape(text)

    def _more(self, hidden: int) -> list[str]:
        return [f"… and {hidden} more"] if hidden > 0 else []

    def _build_final(
        self,
        job: Job,
        headline: str,
        caps: dict[str, int],
    ) -> list[str]:
        results = list(getattr(job, "upload_results", []) or [])
        failures = list(getattr(job, "upload_failures", []) or [])
        metadata = getattr(job, "metadata", None) or {}
        skipped = list(metadata.get("part2_skipped_large_images") or [])

        articles, image_results, video_results = self._split_results(results)

        all_video_failures = [f for f in failures if self._provider_of(f) == "video_bot"]
        # A bot that failed while another bot still gave a link is a warning,
        # not a failed video.
        video_failures = [f for f in all_video_failures if not self._is_partial(f)]
        partial_failures = [f for f in all_video_failures if self._is_partial(f)]
        video_groups = self._group_video_links(video_results)
        image_failures = [f for f in failures if self._provider_of(f) == "imgbb"]
        article_failures = [
            f for f in failures if self._provider_of(f) == "telegraph_article"
        ]

        lines: list[str] = ["", headline]

        # ---- overall status ------------------------------------------------
        lines.append("")
        lines.append("📊 <b>Overall:</b>")
        image_count = self._int_value(job, "image_count")
        video_count = self._int_value(job, "video_count")

        if image_count or image_results or image_failures or skipped:
            lines.append(
                f"🖼️ Images: {len(image_results)} uploaded"
                f" · {len(skipped)} skipped · {len(image_failures)} failed"
            )
        if video_count or video_results or video_failures:
            total = video_count or (len(video_groups) + len(video_failures))
            lines.append(
                f"🎬 Videos: {len(video_groups)}/{total} links received"
                f" · {len(video_failures)} failed"
            )
            if partial_failures:
                lines.append(
                    f"⚠️ Video bots: {len(partial_failures)} upload(s) failed"
                    " (other bot links received)"
                )
        if articles:
            lines.append("📝 Telegraph article: created")
        elif article_failures:
            lines.append("📝 Telegraph article: failed")
        elif image_results:
            lines.append("📝 Telegraph article: not created")

        # ---- Telegraph article ----------------------------------------------
        if articles:
            lines.extend(["", "📝 <b>Telegraph article (images):</b>"])
            for article in articles:
                url = self._get_value(article, "url", "")
                if url:
                    lines.append(self._escape(str(url)))

        # ---- successful videos: filename -> URL(s) ---------------------------
        if video_groups:
            lines.extend(["", "🎬 <b>Video links:</b>"])
            shown = video_groups[: caps["video"]]
            for name_raw, group in shown:
                name = self._short(name_raw or "video", self.MAX_NAME_CHARS)
                lines.append(f"• <b>{name}</b>")
                for result in group:
                    provider = self._provider_label_of(result)
                    url = self._escape(str(self._get_value(result, "url", "")))
                    lines.append(f"  {self._escape(provider)}: {url}")
            lines.extend(self._more(len(video_groups) - len(shown)))

        # ---- failures / skips with reasons -----------------------------------
        failed_entries: list[str] = []
        for failure in video_failures:
            failed_entries.append(self._failure_line(failure, "🎬"))
        for failure in partial_failures:
            failed_entries.append(self._failure_line(failure, "⚠️🎬"))
        for failure in article_failures:
            failed_entries.append(self._failure_line(failure, "📝"))
        for failure in image_failures:
            failed_entries.append(self._failure_line(failure, "🖼️"))

        if failed_entries:
            lines.extend(["", "❌ <b>Failed:</b>"])
            shown_failed = failed_entries[: caps["failed"]]
            lines.extend(shown_failed)
            lines.extend(self._more(len(failed_entries) - len(shown_failed)))

        if skipped:
            lines.extend(["", "⏭️ <b>Skipped:</b>"])
            shown_skipped = skipped[: caps["skipped"]]
            for item in shown_skipped:
                lines.append(self._skipped_line(item))
            lines.extend(self._more(len(skipped) - len(shown_skipped)))

        # ---- ImgBB links (lowest priority; dropped first when too long) ------
        if image_results and caps["imgbb"]:
            lines.extend(["", "🔗 <b>ImgBB image links:</b>"])
            shown_images = image_results[: caps["imgbb"]]
            lines.extend(self._render_upload_results(shown_images))
            lines.extend(self._more(len(image_results) - len(shown_images)))
        elif image_results:
            lines.extend(
                [
                    "",
                    f"🔗 {len(image_results)} ImgBB image link(s) are embedded "
                    "in the Telegraph article.",
                ]
            )

        return lines

    def _failure_line(self, failure: Any, icon: str) -> str:
        name = self._short(
            self._get_value(failure, "filename", "")
            or self._get_value(failure, "path", "")
            or "file",
            self.MAX_NAME_CHARS,
        )
        error = self._short(
            self._get_value(failure, "error", "") or "Unknown upload error",
            self.MAX_REASON_CHARS,
        )
        return f"• {icon} <b>{name}</b> — {error}"

    def _skipped_line(self, item: Any) -> str:
        if not isinstance(item, dict):
            return f"• 🖼️ <b>{self._short(item, self.MAX_NAME_CHARS)}</b> — skipped"
        name = self._short(item.get("filename", "") or "image", self.MAX_NAME_CHARS)
        size = item.get("size_bytes")
        limit = item.get("limit_bytes")
        if isinstance(size, int) and isinstance(limit, int) and limit > 0:
            reason = (
                f"over the {limit / (1024 * 1024):.0f} MiB image limit "
                f"({size / (1024 * 1024):.1f} MiB)"
            )
        else:
            reason = "over the 2 MiB image limit"
        return f"• 🖼️ <b>{name}</b> — {reason}"

    @staticmethod
    def _is_partial(failure: Any) -> bool:
        """True for a failure of ONE video bot whose video got another link."""
        extra = ProgressRenderer._get_value(failure, "extra", None)
        return bool(isinstance(extra, dict) and extra.get("partial"))

    def _group_video_links(self, video_results: list[Any]) -> list[tuple[str, list[Any]]]:
        """Group video link results by filename (keeps first-seen order)."""
        groups: dict[str, list[Any]] = {}
        for result in video_results:
            key = str(self._get_value(result, "filename", "") or "video")
            groups.setdefault(key, []).append(result)
        return list(groups.items())

    def _provider_of(self, item: Any) -> str:
        return str(self._get_value(item, "provider", "") or "").lower()

    def _provider_label_of(self, item: Any) -> str:
        label = self._video_provider_name(item, self._get_value(item, "provider", ""))
        return str(label or "video bot")

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

            # A video-bot link: show the provider behind it (e.g. DiskWala).
            provider = self._video_provider_name(result, provider)

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

            provider = self._video_provider_name(failure, provider)

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
    def _split_results(
        results: Iterable[Any],
    ) -> tuple[list[Any], list[Any], list[Any]]:
        """Split results into (Telegraph articles, ImgBB images, video links)."""

        articles: list[Any] = []
        images: list[Any] = []
        videos: list[Any] = []

        for result in results:
            provider = str(
                ProgressRenderer._get_value(result, "provider", "")
            ).lower()

            if provider == "telegraph_article":
                articles.append(result)
            elif provider == "video_bot":
                videos.append(result)
            else:
                images.append(result)

        return articles, images, videos

    @staticmethod
    def _count_videos(items: Iterable[Any]) -> int:
        """Count unique videos among results/failures (provider ``video_bot``)."""

        keys: set[str] = set()

        for item in items:
            provider = str(
                ProgressRenderer._get_value(item, "provider", "")
            ).lower()

            if provider != "video_bot":
                continue

            key = str(
                ProgressRenderer._get_value(item, "filename", "")
                or ProgressRenderer._get_value(item, "path", "")
                or ProgressRenderer._get_value(item, "url", "")
                or ""
            )

            if key:
                keys.add(key)

        return len(keys)

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
    def _video_provider_name(item: Any, provider: Any) -> Any:
        """Real provider name for ``video_bot`` items, otherwise unchanged."""

        if str(provider).lower() != "video_bot":
            return provider

        extra = ProgressRenderer._get_value(item, "extra", None)

        if isinstance(extra, dict):
            name = extra.get("provider_name")

            if name:
                return ProgressRenderer._provider_label(name)

            bot = extra.get("bot")

            if bot:
                return f"@{bot}"

        return provider

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
