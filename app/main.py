"""Application entry point for the Telegram media processor."""

from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path
from typing import Any

from .archive_processor import (
    ExtractionCancelledError,
    safe_extract,
    validate_archive,
)
from .config import ConfigError, Settings, resolve_article_title
from .job_manager import (
    Job,
    JobManager,
    JobStatus,
    UploadFailure,
    UploadResult,
)
from .logging_config import setup_logging
from .login_bot import LoginBot
from .media_scanner import scan_directory
from .progress import ProgressRenderer
from .telegram_client import TelegramUserbot
from .uploaders import (
    PART2_IMAGE_MAX_BYTES,
    ImgBBUploader,
    TelegraphPublisher,
    UploadCancelled,
    UploadError,
    MultiVideoBotUploader,
    VideoTimeoutError,
    VideoTooLargeError,
    video_too_large_message,
)


logger = logging.getLogger("app.main")

HEALTH_HOST = "0.0.0.0"
HEALTH_PORT = 8000


class JobCancelled(Exception):
    """Raised inside the worker when a job's cancellation was requested.

    Deliberately NOT ``asyncio.CancelledError``: that would tear down the
    worker task itself instead of just stopping one job.
    """


# ============================================================================
# HEALTH SERVER
# ============================================================================


async def _health_handler(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """Return a minimal HTTP health response."""

    try:
        await reader.read(1024)

        response = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/plain; charset=utf-8\r\n"
            "Content-Length: 2\r\n"
            "Connection: close\r\n"
            "\r\n"
            "OK"
        )

        writer.write(response.encode("ascii"))
        await writer.drain()

    except Exception:
        logger.debug(
            "Health connection handling failed",
            exc_info=True,
        )

    finally:
        writer.close()

        try:
            await writer.wait_closed()
        except Exception:
            pass


# ============================================================================
# PIPELINE WORKER
# ============================================================================


class PipelineWorker:
    """Background worker for archive/media processing."""

    def __init__(
        self,
        settings: Settings,
        job_manager: JobManager,
        *,
        imgbb_uploader: Any = None,
        telegraph_publisher: Any = None,
        video_uploader: Any = None,
    ) -> None:
        self.settings = settings
        self.job_manager = job_manager

        # The PipelineWorker queue is the ONLY processing queue.
        self._queue: asyncio.Queue[Job] = asyncio.Queue()
        self._workers: list[asyncio.Task] = []
        self._stopping = False

        self.progress = ProgressRenderer(
            final_post_template=getattr(settings, "final_post_template", None)
        )

        # Part 2: images go to ImgBB ONLY. Videos must never reach it.
        self.imgbb_uploader = imgbb_uploader or ImgBBUploader(
            api_key=settings.imgbb_api_key,
        )

        # Telegraph never receives files or video links: it only publishes an
        # article that embeds the ImgBB IMAGE links.
        self.telegraph_publisher = telegraph_publisher or TelegraphPublisher(
            access_token=settings.telegraph_access_token,
            author_name=settings.telegraph_author_name,
        )

        # Part 3: videos go ONLY to the configured video uploader bots
        # (VIDEO_BOTS, tried in order) and come back as links stored in the job
        # results/metadata (never in Telegraph, never in ImgBB).
        # Set by Application so the bots can use the userbot client.
        self.telegram_client_provider: Any = None
        self.video_uploader = video_uploader or MultiVideoBotUploader(
            settings.video_bots,
            client_provider=lambda: (
                self.telegram_client_provider()
                if self.telegram_client_provider
                else None
            ),
            timeout_seconds=settings.video_link_timeout_seconds,
            extra_url_pattern=settings.video_url_pattern,
            max_size_bytes=settings.video_max_size_bytes,
            send_attempts=settings.video_bot_send_attempts,
            fallback_on_timeout=settings.video_bot_fallback_on_timeout,
            mode=settings.video_bot_mode,
            require_reply=settings.video_bot_require_reply,
        )

    # ------------------------------------------------------------------
    # QUEUE
    # ------------------------------------------------------------------

    async def submit(
        self,
        job: Job,
    ) -> None:
        """Submit a Job to the actual processing queue."""

        if self._stopping:
            raise RuntimeError(
                "Pipeline worker is stopping"
            )

        await self._queue.put(job)

        logger.info(
            "Job submitted to pipeline: %s",
            job.job_id,
        )

    def queue_size(self) -> int:
        """Number of jobs waiting in the real processing queue."""

        return self._queue.qsize()

    async def start(self) -> None:
        """Start pipeline workers."""

        if self._workers:
            logger.warning(
                "Pipeline workers are already running"
            )
            return

        self._stopping = False

        worker_count = max(
            1,
            int(self.settings.worker_count),
        )

        logger.info(
            "Starting %s pipeline worker(s)",
            worker_count,
        )

        for index in range(worker_count):
            task = asyncio.create_task(
                self._worker_loop(index + 1),
                name=f"pipeline-worker-{index + 1}",
            )

            self._workers.append(task)

    async def stop(self) -> None:
        """Stop pipeline workers."""

        if not self._workers:
            return

        logger.info(
            "Stopping pipeline workers"
        )

        self._stopping = True

        for task in self._workers:
            task.cancel()

        await asyncio.gather(
            *self._workers,
            return_exceptions=True,
        )

        self._workers.clear()

        logger.info(
            "Pipeline workers stopped"
        )

    async def _worker_loop(
        self,
        worker_id: int,
    ) -> None:
        """Continuously process submitted jobs."""

        logger.info(
            "Pipeline worker %s started",
            worker_id,
        )

        while not self._stopping:
            try:
                job = await self._queue.get()

                try:
                    await self._process(job)

                except asyncio.CancelledError:
                    raise

                except Exception:
                    logger.exception(
                        "Unhandled error processing job %s",
                        job.job_id,
                    )

                finally:
                    self._queue.task_done()

            except asyncio.CancelledError:
                break

            except Exception:
                logger.exception(
                    "Pipeline worker %s loop error",
                    worker_id,
                )

        logger.info(
            "Pipeline worker %s stopped",
            worker_id,
        )

    # ------------------------------------------------------------------
    # PROCESS
    # ------------------------------------------------------------------

    def _checkpoint(self, job: Job) -> None:
        """Raise JobCancelled if cancellation was requested for ``job``."""

        if job.cancel_requested:
            raise JobCancelled()

    async def _process(
        self,
        job: Job,
    ) -> None:
        """Run the full pipeline (ZIP -> images + videos) for one job."""

        job_id = job.job_id

        if job.is_terminal():
            logger.info(
                "Job %s is already %s; skipping",
                job_id,
                job.status,
            )
            return

        try:
            # ==========================================================
            # DOWNLOAD
            # ==========================================================

            # Covers jobs cancelled while waiting in the queue: they are
            # never downloaded, extracted or uploaded.
            self._checkpoint(job)

            self.job_manager.set_status(job_id, JobStatus.DOWNLOADING)
            await self._notify(job)

            logger.info("Job %s: downloading archive", job_id)
            archive_path = await self._download(job)
            logger.info("Job %s: download completed", job_id)

            self._checkpoint(job)  # after download

            # ==========================================================
            # VALIDATE
            # ==========================================================

            archive_info = await asyncio.to_thread(
                validate_archive,
                archive_path,
                self.settings,
            )

            self.job_manager.update_metadata(
                job_id,
                {
                    "archive_size_bytes": archive_info.archive_size_bytes,
                    "archive_member_count": archive_info.member_count,
                    "archive_file_count": archive_info.file_count,
                    "archive_compression_ratio": archive_info.compression_ratio,
                },
            )

            # ==========================================================
            # EXTRACT
            # ==========================================================

            self.job_manager.set_status(job_id, JobStatus.EXTRACTING)
            await self._notify(job)

            if not job.extract_dir:
                raise RuntimeError("Job extract directory is not configured")

            logger.info("Job %s: extracting archive", job_id)

            extraction_result = await asyncio.to_thread(
                safe_extract,
                archive_path,
                Path(job.extract_dir),
                self.settings,
                should_cancel=lambda: job.cancel_requested,
            )

            self.job_manager.set_extraction_stats(
                job_id,
                file_count=extraction_result.extracted_files,
                total_size_bytes=extraction_result.total_bytes,
            )

            self._checkpoint(job)  # after extraction

            # ==========================================================
            # SCAN
            # ==========================================================

            self.job_manager.set_status(job_id, JobStatus.SCANNING)
            await self._notify(job)

            logger.info("Job %s: scanning media", job_id)

            scan_result = await asyncio.to_thread(
                scan_directory,
                Path(job.extract_dir),
            )

            self.job_manager.set_media_counts(
                job_id,
                image_count=scan_result.image_count,
                video_count=scan_result.video_count,
                ignored_count=scan_result.ignored_count,
            )

            self.job_manager.update_metadata(
                job_id,
                {
                    "scan_counts": scan_result.counts(),
                    "detected_images": [m.to_dict() for m in scan_result.images],
                    "detected_videos": [m.to_dict() for m in scan_result.videos],
                },
            )

            logger.info(
                "Job %s: scan completed (images=%s videos=%s ignored=%s)",
                job_id,
                scan_result.image_count,
                scan_result.video_count,
                scan_result.ignored_count,
            )

            self._checkpoint(job)  # after scan

            # ==========================================================
            # MEDIA — images -> ImgBB -> Telegraph article (image URLs only),
            # videos -> video bot -> link stored per video filename.
            # ==========================================================

            await self._upload_media(job, scan_result.images, scan_result.videos)

            # ==========================================================
            # FINAL STATUS
            # ==========================================================

            self._checkpoint(job)  # before final completion

            if job.upload_failures:
                self.job_manager.complete_with_errors(job_id)
            else:
                self.job_manager.complete(job_id)

            await self._notify(job)

            logger.info(
                "Job %s finished with status=%s",
                job_id,
                job.status,
            )

        except (JobCancelled, ExtractionCancelledError):
            self.job_manager.mark_cancelled(job_id)
            logger.info("Job %s: stopped after cancellation request", job_id)
            await self._notify(job)

        except asyncio.CancelledError:
            # Application shutdown (the worker task is being cancelled). The
            # job is marked CANCELLED and the single status message is edited
            # one last time, then the cancellation propagates. Temporary video
            # bot handlers were already removed by the uploader's ``finally``.
            logger.info("Job %s: processing task cancelled", job_id)
            try:
                if not job.is_terminal():
                    self.job_manager.set_metadata(
                        job_id, "cancel_reason", "application shutdown"
                    )
                    self.job_manager.mark_cancelled(job_id)
                await asyncio.wait_for(self._notify(job), timeout=5)
            except BaseException:  # noqa: BLE001 - best effort, never block shutdown
                logger.debug(
                    "Job %s: could not publish shutdown state", job_id, exc_info=True
                )
            raise

        except Exception as exc:
            logger.exception("Job %s: processing failed", job_id)

            try:
                if not job.is_terminal():
                    self.job_manager.set_error(
                        job_id,
                        f"{type(exc).__name__}: {exc}",
                        failed=True,
                    )

                await self._notify(job)

            except Exception:
                logger.exception(
                    "Job %s: could not update failure state",
                    job_id,
                )

        finally:
            if not self.settings.keep_job_files:
                self.job_manager.cleanup_job_files(job_id)

    # ------------------------------------------------------------------
    # DOWNLOAD
    # ------------------------------------------------------------------

    async def _download(
        self,
        job: Job,
    ) -> Path:
        """Download ZIP from Telegram."""

        message = getattr(
            job,
            "_telegram_message",
            None,
        )

        if message is None:
            raise RuntimeError(
                "Telegram source message is unavailable"
            )

        if not job.archive_path:
            raise RuntimeError(
                "Job archive path is not configured"
            )

        archive_path = Path(
            job.archive_path,
        )

        archive_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        downloaded = await message.download_media(
            file=str(archive_path),
        )

        if not downloaded:
            raise RuntimeError(
                "Telegram returned no downloaded file"
            )

        downloaded_path = Path(
            downloaded,
        )

        if not downloaded_path.is_file():
            raise RuntimeError(
                "Downloaded file does not exist"
            )

        return downloaded_path

    # ------------------------------------------------------------------
    # IMAGE UPLOAD (Part 2: images only)
    # ------------------------------------------------------------------

    @staticmethod
    def _skipped_entry(media: Any, size: int) -> dict[str, Any]:
        return {
            "filename": str(getattr(media, "filename", Path(media.path).name)),
            "relative_path": getattr(media, "relative_path", ""),
            "size_bytes": size,
            "limit_bytes": PART2_IMAGE_MAX_BYTES,
            "reason": "exceeds_part2_image_limit",
        }

    async def _upload_media(
        self,
        job: Job,
        images: list[Any],
        videos: list[Any],
    ) -> None:
        """Process images and videos.

        Images: ImgBB -> Telegraph article that embeds ONLY the ImgBB URLs.
        Videos: video bot only; the generated links stay in the job
        results/metadata and never reach ImgBB or Telegraph.
        """

        # Image problems (ImgBB / Telegraph) must NEVER stop the videos.
        try:
            image_urls = await self._upload_images(job, images)

            if image_urls:
                self._checkpoint(job)  # before Telegraph article
                await self._publish_article(job, image_urls)
                await self._notify(job)
            else:
                logger.info(
                    "Job %s: no uploaded images, Telegraph article not created",
                    job.job_id,
                )

        except (JobCancelled, asyncio.CancelledError):
            raise

        except Exception as exc:
            logger.exception(
                "Job %s: image/article stage failed; continuing with videos",
                job.job_id,
            )
            self._record_article_failure(
                job,
                Path(job.archive_name).stem or "Media",
                f"{type(exc).__name__}: {exc}",
            )

        await self._upload_videos(job, videos)

    async def _ensure_uploading(self, job: Job) -> None:
        if job.status != JobStatus.UPLOADING:
            self.job_manager.set_status(job.job_id, JobStatus.UPLOADING)
            await self._notify(job)

    async def _upload_images(
        self,
        job: Job,
        images: list[Any],
    ) -> list[str]:
        """Upload eligible images (<= 2 MiB) to ImgBB; return their URLs.

        Images over the limit are skipped (recorded in
        ``metadata["part2_skipped_large_images"]``) and ImgBB is not called
        for them. This intentional skip is not an upload failure.
        """

        eligible: list[tuple[Any, Path, str]] = []
        skipped: list[dict[str, Any]] = []

        for media in images:
            image_path = Path(media.path)
            filename = str(getattr(media, "filename", image_path.name))

            try:
                size = image_path.stat().st_size
            except OSError as exc:
                self._record_failure(job, media, filename, f"Could not stat image: {exc}")
                continue

            if size > PART2_IMAGE_MAX_BYTES:
                skipped.append(self._skipped_entry(media, size))
                logger.info(
                    "Job %s: skipping %s (%s bytes > Part 2 limit)",
                    job.job_id,
                    filename,
                    size,
                )
                continue

            eligible.append((media, image_path, filename))

        self.job_manager.set_metadata(
            job.job_id,
            "part2_skipped_large_images",
            skipped,
        )

        if not eligible:
            return []

        self._checkpoint(job)  # before image uploads
        await self._ensure_uploading(job)

        imgbb_urls: list[str] = []
        imgbb_results: list[dict[str, str]] = []

        for media, image_path, filename in eligible:
            self._checkpoint(job)

            url = await self._upload_one(job, media, image_path, filename)
            if url:
                imgbb_urls.append(url)
                imgbb_results.append(
                    {
                        "filename": filename,
                        "relative_path": str(getattr(media, "relative_path", "")),
                        "url": url,
                    }
                )
                self.job_manager.set_metadata(
                    job.job_id, "imgbb_results", list(imgbb_results)
                )

            await self._notify(job)

        return imgbb_urls

    async def _upload_videos(
        self,
        job: Job,
        videos: list[Any],
    ) -> list[dict[str, Any]]:
        """Send each video to the video bots, STRICTLY one at a time.

        Per video: send -> wait for the reply to exactly that video (up to
        ``VIDEO_LINK_TIMEOUT``, 900 s by default) -> as soon as the URL arrives
        the next video starts. Only after the full timeout is a video marked
        failed ("Link not received"). Every video ends as ``done`` or
        ``failed`` before the job can be completed: the job is
        COMPLETED only when every video got its link, otherwise
        COMPLETED_WITH_ERRORS (the final post then lists the failed videos).

        Every video keeps its own status/result entry keyed by its filename
        (and relative path), so one failure or timeout never affects another
        video's mapping. Results are stored in ``job.upload_results`` and in
        ``metadata["video_results"]`` / ``["video_failures"]`` /
        ``["video_status"]``. Returns the successful ``video_results`` entries.

        Which bot handled a video (and the provider behind its link) is stored
        per video as ``bot`` / ``provider``.
        """

        if not videos:
            self.job_manager.set_metadata(job.job_id, "video_processing", "not_required")
            return []

        statuses: list[dict[str, Any]] = [
            {
                "filename": str(getattr(m, "filename", Path(m.path).name)),
                "relative_path": str(getattr(m, "relative_path", "")),
                "status": "pending",
                "url": None,
                "error": None,
                "bot": None,
                "provider": None,
                "links": [],
                "partial_errors": [],
            }
            for m in videos
        ]
        results: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []

        def publish_metadata() -> None:
            self.job_manager.update_metadata(
                job.job_id,
                {
                    "video_processing": "video_bot",
                    "video_files": [m.to_dict() for m in videos],
                    "video_status": [dict(e) for e in statuses],
                    "video_results": [dict(r) for r in results],
                    "video_failures": [dict(f) for f in failures],
                },
            )

        publish_metadata()

        self._checkpoint(job)  # before video processing
        await self._ensure_uploading(job)

        client = self._telegram_client(job)
        limit = self.settings.video_max_size_bytes

        total_videos = len(videos)
        link_timeout = int(getattr(self.settings, "video_link_timeout_seconds", 900))

        for position, (media, entry) in enumerate(zip(videos, statuses), start=1):
            self._checkpoint(job)  # between videos

            video_path = Path(media.path)
            filename = entry["filename"]
            extra = {
                "path": str(video_path),
                "relative_path": entry["relative_path"],
            }

            entry["status"] = "processing"
            publish_metadata()
            await self._notify(job)

            async def on_sent(bot: str, entry: dict[str, Any] = entry) -> None:
                """The bot has the file: now waiting for the reply with the URL."""
                entry["status"] = "waiting_link"
                entry["bot"] = bot
                publish_metadata()
                await self._notify(job)

            async def on_attempt(
                bot: str,
                index: int,
                total: int,
                entry: dict[str, Any] = entry,
            ) -> None:
                """Show which bot is being tried (edits the progress message)."""
                entry["status"] = "processing"
                entry["bot"] = bot
                publish_metadata()
                await self._notify(job)

            try:
                size = self._video_size(video_path)
                if size > limit:
                    # Never sent to any uploader bot.
                    raise VideoTooLargeError(video_too_large_message(size, limit))

                logger.info(
                    "Job %s: sending video %d/%d to video bot(s): %s",
                    job.job_id, position, total_videos, filename,
                )

                recorded: set[tuple[str, str]] = set()

                def record_link(link: dict[str, Any]) -> None:
                    """Store ONE link of this video (one per video bot)."""
                    url = str(link.get("url") or "")
                    bot = link.get("bot")
                    provider = link.get("provider_name")
                    if not url or (str(bot), url) in recorded:
                        return
                    recorded.add((str(bot), url))

                    if any(r["url"] == url for r in results):
                        logger.warning(
                            "Job %s: video bot returned the same link for several videos (%s)",
                            job.job_id,
                            filename,
                        )

                    link_extra = dict(extra)
                    link_extra["bot"] = bot
                    link_extra["provider_name"] = provider
                    self.job_manager.add_upload_result(
                        job.job_id,
                        UploadResult(
                            media_type="video",
                            filename=filename,
                            provider="video_bot",
                            url=url,
                            size_bytes=size,
                            extra=link_extra,
                        ),
                    )
                    entry["links"].append({"bot": bot, "provider": provider, "url": url})
                    # First link also fills the legacy single-link fields.
                    if not entry.get("url"):
                        entry.update(url=url, bot=bot, provider=provider)
                    results.append(
                        {
                            "filename": filename,
                            "file": filename,
                            "relative_path": entry["relative_path"],
                            "provider": provider,
                            "bot": bot,
                            "url": url,
                            "status": "completed",
                        }
                    )
                    publish_metadata()

                upload_kwargs: dict[str, Any] = {
                    "client": client,
                    "should_cancel": lambda: job.cancel_requested,
                }
                if getattr(self.video_uploader, "supports_progress_callback", False):
                    upload_kwargs["on_attempt"] = on_attempt
                if getattr(self.video_uploader, "supports_link_callback", False):
                    upload_kwargs["on_link"] = record_link
                if getattr(self.video_uploader, "supports_sent_callback", False):
                    upload_kwargs["on_sent"] = on_sent

                response = await self.video_uploader.upload(video_path, **upload_kwargs)

                # Uploaders without the link callback only report at the end.
                response_links = response.get("links")
                if not response_links:
                    response_links = [
                        {
                            "bot": response.get("bot"),
                            "provider_name": response.get("provider_name"),
                            "url": response.get("url"),
                        }
                    ]
                for link in response_links:
                    record_link(link)

                if not entry["links"]:
                    raise UploadError("Video bot returned no link")

                entry.update(status="done", error=None)
                logger.info(
                    "Job %s: URL received for video %d/%d %s (%d link(s))",
                    job.job_id, position, total_videos, filename, len(entry["links"]),
                )

                # Some bots failed while others gave a link: report them, keep
                # the video as successful.
                for bot_failure in response.get("failures") or []:
                    self._record_bot_failure(
                        job, entry, failures, bot_failure, extra
                    )

            except UploadCancelled:
                entry.update(status="cancelled")
                publish_metadata()
                raise JobCancelled()

            except asyncio.CancelledError:
                # Shutdown while this video was in flight (links already
                # received stay stored in the job results).
                entry.update(status="cancelled")
                publish_metadata()
                raise

            except VideoTimeoutError as exc:
                logger.warning(
                    "Job %s: timeout - no URL for video %d/%d %s within %ds",
                    job.job_id, position, total_videos, filename, link_timeout,
                )
                self._fail_video(job, entry, failures, str(exc), extra, exc)

            except UploadError as exc:
                logger.warning(
                    "Job %s: video %d/%d %s failed: %s",
                    job.job_id, position, total_videos, filename, exc,
                )
                self._fail_video(job, entry, failures, str(exc), extra, exc)

            except Exception as exc:
                logger.exception("Job %s: unexpected video bot error for %s", job.job_id, filename)
                self._fail_video(job, entry, failures, f"{type(exc).__name__}: {exc}", extra, exc)

            publish_metadata()
            await self._notify(job)

            if position < total_videos:
                logger.info(
                    "Job %s: video %d/%d finished (%s); moving to next video %d/%d",
                    job.job_id, position, total_videos, entry["status"],
                    position + 1, total_videos,
                )

        # Safety net: a video must never stay "pending"/"processing"/"waiting"
        # while the job is completed - that would hide a missing link.
        for entry in statuses:
            if entry["status"] not in ("done", "failed"):
                logger.warning(
                    "Job %s: video %s ended in state %r without a link; marking it failed",
                    job.job_id, entry["filename"], entry["status"],
                )
                self._fail_video(
                    job,
                    entry,
                    failures,
                    "Link not received",
                    {"path": "", "relative_path": entry["relative_path"]},
                )

        links_received = sum(1 for e in statuses if e["status"] == "done")
        self.job_manager.set_metadata(
            job.job_id,
            "video_summary",
            {
                "total": total_videos,
                "links_received": links_received,
                "failed": total_videos - links_received,
            },
        )
        logger.info(
            "Job %s: video links received for %d/%d videos",
            job.job_id, links_received, total_videos,
        )
        publish_metadata()

        return results

    @staticmethod
    def _video_size(path: Path) -> int:
        """Size of an existing video file (without reading it)."""
        if not path.is_file():
            raise UploadError(f"Video file does not exist: {path.name}")
        try:
            return path.stat().st_size
        except OSError as exc:
            raise UploadError(f"Could not stat video: {exc}") from exc

    @staticmethod
    def _telegram_client(job: Job) -> Any:
        """Client of the message that started the job (the userbot client)."""
        message = getattr(job, "_telegram_message", None)
        return getattr(message, "client", None) if message is not None else None

    def _record_bot_failure(
        self,
        job: Job,
        entry: dict[str, Any],
        failures: list[dict[str, Any]],
        bot_failure: dict[str, Any],
        extra: dict[str, Any],
    ) -> None:
        """One bot failed for a video that still got a link from another bot."""
        bot = str(bot_failure.get("bot") or "")
        reason = str(bot_failure.get("reason") or bot_failure.get("kind") or "failed")
        provider_lookup = getattr(self.video_uploader, "provider_for_bot", None)
        provider = provider_lookup(bot) if callable(provider_lookup) and bot else None

        entry["partial_errors"].append({"bot": bot, "provider": provider, "error": reason})
        failures.append(
            {
                "filename": entry["filename"],
                "file": entry["filename"],
                "relative_path": entry["relative_path"],
                "provider": provider,
                "bot": bot,
                "error": reason,
                "reason": reason,
                "status": "partial",
            }
        )
        failure_extra = dict(extra)
        failure_extra["bot"] = bot
        failure_extra["provider_name"] = provider
        failure_extra["partial"] = True
        self.job_manager.add_upload_failure(
            job.job_id,
            UploadFailure(
                provider="video_bot",
                media_type="video",
                filename=entry["filename"],
                error=f"@{bot}: {reason}" if bot else reason,
                extra=failure_extra,
            ),
        )

    def _fail_video(
        self,
        job: Job,
        entry: dict[str, Any],
        failures: list[dict[str, Any]],
        error: str,
        extra: dict[str, Any],
        exc: BaseException | None = None,
    ) -> None:
        """Record one failed video (status entry, failure list, job failure)."""
        # The last bot that was tried (set by the multi-bot uploader) and the
        # provider that bot is known to use (learned from earlier links).
        bot = getattr(exc, "bot", None) or entry.get("bot")
        provider = None
        provider_lookup = getattr(self.video_uploader, "provider_for_bot", None)
        if callable(provider_lookup) and bot:
            provider = provider_lookup(bot)

        entry.update(status="failed", url=None, error=error, bot=bot, provider=provider)
        failures.append(
            {
                "filename": entry["filename"],
                "file": entry["filename"],
                "relative_path": entry["relative_path"],
                "provider": provider,
                "bot": bot,
                "error": error,
                "reason": error,
                "status": "failed",
            }
        )
        extra = dict(extra)
        extra["bot"] = bot
        extra["provider_name"] = provider
        self.job_manager.add_upload_failure(
            job.job_id,
            UploadFailure(
                provider="video_bot",
                media_type="video",
                filename=entry["filename"],
                error=error,
                extra=extra,
            ),
        )

    def _record_failure(
        self,
        job: Job,
        media: Any,
        filename: str,
        error: str,
    ) -> None:
        self.job_manager.add_upload_failure(
            job.job_id,
            UploadFailure(
                provider="imgbb",
                media_type="image",
                filename=filename,
                error=error,
                extra={"path": str(media.path)},
            ),
        )

    async def _publish_article(
        self,
        job: Job,
        image_urls: list[str],
    ) -> None:
        """Create the Telegraph article(s) from the ImgBB image URLs only."""

        # Fixed title from TELEGRAPH_TITLE; falls back to the archive name.
        title = resolve_article_title(
            getattr(self.settings, "telegraph_title", None), job.archive_name
        )

        try:
            logger.info(
                "Job %s: creating Telegraph article (%s image(s))",
                job.job_id,
                len(image_urls),
            )

            articles = await self.telegraph_publisher.publish(title, image_urls)

            for article in articles:
                self.job_manager.add_upload_result(
                    job.job_id,
                    UploadResult(
                        media_type="article",
                        filename=str(article.get("title") or title),
                        provider="telegraph_article",
                        url=str(article.get("url", "")),
                        extra={
                            "path": article.get("path"),
                            "image_count": article.get("image_count"),
                        },
                    ),
                )

            self.job_manager.set_metadata(
                job.job_id,
                "telegraph_articles",
                [str(a.get("url", "")) for a in articles],
            )

        except UploadError as exc:
            logger.warning("Job %s: Telegraph article failed: %s", job.job_id, exc)
            self._record_article_failure(job, title, str(exc))

        except Exception as exc:
            logger.exception("Job %s: unexpected Telegraph error", job.job_id)
            self._record_article_failure(job, title, f"{type(exc).__name__}: {exc}")

    def _record_article_failure(self, job: Job, title: str, error: str) -> None:
        self.job_manager.add_upload_failure(
            job.job_id,
            UploadFailure(
                provider="telegraph_article",
                media_type="article",
                filename=title,
                error=error,
            ),
        )

    async def _upload_one(
        self,
        job: Job,
        media: Any,
        image_path: Path,
        filename: str,
    ) -> str | None:
        """Upload one image to ImgBB, record the outcome, return its URL."""

        extra = {
            "path": str(image_path),
            "relative_path": getattr(media, "relative_path", ""),
        }

        try:
            logger.info(
                "Job %s: uploading image to imgbb: %s",
                job.job_id,
                filename,
            )

            response = await self.imgbb_uploader.upload(image_path)
            url = str(response.get("url", ""))

            self.job_manager.add_upload_result(
                job.job_id,
                UploadResult(
                    media_type="image",
                    filename=filename,
                    provider="imgbb",
                    url=url,
                    display_url=response.get("display_url"),
                    provider_id=response.get("id"),
                    size_bytes=response.get("size"),
                    width=response.get("width"),
                    height=response.get("height"),
                    extra=extra,
                ),
            )

            return url or None

        except UploadError as exc:
            logger.warning(
                "Job %s: imgbb upload failed for %s: %s",
                job.job_id,
                filename,
                exc,
            )
            error = str(exc)

        except Exception as exc:
            logger.exception(
                "Job %s: unexpected imgbb error for %s",
                job.job_id,
                filename,
            )
            error = f"{type(exc).__name__}: {exc}"

        self.job_manager.add_upload_failure(
            job.job_id,
            UploadFailure(
                provider="imgbb",
                media_type="image",
                filename=filename,
                error=error,
                extra=extra,
            ),
        )
        return None

    # ------------------------------------------------------------------
    # PROGRESS
    # ------------------------------------------------------------------

    async def _notify(
        self,
        job: Job,
    ) -> None:
        """Edit the Telegram status message (the single progress/final message).

        Link previews are disabled so many URLs do not flood the chat. If the
        edit of a FINAL (terminal) message fails, a compact copy is sent as a
        reply so the person still gets the result. Telegram errors never stop
        the media pipeline.
        """

        status_message = getattr(
            job,
            "_status_message",
            None,
        )

        if status_message is None:
            return

        try:
            text = self.progress.render(
                job,
            )

            await status_message.edit(
                text,
                parse_mode="html",
                link_preview=False,
            )
            return

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.debug(
                "Could not update progress message for job %s",
                job.job_id,
                exc_info=True,
            )

        if not job.is_terminal():
            return

        try:
            await status_message.reply(
                self.progress.render(job)[: self.progress.MAX_MESSAGE_CHARS],
                parse_mode="html",
                link_preview=False,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug(
                "Could not send final message for job %s",
                job.job_id,
                exc_info=True,
            )


# ============================================================================
# APPLICATION
# ============================================================================


class Application:
    """Coordinate health server, pipeline, userbot and login bot."""

    def __init__(
        self,
        settings: Settings,
    ) -> None:
        self.settings = settings

        self.job_manager = JobManager(
            settings.job_dir,
        )

        self.pipeline = PipelineWorker(
            settings,
            self.job_manager,
        )

        self.userbot = TelegramUserbot(
            settings,
            self.job_manager,
            self.pipeline,
        )

        # The video bot is contacted with the userbot's own Telegram client.
        self.pipeline.telegram_client_provider = lambda: self.userbot.client

        self.login_bot = LoginBot(
            settings,
            self.userbot,
        )

        self.health_server: asyncio.AbstractServer | None = None

        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        """Start all application components."""

        self.settings.ensure_directories()

        # --------------------------------------------------------------
        # Health server
        # --------------------------------------------------------------

        self.health_server = await asyncio.start_server(
            _health_handler,
            HEALTH_HOST,
            HEALTH_PORT,
        )

        logger.info(
            "Health server listening on %s:%s",
            HEALTH_HOST,
            HEALTH_PORT,
        )

        # --------------------------------------------------------------
        # Pipeline
        # --------------------------------------------------------------

        await self.pipeline.start()

        # --------------------------------------------------------------
        # Userbot
        # --------------------------------------------------------------

        await self.userbot.start()

        if await self.userbot.client.is_user_authorized():
            logger.info(
                "Existing Telegram userbot session is authorized"
            )
        else:
            logger.info(
                "Telegram userbot is waiting for QR login"
            )

        # --------------------------------------------------------------
        # Login bot
        # --------------------------------------------------------------

        await self.login_bot.start()

        logger.info(
            "Telegram Media Processor is ready"
        )

    async def stop(self) -> None:
        """Stop all application components."""

        logger.info(
            "Stopping Telegram Media Processor"
        )

        self._stop_event.set()

        if self.health_server is not None:
            self.health_server.close()

            try:
                await self.health_server.wait_closed()
            except Exception:
                pass

            self.health_server = None

        try:
            await self.login_bot.stop()
        except Exception:
            logger.exception(
                "Login bot shutdown failed"
            )

        # The pipeline stops BEFORE the userbot: running jobs are cancelled
        # (temporary video-bot handlers removed, final message edited) while
        # the Telegram client is still connected.
        try:
            await self.pipeline.stop()
        except Exception:
            logger.exception(
                "Pipeline shutdown failed"
            )

        # Persists/closes the session exactly as before (unchanged).
        try:
            await self.userbot.stop()
        except Exception:
            logger.exception(
                "Userbot shutdown failed"
            )

        logger.info(
            "Telegram Media Processor stopped"
        )

    async def run(self) -> None:
        """Start application and wait for shutdown."""

        await self.start()

        try:
            await self._stop_event.wait()

        finally:
            await self.stop()

    def request_stop(self) -> None:
        """Request graceful application shutdown."""

        self._stop_event.set()


# ============================================================================
# SIGNAL HANDLING
# ============================================================================


def _install_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    application: Application,
) -> None:
    """Install SIGINT/SIGTERM handlers where supported."""

    def handle_signal() -> None:
        logger.info(
            "Shutdown signal received"
        )

        application.request_stop()

    for sig in (
        signal.SIGINT,
        signal.SIGTERM,
    ):
        try:
            loop.add_signal_handler(
                sig,
                handle_signal,
            )

        except (
            NotImplementedError,
            RuntimeError,
        ):
            # Windows/event-loop implementations may not support this.
            logger.debug(
                "Signal handler unavailable for %s",
                sig,
            )


# ============================================================================
# ENTRY POINT
# ============================================================================


async def async_main() -> None:
    """Async application entry point."""

    try:
        settings = Settings.from_env()

    except ConfigError:
        # Logging may not be configured yet, so use stderr/default logger.
        logging.basicConfig(
            level=logging.ERROR,
        )

        logger.exception(
            "Configuration error"
        )

        raise

    setup_logging(
        settings,
    )

    logger.info(
        "Starting Telegram Media Processor"
    )

    application = Application(
        settings,
    )

    loop = asyncio.get_running_loop()

    _install_signal_handlers(
        loop,
        application,
    )

    await application.run()


def main() -> None:
    """Synchronous application entry point."""

    try:
        asyncio.run(
            async_main()
        )

    except KeyboardInterrupt:
        logger.info(
            "Application interrupted by user"
        )


if __name__ == "__main__":
    main()
