"""Application entry point for the Telegram media processor."""

from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path
from typing import Any

from .archive_processor import safe_extract, validate_archive
from .config import ConfigError, Settings
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
from .uploaders.imgbb import (
    ImgBBUploader,
    UploadError as ImgBBUploadError,
)

logger = logging.getLogger("app.main")

HEALTH_HOST = "0.0.0.0"
HEALTH_PORT = 8000


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
    ) -> None:
        self.settings = settings
        self.job_manager = job_manager

        self._queue: asyncio.Queue[Job] = asyncio.Queue()
        self._workers: list[asyncio.Task] = []
        self._stopping = False

        self.progress = ProgressRenderer()

        self.imgbb_uploader = ImgBBUploader(
            api_key=settings.imgbb_api_key,
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

    async def _process(
        self,
        job: Job,
    ) -> None:
        """Run the complete Part 2 processing pipeline."""

        job_id = job.job_id

        try:
            # ==========================================================
            # DOWNLOAD
            # ==========================================================

            self.job_manager.set_status(
                job_id,
                JobStatus.DOWNLOADING,
            )

            await self._notify(job)

            logger.info(
                "Job %s: downloading archive",
                job_id,
            )

            archive_path = await self._download(job)

            logger.info(
                "Job %s: download completed: %s",
                job_id,
                archive_path,
            )

            # ==========================================================
            # VALIDATE
            # ==========================================================

            await self._notify(job)

            logger.info(
                "Job %s: validating archive",
                job_id,
            )

            archive_info = await asyncio.to_thread(
                validate_archive,
                archive_path,
                self.settings,
            )

            self.job_manager.set_metadata(
                job_id,
                "archive_size_bytes",
                archive_info.archive_size_bytes,
            )

            self.job_manager.set_metadata(
                job_id,
                "archive_member_count",
                archive_info.member_count,
            )

            self.job_manager.set_metadata(
                job_id,
                "archive_file_count",
                archive_info.file_count,
            )

            self.job_manager.set_metadata(
                job_id,
                "archive_compression_ratio",
                archive_info.compression_ratio,
            )

            # ==========================================================
            # EXTRACT
            # ==========================================================

            self.job_manager.set_status(
                job_id,
                JobStatus.EXTRACTING,
            )

            await self._notify(job)

            logger.info(
                "Job %s: extracting archive",
                job_id,
            )

            if not job.extract_dir:
                raise RuntimeError(
                    "Job extract directory is not configured"
                )

            extraction_result = await asyncio.to_thread(
                safe_extract,
                archive_path,
                Path(job.extract_dir),
                self.settings,
            )

            self.job_manager.set_extraction_stats(
                job_id,
                file_count=extraction_result.extracted_files,
                total_size_bytes=extraction_result.total_bytes,
            )

            logger.info(
                "Job %s: extracted %s files (%s bytes)",
                job_id,
                extraction_result.extracted_files,
                extraction_result.total_bytes,
            )

            # ==========================================================
            # SCAN
            # ==========================================================

            self.job_manager.set_status(
                job_id,
                JobStatus.SCANNING,
            )

            await self._notify(job)

            logger.info(
                "Job %s: scanning media",
                job_id,
            )

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

            logger.info(
                "Job %s: scan completed "
                "(images=%s videos=%s ignored=%s)",
                job_id,
                scan_result.image_count,
                scan_result.video_count,
                scan_result.ignored_count,
            )

            self.job_manager.set_metadata(
                job_id,
                "scan_counts",
                scan_result.counts(),
            )

            self.job_manager.set_metadata(
                job_id,
                "detected_images",
                [
                    media.to_dict()
                    for media in scan_result.images
                ],
            )

            self.job_manager.set_metadata(
                job_id,
                "detected_videos",
                [
                    media.to_dict()
                    for media in scan_result.videos
                ],
            )

            # ==========================================================
            # IMAGE UPLOAD
            # ==========================================================

            if scan_result.image_count > 0:
                self.job_manager.set_status(
                    job_id,
                    JobStatus.UPLOADING,
                )

                await self._notify(job)

                await self._upload_images(
                    job,
                    scan_result.images,
                )

            # ==========================================================
            # VIDEO — PART 3
            # ==========================================================

            if scan_result.video_count > 0:
                logger.info(
                    "Job %s: detected %s video(s); "
                    "video-bot processing reserved for Part 3",
                    job_id,
                    scan_result.video_count,
                )

                self.job_manager.set_metadata(
                    job_id,
                    "video_processing",
                    "pending_part3",
                )

                self.job_manager.set_metadata(
                    job_id,
                    "video_files",
                    [
                        media.to_dict()
                        for media in scan_result.videos
                    ],
                )

            else:
                self.job_manager.set_metadata(
                    job_id,
                    "video_processing",
                    "not_required",
                )

            # ==========================================================
            # FINAL STATUS
            # ==========================================================

            if job.upload_failures:
                self.job_manager.complete_with_errors(
                    job_id,
                )
            else:
                self.job_manager.complete(
                    job_id,
                )

            await self._notify(job)

            logger.info(
                "Job %s finished with status=%s",
                job_id,
                job.status,
            )

        except asyncio.CancelledError:
            logger.info(
                "Job %s: processing task cancelled",
                job_id,
            )
            raise

        except Exception as exc:
            logger.exception(
                "Job %s: processing failed",
                job_id,
            )

            try:
                current_job = self.job_manager.require_job(
                    job_id,
                )

                if not current_job.is_terminal():
                    self.job_manager.set_error(
                        job_id,
                        f"{type(exc).__name__}: {exc}",
                        failed=True,
                    )

                await self._notify(
                    current_job,
                )

            except Exception:
                logger.exception(
                    "Job %s: could not update failure state",
                    job_id,
                )

        finally:
            self.job_manager.remove_from_queue(
                job_id,
            )

            if not self.settings.keep_job_files:
                self.job_manager.cleanup_job_files(
                    job_id,
                )

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
    # IMAGE UPLOAD
    # ------------------------------------------------------------------

    async def _upload_images(
        self,
        job: Job,
        images: list[Any],
    ) -> None:
        """Upload detected images to ImgBB."""

        for media in images:
            if job.cancel_requested:
                raise asyncio.CancelledError()

            image_path = Path(
                media.path,
            )

            filename = getattr(
                media,
                "filename",
                image_path.name,
            )

            # ----------------------------------------------------------
            # ImgBB
            # ----------------------------------------------------------

            try:
                logger.info(
                    "Job %s: uploading image to ImgBB: %s",
                    job.job_id,
                    filename,
                )

                response = await self.imgbb_uploader.upload(
                    image_path,
                )

                result = UploadResult(
                    provider="imgbb",
                    media_type="image",
                    filename=str(filename),
                    url=str(
                        response.get("url", "")
                    ),
                    display_url=response.get(
                        "display_url"
                    ),
                    provider_id=response.get(
                        "id"
                    ),
                    size_bytes=response.get(
                        "size"
                    ),
                    width=response.get(
                        "width"
                    ),
                    height=response.get(
                        "height"
                    ),
                    extra={
                        "path": str(image_path),
                        "relative_path": getattr(
                            media,
                            "relative_path",
                            "",
                        ),
                    },
                )

                self.job_manager.add_upload_result(
                    job.job_id,
                    result,
                )

            except ImgBBUploadError as exc:
                logger.warning(
                    "Job %s: ImgBB upload failed for %s: %s",
                    job.job_id,
                    filename,
                    exc,
                )

                self.job_manager.add_upload_failure(
                    job.job_id,
                    UploadFailure(
                        provider="imgbb",
                        media_type="image",
                        filename=str(filename),
                        error=str(exc),
                        extra={
                            "path": str(image_path),
                        },
                    ),
                )

            except Exception as exc:
                logger.exception(
                    "Job %s: unexpected ImgBB error for %s",
                    job.job_id,
                    filename,
                )

                self.job_manager.add_upload_failure(
                    job.job_id,
                    UploadFailure(
                        provider="imgbb",
                        media_type="image",
                        filename=str(filename),
                        error=(
                            f"{type(exc).__name__}: {exc}"
                        ),
                        extra={
                            "path": str(image_path),
                        },
                    ),
                )

            await self._notify(job)

    # ------------------------------------------------------------------
    # PROGRESS
    # ------------------------------------------------------------------

    async def _notify(
        self,
        job: Job,
    ) -> None:
        """Edit the Telegram status message."""

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
            )

        except Exception:
            # Telegram edit errors must never kill the media pipeline.
            logger.debug(
                "Could not update progress message for job %s",
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

        try:
            await self.userbot.stop()
        except Exception:
            logger.exception(
                "Userbot shutdown failed"
            )

        try:
            await self.pipeline.stop()
        except Exception:
            logger.exception(
                "Pipeline shutdown failed"
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
        "Starting Telegram Media Processor (PART 2)"
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
