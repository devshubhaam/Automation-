"""Application entry point for the Telegram media processor."""

from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path
from typing import Any, Optional

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
from .telegram_client import TelegramUserbot
from .uploaders.imgbb import (
    ImgBBUploader,
    UploadError as ImgBBUploadError,
)
from .uploaders.telegraph import (
    TelegraphUploader,
    UploadError as TelegraphUploadError,
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
    """
    Background worker for archive processing.

    Part 2 workflow:

        Telegram ZIP
            ↓
        Download
            ↓
        Validate ZIP
            ↓
        Extract
            ↓
        Scan
            ↓
        Images
          ├── ImgBB
          └── Telegraph
            ↓
        Complete

    Videos are ONLY detected here.

    Video → target Telegram bot is intentionally deferred to Part 3.
    """

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

        # ------------------------------------------------------------------
        # Part 2 uploaders
        # ------------------------------------------------------------------

        self.imgbb_uploader = ImgBBUploader(
            api_key=settings.imgbb_api_key,
        )

        self.telegraph_uploader = TelegraphUploader()

    # ------------------------------------------------------------------
    # QUEUE
    # ------------------------------------------------------------------

    async def submit(
        self,
        job: Job,
    ) -> None:
        """Submit a Job object to the processing queue."""

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
                        getattr(
                            job,
                            "job_id",
                            "unknown",
                        ),
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
    # MAIN PROCESSING
    # ------------------------------------------------------------------

    async def _process(
        self,
        job: Job,
    ) -> None:
        """Run the complete Part 2 archive/image pipeline."""

        job_id = job.job_id

        try:
            # ==============================================================
            # DOWNLOAD
            # ==============================================================

            self.job_manager.set_status(
                job_id,
                JobStatus.DOWNLOADING,
            )

            logger.info(
                "Job %s: downloading archive",
                job_id,
            )

            archive_path = await self._download(
                job,
            )

            logger.info(
                "Job %s: download completed: %s",
                job_id,
                archive_path,
            )

            # ==============================================================
            # VALIDATE ZIP
            # ==============================================================

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

            # ==============================================================
            # EXTRACT
            # ==============================================================

            self.job_manager.set_status(
                job_id,
                JobStatus.EXTRACTING,
            )

            logger.info(
                "Job %s: extracting archive",
                job_id,
            )

            extraction_result = await asyncio.to_thread(
                safe_extract,
                archive_path,
                Path(job.extract_dir),
                self.settings,
            )

            self.job_manager.set_extraction_stats(
                job_id,
                files=extraction_result.extracted_files,
                bytes_total=extraction_result.total_bytes,
            )

            logger.info(
                "Job %s: extracted %s files (%s bytes)",
                job_id,
                extraction_result.extracted_files,
                extraction_result.total_bytes,
            )

            # ==============================================================
            # SCAN
            # ==============================================================

            self.job_manager.set_status(
                job_id,
                JobStatus.SCANNING,
            )

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
                images=scan_result.image_count,
                videos=scan_result.video_count,
                ignored=scan_result.ignored_count,
            )

            logger.info(
                "Job %s: scan completed "
                "(images=%s videos=%s ignored=%s)",
                job_id,
                scan_result.image_count,
                scan_result.video_count,
                scan_result.ignored_count,
            )

            # Save scan information for future Part 3.
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

            # ==============================================================
            # PART 2 — IMAGE UPLOAD
            # ==============================================================

            if scan_result.image_count > 0:
                self.job_manager.set_status(
                    job_id,
                    JobStatus.UPLOADING,
                )

                logger.info(
                    "Job %s: starting image uploads",
                    job_id,
                )

                await self._upload_images(
                    job,
                    scan_result,
                )

            # ==============================================================
            # VIDEOS — PART 3 PLACEHOLDER
            # ==============================================================

            if scan_result.video_count > 0:
                logger.info(
                    "Job %s: detected %s video(s); "
                    "target Telegram bot workflow is "
                    "reserved for Part 3",
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

            # ==============================================================
            # FINAL STATUS
            # ==============================================================

            if job.upload_failures:
                self.job_manager.complete_with_errors(
                    job_id,
                )

                logger.warning(
                    "Job %s completed with %s upload failure(s)",
                    job_id,
                    len(job.upload_failures),
                )

            else:
                self.job_manager.complete(
                    job_id,
                )

                logger.info(
                    "Job %s completed successfully",
                    job_id,
                )

        # ==================================================================
        # CANCELLATION
        # ==================================================================

        except asyncio.CancelledError:
            logger.info(
                "Job %s: worker task cancelled",
                job_id,
            )
            raise

        # ==================================================================
        # FAILURE
        # ==================================================================

        except Exception as exc:
            logger.exception(
                "Job %s: processing failed",
                job_id,
            )

            try:
                current_job = self.job_manager.require_job(
                    job_id,
                )

                if current_job.status not in {
                    JobStatus.COMPLETED,
                    JobStatus.COMPLETED_WITH_ERRORS,
                    JobStatus.FAILED,
                    JobStatus.CANCELLED,
                }:
                    self.job_manager.set_error(
                        job_id,
                        f"{type(exc).__name__}: {exc}",
                        failed=True,
                    )

            except Exception:
                logger.exception(
                    "Job %s: failed to update final error state",
                    job_id,
                )

        # ==================================================================
        # CLEANUP
        # ==================================================================

        finally:
            try:
                self.job_manager.remove_from_queue(
                    job_id,
                )

            except Exception:
                logger.exception(
                    "Job %s: queue bookkeeping failed",
                    job_id,
                )

            if not self.settings.keep_job_files:
                try:
                    self.job_manager.cleanup_job_files(
                        job_id,
                    )

                except Exception:
                    logger.exception(
                        "Job %s: cleanup failed",
                        job_id,
                    )

    # ------------------------------------------------------------------
    # TELEGRAM DOWNLOAD
    # ------------------------------------------------------------------

    async def _download(
        self,
        job: Job,
    ) -> Path:
        """Download the source Telegram document."""

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
                "Downloaded Telegram file does not exist"
            )

        return downloaded_path

    # ------------------------------------------------------------------
    # IMAGE UPLOADS
    # ------------------------------------------------------------------

    async def _upload_images(
        self,
        job: Job,
        scan_result: Any,
    ) -> None:
        """
        Upload each image to both ImgBB and Telegraph.

        IMPORTANT:
        Videos never enter this method.
        """

        image_files = getattr(
            scan_result,
            "images",
            [],
        )

        for media in image_files:
            image_path = self._safe_media_path(
                media,
            )

            if image_path is None:
                filename = getattr(
                    media,
                    "filename",
                    "unknown",
                )

                self.job_manager.add_upload_failure(
                    job.job_id,
                    UploadFailure(
                        provider="local",
                        media_type="image",
                        filename=str(filename),
                        path="",
                        error="Image file is missing or invalid",
                    ),
                )

                continue

            filename = getattr(
                media,
                "filename",
                image_path.name,
            )

            # ==========================================================
            # IMGBB
            # ==========================================================

            try:
                logger.info(
                    "Job %s: ImgBB upload started: %s",
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
                    path=str(image_path),
                    url=str(
                        response.get(
                            "url",
                            "",
                        )
                    ),
                    display_url=response.get(
                        "display_url",
                    ),
                    provider_id=response.get(
                        "id",
                    ),
                    size_bytes=response.get(
                        "size_bytes",
                    ),
                    width=response.get(
                        "width",
                    ),
                    height=response.get(
                        "height",
                    ),
                    metadata=response,
                )

                self.job_manager.add_upload_result(
                    job.job_id,
                    result,
                )

                logger.info(
                    "Job %s: ImgBB upload successful: %s",
                    job.job_id,
                    filename,
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
                        path=str(image_path),
                        error=str(exc),
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
                        path=str(image_path),
                        error=f"{type(exc).__name__}: {exc}",
                    ),
                )

            # ==========================================================
            # TELEGRAPH
            # ==========================================================

            try:
                logger.info(
                    "Job %s: Telegraph upload started: %s",
                    job.job_id,
                    filename,
                )

                response = await self.telegraph_uploader.upload(
                    image_path,
                )

                telegraph_url = str(
                    response.get(
                        "url",
                        "",
                    )
                )

                result = UploadResult(
                    provider="telegraph",
                    media_type="image",
                    filename=str(filename),
                    path=str(image_path),
                    url=telegraph_url,
                    display_url=telegraph_url or None,
                    provider_id=None,
                    size_bytes=response.get(
                        "size_bytes",
                    ),
                    width=response.get(
                        "width",
                    ),
                    height=response.get(
                        "height",
                    ),
                    metadata=response,
                )

                self.job_manager.add_upload_result(
                    job.job_id,
                    result,
                )

                logger.info(
                    "Job %s: Telegraph upload successful: %s",
                    job.job_id,
                    filename,
                )

            except TelegraphUploadError as exc:
                logger.warning(
                    "Job %s: Telegraph upload failed for %s: %s",
                    job.job_id,
                    filename,
                    exc,
                )

                self.job_manager.add_upload_failure(
                    job.job_id,
                    UploadFailure(
                        provider="telegraph",
                        media_type="image",
                        filename=str(filename),
                        path=str(image_path),
                        error=str(exc),
                    ),
                )

            except Exception as exc:
                logger.exception(
                    "Job %s: unexpected Telegraph error for %s",
                    job.job_id,
                    filename,
                )

                self.job_manager.add_upload_failure(
                    job.job_id,
                    UploadFailure(
                        provider="telegraph",
                        media_type="image",
                        filename=str(filename),
                        path=str(image_path),
                        error=f"{type(exc).__name__}: {exc}",
                    ),
                )

    # ------------------------------------------------------------------
    # MEDIA PATH
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_media_path(
        media: Any,
    ) -> Optional[Path]:
        """Extract and validate a detected media path."""

        value = getattr(
            media,
            "path",
            None,
        )

        if value is None:
            return None

        path = Path(
            value,
        )

        try:
            if not path.is_file():
                return None
        except OSError:
            return None

        return path


# ============================================================================
# APPLICATION
# ============================================================================


class Application:
    """Coordinate health server, Telegram clients and pipeline."""

    def __init__(
        self,
        settings: Settings,
    ) -> None:
        self.settings = settings

        self.job_manager: Optional[JobManager] = None
        self.pipeline: Optional[PipelineWorker] = None
        self.userbot: Optional[TelegramUserbot] = None
        self.login_bot: Optional[LoginBot] = None

        self.health_server: Optional[
            asyncio.AbstractServer
        ] = None

        self._stop_event = asyncio.Event()

    # ------------------------------------------------------------------
    # HEALTH SERVER
    # ------------------------------------------------------------------

    async def start_health_server(self) -> None:
        """Start HTTP health server for Koyeb."""

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

    async def stop_health_server(self) -> None:
        """Stop HTTP health server."""

        if self.health_server is None:
            return

        self.health_server.close()

        await self.health_server.wait_closed()

        self.health_server = None

        logger.info(
            "Health server stopped"
        )

    # ------------------------------------------------------------------
    # START
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start all application components."""

        logger.info(
            "Starting Telegram Media Processor (PART 2)"
        )

        self.settings.ensure_directories()

        # Health server must start early for Koyeb.
        await self.start_health_server()

        # --------------------------------------------------------------
        # Job manager
        # --------------------------------------------------------------

        self.job_manager = JobManager(
            self.settings.job_dir,
        )

        # --------------------------------------------------------------
        # Pipeline
        # --------------------------------------------------------------

        self.pipeline = PipelineWorker(
            self.settings,
            self.job_manager,
        )

        # --------------------------------------------------------------
        # Telegram userbot
        # --------------------------------------------------------------

        self.userbot = TelegramUserbot(
            self.settings,
            self.job_manager,
            self.pipeline,
        )

        # --------------------------------------------------------------
        # QR login bot
        # --------------------------------------------------------------

        self.login_bot = LoginBot(
            self.settings,
            self.userbot,
        )

        # --------------------------------------------------------------
        # Start userbot
        # --------------------------------------------------------------

        await self.userbot.start()

        authorized = (
            await self.userbot.client.is_user_authorized()
        )

        if authorized:
            logger.info(
                "Existing Telegram userbot session is authorized"
            )

        else:
            if not self.settings.bot_token:
                raise ConfigError(
                    "BOT_TOKEN is required for first-time QR login"
                )

            if self.settings.bot_owner_id is None:
                raise ConfigError(
                    "BOT_OWNER_ID is required for first-time QR login"
                )

            logger.info(
                "Telegram userbot is not authorized; "
                "starting QR login bot"
            )

            await self.login_bot.start()

            await self.login_bot.wait_until_authorized()

            logger.info(
                "QR authentication completed"
            )

        # --------------------------------------------------------------
        # Start pipeline
        # --------------------------------------------------------------

        await self.pipeline.start()

        logger.info(
            "Telegram Media Processor is ready"
        )

    # ------------------------------------------------------------------
    # RUN
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Run until shutdown is requested."""

        await self.start()

        try:
            await self._stop_event.wait()

        finally:
            await self.stop()

    # ------------------------------------------------------------------
    # STOP
    # ------------------------------------------------------------------

    async def stop(self) -> None:
        """Gracefully stop all components."""

        logger.info(
            "Stopping Telegram Media Processor"
        )

        if self.pipeline is not None:
            await self.pipeline.stop()

        if self.login_bot is not None:
            await self.login_bot.stop()

        if self.userbot is not None:
            await self.userbot.stop()

        await self.stop_health_server()

        logger.info(
            "Telegram Media Processor stopped"
        )

    def request_stop(self) -> None:
        """Request graceful shutdown."""

        self._stop_event.set()


# ============================================================================
# ENTRYPOINT
# ============================================================================


async def async_main() -> None:
    """Async application entry point."""

    settings = Settings.from_env()

    setup_logging(
        settings,
    )

    application = Application(
        settings,
    )

    loop = asyncio.get_running_loop()

    for signal_name in (
        "SIGINT",
        "SIGTERM",
    ):
        try:
            signal_value = getattr(
                signal,
                signal_name,
            )

            loop.add_signal_handler(
                signal_value,
                application.request_stop,
            )

        except (
            AttributeError,
            NotImplementedError,
        ):
            # Restricted runtimes / Windows.
            pass

    await application.run()


def main() -> None:
    """Synchronous entry point."""

    try:
        asyncio.run(
            async_main()
        )

    except KeyboardInterrupt:
        logging.getLogger(
            "app.main"
        ).info(
            "Application interrupted by user"
        )

    except ConfigError:
        logging.getLogger(
            "app.main"
        ).exception(
            "Configuration error"
        )
        raise

    except Exception:
        logging.getLogger(
            "app.main"
        ).exception(
            "Application terminated unexpectedly"
        )
        raise


if __name__ == "__main__":
    main()
