"""Unit tests for app.config, app.progress and the Telegram message helpers.

No credentials and no network are required: Telethon is only imported for the
pure helper functions (``is_zip_document`` / ``document_filename``), and the
progress renderer is exercised through a fake in-memory message editor.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app.config import ConfigError, Settings
from app.job_manager import JobManager, JobStatus
from app.progress import ProgressReporter, render_progress
from app.telegram_client import document_filename, is_zip_document

VALID_HASH = "0123456789abcdef0123456789abcdef"


def make_settings(tmp_path, **overrides) -> Settings:
    values = dict(
        api_id=12345,
        api_hash=VALID_HASH,
        session_name="unit",
        session_dir=tmp_path / "sessions",
        download_dir=tmp_path / "downloads",
        job_dir=tmp_path / "jobs",
        log_dir=tmp_path / "logs",
    )
    values.update(overrides)
    return Settings(**values)


class TestConfig:
    def test_from_env_reads_and_converts(self, tmp_path, monkeypatch):
        env = tmp_path / ".env"
        env.write_text(
            "\n".join(
                [
                    "API_ID=777777",
                    f"API_HASH={VALID_HASH}",
                    "SESSION_NAME=from_env",
                    "MAX_ARCHIVE_SIZE_MB=250",
                    "MAX_EXTRACTED_SIZE_MB=900",
                    "MAX_FILES_PER_ARCHIVE=42",
                    "LOG_LEVEL=debug",
                    "KEEP_JOB_FILES=true",
                ]
            ),
            encoding="utf-8",
        )
        for key in ("API_ID", "API_HASH", "SESSION_NAME", "MAX_ARCHIVE_SIZE_MB"):
            monkeypatch.delenv(key, raising=False)

        settings = Settings.from_env(env)
        assert settings.api_id == 777777
        assert settings.session_name == "from_env"
        assert settings.max_archive_size_mb == 250
        assert settings.max_extracted_size_mb == 900
        assert settings.max_files_per_archive == 42
        assert settings.log_level == "DEBUG"
        assert settings.keep_job_files is True
        assert settings.max_archive_size_bytes == 250 * 1024 * 1024

    def test_missing_api_id_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("API_ID", raising=False)
        monkeypatch.delenv("API_HASH", raising=False)
        empty = tmp_path / ".env"
        empty.write_text("", encoding="utf-8")
        with pytest.raises(ConfigError):
            Settings.from_env(empty)

    def test_non_numeric_api_id_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv("API_ID", "not-a-number")
        monkeypatch.setenv("API_HASH", VALID_HASH)
        with pytest.raises(ConfigError):
            Settings.from_env(tmp_path / "missing.env")

    def test_invalid_log_level_raises(self, tmp_path):
        with pytest.raises(ConfigError):
            make_settings(tmp_path, log_level="LOUD")

    def test_repr_redacts_api_hash(self, tmp_path):
        settings = make_settings(tmp_path)
        assert VALID_HASH not in repr(settings)
        assert "redacted" in repr(settings)

    def test_ensure_directories(self, tmp_path):
        settings = make_settings(tmp_path)
        settings.ensure_directories()
        for directory in (
            settings.session_dir,
            settings.download_dir,
            settings.job_dir,
            settings.log_dir,
        ):
            assert directory.is_dir()


class TestTelegramDocumentHelpers:
    def test_zip_by_filename(self):
        doc = SimpleNamespace(file_name="media.ZIP", mime_type="application/octet-stream")
        assert is_zip_document(doc) is True

    def test_zip_by_mime(self):
        doc = SimpleNamespace(file_name=None, mime_type="application/zip")
        assert is_zip_document(doc) is True

    @pytest.mark.parametrize(
        "name,mime",
        [
            ("photo.jpg", "image/jpeg"),
            ("video.mp4", "video/mp4"),
            ("document.pdf", "application/pdf"),
            ("hello.txt", "text/plain"),
        ],
    )
    def test_non_zip_documents(self, name, mime):
        assert is_zip_document(SimpleNamespace(file_name=name, mime_type=mime)) is False

    def test_none_document(self):
        assert is_zip_document(None) is False

    def test_document_filename_fallback(self):
        assert document_filename(SimpleNamespace(file_name=None)) == "archive.zip"
        assert document_filename(SimpleNamespace(file_name="a/b/media.zip")) == "media.zip"


class FakeEditor:
    """In-memory stand-in for the Telegram message editor."""

    def __init__(self) -> None:
        self.messages: dict[int, str] = {}
        self.edits = 0
        self._next = 100

    async def send(self, text: str) -> int:
        self._next += 1
        self.messages[self._next] = text
        return self._next

    async def edit(self, message_id: int, text: str) -> None:
        self.edits += 1
        self.messages[message_id] = text


class TestProgressRendering:
    def test_received_message(self, tmp_path):
        manager = JobManager(tmp_path / "jobs")
        job = manager.create_job(archive_name="media.zip")
        text = render_progress(job)
        assert "⏳ New job" in text
        assert job.job_id in text
        assert "Status: RECEIVED" in text

    def test_downloading_message(self, tmp_path):
        manager = JobManager(tmp_path / "jobs")
        job = manager.create_job(archive_name="media.zip")
        manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        assert "📥 Downloading ZIP..." in render_progress(manager.get(job.job_id))

    def test_extracting_message(self, tmp_path):
        manager = JobManager(tmp_path / "jobs")
        job = manager.create_job(archive_name="media.zip")
        manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        manager.set_status(job.job_id, JobStatus.EXTRACTING)
        assert "📦 Extracting archive..." in render_progress(manager.get(job.job_id))

    def test_scanning_message(self, tmp_path):
        manager = JobManager(tmp_path / "jobs")
        job = manager.create_job(archive_name="media.zip")
        manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        manager.set_status(job.job_id, JobStatus.EXTRACTING)
        manager.set_status(job.job_id, JobStatus.SCANNING)
        assert "🔍 Scanning files..." in render_progress(manager.get(job.job_id))

    def test_completed_message_lists_media(self, tmp_path):
        manager = JobManager(tmp_path / "jobs")
        job = manager.create_job(archive_name="media.zip")
        manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        manager.set_status(job.job_id, JobStatus.EXTRACTING)
        manager.set_status(job.job_id, JobStatus.SCANNING)
        manager.update_counts(
            job.job_id,
            {"total_files": 6, "image_files": 3, "video_files": 2, "ignored_files": 1},
        )
        manager.set_status(job.job_id, JobStatus.COMPLETED)
        text = render_progress(
            manager.get(job.job_id),
            images=["Sample/Images/1.jpg", "Sample/Images/2.png"],
            videos=["Sample/Videos/1.mp4"],
            ignored=["Sample/Other/readme.txt"],
        )
        assert "✅ Scan complete" in text
        assert "🖼 Images: 3" in text
        assert "🎬 Videos: 2" in text
        assert "📄 Ignored: 1" in text
        assert "PART 1 pipeline completed successfully." in text

    def test_failed_message_hides_traceback(self, tmp_path):
        manager = JobManager(tmp_path / "jobs")
        job = manager.create_job(archive_name="media.zip")
        manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        manager.fail(job.job_id, "The archive contains an unsafe file path.")
        text = render_progress(manager.get(job.job_id), detail="ZIP extraction")
        assert "❌ Processing failed" in text
        assert "Traceback" not in text
        assert "Stage: ZIP extraction" in text

    def test_cancelled_message(self, tmp_path):
        manager = JobManager(tmp_path / "jobs")
        job = manager.create_job(archive_name="media.zip")
        manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        manager.cancel(job.job_id)
        assert "🛑 Job cancelled" in render_progress(manager.get(job.job_id))

    def test_message_length_within_telegram_limit(self, tmp_path):
        manager = JobManager(tmp_path / "jobs")
        job = manager.create_job(archive_name="media.zip")
        manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        manager.set_status(job.job_id, JobStatus.EXTRACTING)
        manager.set_status(job.job_id, JobStatus.SCANNING)
        manager.set_status(job.job_id, JobStatus.COMPLETED)
        text = render_progress(
            manager.get(job.job_id),
            images=[f"dir/image{i}.jpg" for i in range(500)],
            videos=[f"dir/video{i}.mp4" for i in range(500)],
        )
        assert len(text) <= 4096


class TestProgressReporter:
    def test_sends_once_then_edits(self, tmp_path):
        manager = JobManager(tmp_path / "jobs")
        job = manager.create_job(archive_name="media.zip")
        editor = FakeEditor()
        reporter = ProgressReporter(editor)

        async def scenario():
            await reporter.start(job)
            await reporter.update(manager.get(job.job_id), detail="one")
            manager.set_status(job.job_id, JobStatus.DOWNLOADING)
            await reporter.update(manager.get(job.job_id))
            await reporter.update(manager.get(job.job_id))  # identical -> no edit

        asyncio.run(scenario())
        assert len(editor.messages) == 1  # one message reused
        assert editor.edits >= 2

    def test_editor_failure_does_not_crash(self, tmp_path):
        manager = JobManager(tmp_path / "jobs")
        job = manager.create_job(archive_name="media.zip")

        class ExplodingEditor(FakeEditor):
            async def edit(self, message_id, text):
                raise RuntimeError("flood wait")

        reporter = ProgressReporter(ExplodingEditor())

        async def scenario():
            await reporter.start(job)
            await reporter.update(job, detail="x")

        asyncio.run(scenario())  # must not raise
