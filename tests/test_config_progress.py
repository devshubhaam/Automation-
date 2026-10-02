"""Unit tests for app.config and app.progress (offline, no credentials)."""

from __future__ import annotations

import pytest

from app.config import ConfigError, Settings
from app.job_manager import JobManager, JobStatus, UploadFailure, UploadResult
from app.progress import ProgressRenderer

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

    def test_imgbb_and_mongodb_settings_from_env(self, tmp_path, monkeypatch):
        for key in ("API_ID", "API_HASH"):
            monkeypatch.delenv(key, raising=False)
        env = tmp_path / ".env"
        env.write_text(
            "\n".join(
                [
                    "API_ID=1",
                    f"API_HASH={VALID_HASH}",
                    "IMGBB_API_KEY=imgbb-secret-value",
                    "MONGODB_DATABASE=mydb",
                    "MONGODB_COLLECTION=mycol",
                ]
            ),
            encoding="utf-8",
        )
        for key in ("IMGBB_API_KEY", "MONGODB_DATABASE", "MONGODB_COLLECTION"):
            monkeypatch.delenv(key, raising=False)

        settings = Settings.from_env(env)

        assert settings.imgbb_api_key == "imgbb-secret-value"
        assert settings.mongodb_database == "mydb"
        assert settings.mongodb_collection == "mycol"
        assert "imgbb-secret-value" not in repr(settings)


def make_job(tmp_path):
    manager = JobManager(tmp_path / "jobs")
    return manager, manager.create_job(user_id=1, chat_id=1, archive_name="a.zip", message_id=1)


class TestProgressRenderer:
    def test_status_stages(self, tmp_path):
        manager, job = make_job(tmp_path)
        renderer = ProgressRenderer()
        assert "ZIP received" in renderer.render(job)
        manager.set_status(job.job_id, JobStatus.QUEUED)
        manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        assert "Downloading" in renderer.render(job)
        manager.set_status(job.job_id, JobStatus.EXTRACTING)
        assert "Extracting" in renderer.render(job)
        manager.set_status(job.job_id, JobStatus.SCANNING)
        assert "Scanning" in renderer.render(job)

    def test_uploading_counts_skipped_and_reserves_videos(self, tmp_path):
        manager, job = make_job(tmp_path)
        for status in (JobStatus.QUEUED, JobStatus.DOWNLOADING, JobStatus.EXTRACTING,
                       JobStatus.SCANNING, JobStatus.UPLOADING):
            manager.set_status(job.job_id, status)
        manager.set_media_counts(job.job_id, image_count=3, video_count=1)
        manager.set_metadata(job.job_id, "part2_skipped_large_images", [{"filename": "big.jpg"}])
        manager.add_upload_result(
            job.job_id,
            UploadResult(media_type="image", filename="a.jpg", provider="imgbb", url="https://x/a"),
        )
        # A Telegraph article is NOT an image upload and must not be counted as one.
        manager.add_upload_result(
            job.job_id,
            UploadResult(media_type="article", filename="a", provider="telegraph_article", url="https://telegra.ph/a"),
        )
        text = ProgressRenderer().render(job)
        assert "Images: 2/3" in text  # one uploaded (counted once) + one skipped
        assert "Skipped" in text
        assert "reserved for Part 3" in text

    def test_completed_shows_article_and_imgbb_links(self, tmp_path):
        manager, job = make_job(tmp_path)
        for status in (JobStatus.QUEUED, JobStatus.DOWNLOADING, JobStatus.EXTRACTING,
                       JobStatus.SCANNING, JobStatus.UPLOADING):
            manager.set_status(job.job_id, status)
        manager.add_upload_result(
            job.job_id,
            UploadResult(media_type="image", filename="a.jpg", provider="imgbb", url="https://i.ibb.co/a.jpg"),
        )
        manager.add_upload_result(
            job.job_id,
            UploadResult(media_type="article", filename="a", provider="telegraph_article",
                         url="https://telegra.ph/a-10-03"),
        )
        manager.complete(job.job_id)
        text = ProgressRenderer().render(job)
        assert "Telegraph article" in text and "https://telegra.ph/a-10-03" in text
        assert "https://i.ibb.co/a.jpg" in text
        assert text.index("https://telegra.ph/a-10-03") < text.index("https://i.ibb.co/a.jpg")

    def test_completed_with_errors_and_cancelled(self, tmp_path):
        manager, job = make_job(tmp_path)
        for status in (JobStatus.QUEUED, JobStatus.DOWNLOADING):
            manager.set_status(job.job_id, status)
        manager.add_upload_failure(
            job.job_id,
            UploadFailure(media_type="image", filename="a.jpg", provider="imgbb", error="boom"),
        )
        manager.mark_cancelled(job.job_id)
        assert "cancelled" in ProgressRenderer().render(job).lower()

    def test_failed_message_has_no_traceback(self, tmp_path):
        manager, job = make_job(tmp_path)
        manager.set_status(job.job_id, JobStatus.QUEUED)
        manager.fail(job.job_id, "bad zip")
        text = ProgressRenderer().render(job)
        assert "bad zip" in text and "Traceback" not in text
