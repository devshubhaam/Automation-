"""Unit tests for app.job_manager (creation, state machine, cleanup)."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.job_manager import (
    ALLOWED_TRANSITIONS,
    JobManager,
    JobStateError,
    JobStatus,
    generate_job_id,
)


@pytest.fixture()
def manager(tmp_path: Path) -> JobManager:
    return JobManager(tmp_path / "jobs")


def make_job(manager: JobManager, name: str = "media.zip"):
    return manager.create_job(archive_name=name, source_message_id=1, chat_id=42, sender_id=7)


class TestJobCreation:
    def test_creates_job_directory_structure(self, manager):
        job = make_job(manager)
        assert Path(job.job_directory).is_dir()
        assert job.archive_dir.is_dir()
        assert job.extracted_dir.is_dir()
        assert job.status is JobStatus.RECEIVED
        assert job.metadata_path.exists()

    def test_job_ids_are_unique(self, manager):
        ids = {make_job(manager).job_id for _ in range(50)}
        assert len(ids) == 50

    def test_job_id_format(self):
        job_id = generate_job_id()
        assert job_id.startswith("JOB-")
        parts = job_id.split("-")
        assert len(parts) == 4
        assert len(parts[3]) == 4

    def test_archive_name_is_sanitised(self, manager):
        job = manager.create_job(archive_name="../../etc/passwd.zip")
        assert "/" not in job.archive_name

    def test_metadata_has_no_secrets(self, manager):
        job = make_job(manager)
        text = job.metadata_path.read_text(encoding="utf-8")
        for forbidden in ("api_hash", "API_HASH", "password", "session"):
            assert forbidden not in text

    def test_metadata_round_trip(self, manager):
        job = make_job(manager)
        from app.job_manager import Job

        restored = Job.read_metadata(job.metadata_path)
        assert restored.job_id == job.job_id
        assert restored.status is job.status


class TestStatusMachine:
    def test_happy_path_transitions(self, manager):
        job = make_job(manager)
        manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        manager.set_status(job.job_id, JobStatus.EXTRACTING)
        manager.set_status(job.job_id, JobStatus.SCANNING)
        manager.set_status(job.job_id, JobStatus.COMPLETED)
        assert manager.get(job.job_id).status is JobStatus.COMPLETED
        assert manager.get(job.job_id).finished_at is not None

    def test_illegal_transition_rejected(self, manager):
        job = make_job(manager)
        with pytest.raises(JobStateError):
            manager.set_status(job.job_id, JobStatus.SCANNING)

    def test_terminal_states_are_final(self, manager):
        job = make_job(manager)
        manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        manager.fail(job.job_id, "boom")
        assert manager.get(job.job_id).status is JobStatus.FAILED
        with pytest.raises(JobStateError):
            manager.set_status(job.job_id, JobStatus.SCANNING)

    def test_failed_job_records_reason(self, manager):
        job = make_job(manager)
        manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        manager.fail(job.job_id, "unsafe archive member")
        assert manager.get(job.job_id).error == "unsafe archive member"

    def test_cancelled_job(self, manager):
        job = make_job(manager)
        manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        manager.cancel(job.job_id)
        assert manager.get(job.job_id).status is JobStatus.CANCELLED

    def test_request_cancel_flag(self, manager):
        job = make_job(manager)
        manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        assert manager.request_cancel(job.job_id) is True
        assert manager.get(job.job_id).cancel_requested is True
        assert manager.request_cancel(job.job_id) is True

    def test_request_cancel_on_terminal_returns_false(self, manager):
        job = make_job(manager)
        manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        manager.fail(job.job_id, "x")
        assert manager.request_cancel(job.job_id) is False

    def test_unknown_job_raises(self, manager):
        with pytest.raises(JobStateError):
            manager.get("JOB-does-not-exist")

    def test_every_status_has_a_transition_entry(self):
        for status in JobStatus:
            assert status in ALLOWED_TRANSITIONS

    def test_update_counts(self, manager):
        job = make_job(manager)
        manager.update_counts(
            job.job_id,
            {"total_files": 6, "image_files": 3, "video_files": 2, "ignored_files": 1},
        )
        updated = manager.get(job.job_id)
        assert (updated.image_files, updated.video_files, updated.ignored_files) == (3, 2, 1)


class TestQueueAndLookup:
    def test_queue_fifo(self, manager):
        first = make_job(manager, "a.zip")
        second = make_job(manager, "b.zip")
        manager.enqueue(first.job_id)
        manager.enqueue(second.job_id)
        assert manager.queue_size() == 2
        assert manager.dequeue() == first.job_id
        assert manager.dequeue() == second.job_id
        assert manager.dequeue() is None

    def test_enqueue_sets_queued_status(self, manager):
        job = make_job(manager)
        manager.enqueue(job.job_id)
        assert manager.get(job.job_id).status is JobStatus.QUEUED

    def test_active_job_and_latest_job(self, manager):
        first = make_job(manager, "a.zip")
        second = make_job(manager, "b.zip")
        manager.set_status(first.job_id, JobStatus.DOWNLOADING)
        manager.fail(first.job_id, "x")
        assert manager.active_job().job_id == second.job_id
        assert manager.latest_job().job_id == second.job_id

    def test_active_job_none_when_all_terminal(self, manager):
        job = make_job(manager)
        manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        manager.fail(job.job_id, "x")
        assert manager.active_job() is None

    def test_multiple_jobs_have_independent_directories(self, manager):
        first = make_job(manager, "a.zip")
        second = make_job(manager, "b.zip")
        assert first.job_directory != second.job_directory

    def test_set_status_message(self, manager):
        job = make_job(manager)
        manager.set_status_message(job.job_id, 999)
        assert manager.get(job.job_id).status_message_id == 999


class TestCleanup:
    def test_cleanup_removes_archive_and_extracted_keeps_metadata(self, manager):
        job = make_job(manager)
        (job.archive_dir / "media.zip").write_bytes(b"zip")
        (job.extracted_dir / "img.jpg").write_bytes(b"img")

        removed = manager.cleanup_job_files(job.job_id)

        assert set(removed) == {"archive", "extracted"}
        assert not job.archive_dir.exists()
        assert not job.extracted_dir.exists()
        assert job.metadata_path.exists()

    def test_keep_files_flag_preserves_everything(self, tmp_path):
        manager = JobManager(tmp_path / "jobs", keep_files=True)
        job = make_job(manager)
        (job.extracted_dir / "img.jpg").write_bytes(b"img")
        removed = manager.cleanup_job_files(job.job_id)
        assert removed == []
        assert (job.extracted_dir / "img.jpg").exists()

    def test_cleanup_force_overrides_keep_flag(self, tmp_path):
        manager = JobManager(tmp_path / "jobs", keep_files=True)
        job = make_job(manager)
        (job.extracted_dir / "img.jpg").write_bytes(b"img")
        removed = manager.cleanup_job_files(job.job_id, force=True)
        assert set(removed) == {"archive", "extracted"}
        assert not (job.extracted_dir / "img.jpg").exists()

    def test_cleanup_never_touches_other_jobs(self, manager):
        first = make_job(manager, "a.zip")
        second = make_job(manager, "b.zip")
        (first.extracted_dir / "a.jpg").write_bytes(b"a")
        (second.extracted_dir / "b.jpg").write_bytes(b"b")

        manager.cleanup_job_files(first.job_id)

        assert (second.extracted_dir / "b.jpg").exists()
        assert not (first.extracted_dir / "a.jpg").exists()

    def test_cleanup_is_idempotent(self, manager):
        job = make_job(manager)
        manager.cleanup_job_files(job.job_id)
        assert manager.cleanup_job_files(job.job_id) == []
