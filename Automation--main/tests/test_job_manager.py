"""Unit tests for app.job_manager (state machine, cancellation, results)."""

from __future__ import annotations

import json

import pytest

from app.job_manager import (
    ALLOWED_TRANSITIONS,
    JobManager,
    JobStatus,
    UploadFailure,
    UploadResult,
)


@pytest.fixture()
def manager(tmp_path) -> JobManager:
    return JobManager(tmp_path / "jobs")


def make_job(manager: JobManager):
    return manager.create_job(user_id=7, chat_id=42, archive_name="media.zip", message_id=1)


def advance(manager, job, *statuses):
    for status in statuses:
        manager.set_status(job.job_id, status)


class TestStatusMachine:
    def test_job_id_format(self, manager):
        parts = manager.generate_job_id().split("-")
        assert parts[0] == "JOB" and len(parts) == 4

    def test_happy_path(self, manager):
        job = make_job(manager)
        advance(manager, job, JobStatus.QUEUED, JobStatus.DOWNLOADING, JobStatus.EXTRACTING,
                JobStatus.SCANNING, JobStatus.UPLOADING, JobStatus.COMPLETED)
        assert job.status == JobStatus.COMPLETED

    def test_illegal_transition_rejected(self, manager):
        job = make_job(manager)
        with pytest.raises(RuntimeError):
            manager.set_status(job.job_id, JobStatus.SCANNING)

    @pytest.mark.parametrize("terminal", [JobStatus.CANCELLED, JobStatus.FAILED])
    def test_terminal_states_cannot_become_active(self, manager, terminal):
        job = make_job(manager)
        manager.set_status(job.job_id, JobStatus.QUEUED)
        manager.set_status(job.job_id, terminal)
        with pytest.raises(RuntimeError):
            manager.set_status(job.job_id, JobStatus.DOWNLOADING)

    def test_every_status_has_transition_entry(self):
        for name in vars(JobStatus):
            if name.isupper():
                assert getattr(JobStatus, name) in ALLOWED_TRANSITIONS


class TestCancellation:
    def test_cancel_active_job_only_sets_flag(self, manager):
        job = make_job(manager)
        advance(manager, job, JobStatus.QUEUED, JobStatus.DOWNLOADING)
        manager.cancel(job.job_id)
        assert job.cancel_requested is True
        assert job.status == JobStatus.DOWNLOADING
        assert not job.is_terminal()

    def test_cancel_queued_job_only_sets_flag(self, manager):
        job = make_job(manager)
        manager.mark_queued(job.job_id)
        manager.cancel(job.job_id)
        assert job.status == JobStatus.QUEUED and job.cancel_requested

    def test_mark_cancelled_is_terminal(self, manager):
        job = make_job(manager)
        advance(manager, job, JobStatus.QUEUED, JobStatus.DOWNLOADING)
        manager.cancel(job.job_id)
        manager.mark_cancelled(job.job_id)
        assert job.status == JobStatus.CANCELLED

    def test_cancelled_never_becomes_completed(self, manager):
        job = make_job(manager)
        manager.mark_queued(job.job_id)
        manager.mark_cancelled(job.job_id)
        manager.complete(job.job_id)
        manager.complete_with_errors(job.job_id)
        assert job.status == JobStatus.CANCELLED
        with pytest.raises(RuntimeError):
            manager.set_status(job.job_id, JobStatus.COMPLETED)

    def test_completed_job_cannot_be_cancelled(self, manager):
        job = make_job(manager)
        advance(manager, job, JobStatus.QUEUED, JobStatus.DOWNLOADING, JobStatus.EXTRACTING,
                JobStatus.SCANNING, JobStatus.COMPLETED)
        manager.cancel(job.job_id)
        manager.mark_cancelled(job.job_id)
        assert job.status == JobStatus.COMPLETED and not job.cancel_requested


class TestResults:
    def test_results_failures_and_metadata_serialise(self, manager):
        job = make_job(manager)
        manager.add_upload_result(job.job_id, UploadResult("image", "a.jpg", "imgbb", "https://i/a"))
        manager.add_upload_result(job.job_id, UploadResult("image", "a.jpg", "telegraph", "https://t/a"))
        manager.add_upload_failure(job.job_id, UploadFailure("image", "b.jpg", "imgbb", "boom"))
        manager.set_metadata(job.job_id, "video_processing", "pending_part3")
        data = json.loads(json.dumps(job.to_dict()))
        assert [(r["filename"], r["provider"]) for r in data["upload_results"]] == [
            ("a.jpg", "imgbb"), ("a.jpg", "telegraph")]
        assert data["upload_failures"][0]["provider"] == "imgbb"
        assert data["metadata"]["video_processing"] == "pending_part3"

    def test_no_internal_queue(self, manager):
        for name in ("enqueue", "get_next_job", "queue_size", "remove_from_queue"):
            assert not hasattr(manager, name)
