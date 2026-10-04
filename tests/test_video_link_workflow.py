"""Sequential video -> DiskWala link workflow (VIDEO_LINK_TIMEOUT).

Real ``MultiVideoBotUploader`` + ``PipelineWorker`` + ``ProgressRenderer``; only
Telegram (client / bot replies), ImgBB and Telegraph are fakes.

Covered: strictly one video at a time, the next video starts the moment the URL
arrives, a silent bot fails only THAT video after the timeout (job ->
COMPLETED_WITH_ERRORS, final post lists name + reason + summary), 10/10 links ->
plain COMPLETED, reply-to based URL matching, VIDEO_LINK_TIMEOUT config.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

import pytest

from app.config import ConfigError, Settings
from app.job_manager import JobManager, JobStatus, UploadResult
from app.main import PipelineWorker
from app.progress import ProgressRenderer

from .conftest import build_zip
from .test_part3_multibot import DISK, MultiClient, emit, multi, no_events  # noqa: F401
from .test_part3_video import event
from .test_part4_pipeline import StatusMessage, ZipMessage, bot_behaviour, part4_settings
from .test_part3_video import FakeImgBB, FakePublisher

DW = "https://www.diskwala.com/app/DW-"


def url_of(name: str) -> str:
    return f"{DW}{Path(name).stem}"


def names(count: int) -> list[str]:
    return [f"video{i}.mp4" for i in range(1, count + 1)]


def make_run(settings, tmp_path, video_names, client, *, uploader=None, with_image=False):
    """A ZIP job with the given videos (in order) processed by the real pipeline."""
    manager = JobManager(settings.job_dir)
    uploader = uploader or multi(bots=(DISK,), mode="fallback", timeout_seconds=5)
    worker = PipelineWorker(
        settings, manager, imgbb_uploader=FakeImgBB(), telegraph_publisher=FakePublisher(),
        video_uploader=uploader,
    )
    entries = {f"clips/{n}": bytes([65 + i]) * (20 + i) for i, n in enumerate(video_names)}
    if with_image:
        entries["pics/1.jpg"] = b"\xff\xd8one"
    zip_path = build_zip(tmp_path / "album.zip", entries)
    job = manager.create_job(user_id=1, chat_id=1, archive_name="album.zip", message_id=1)
    job._telegram_message = ZipMessage(zip_path, client)
    job._status_message = StatusMessage()
    manager.mark_queued(job.job_id)
    return worker, manager, job


def ok_for_all(delay: float = 0.0):
    async def behaviour(bot, name, sid, handlers):
        if delay:
            await asyncio.sleep(delay)
        await emit(handlers, event(url_of(name), sid + 1, reply_to=sid))
    return behaviour


# --------------------------------------------------------------------------- #
# 1. Sequential: one video at a time, in ZIP order
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_videos_are_sent_one_at_a_time_in_order_and_all_links_arrive(settings, tmp_path, no_events):
    settings = part4_settings(settings)
    order = ["video3.mp4", "video1.mp4", "video4.mp4", "video2.mp4"]       # NOT alphabetical
    client = MultiClient(ok_for_all(delay=0.05))
    worker, manager, job = make_run(settings, tmp_path, order, client)

    await worker._process(job)

    assert client.max_in_flight == 1                                       # never two videos in flight
    scanned = [v["filename"] for v in job.metadata["detected_videos"]]     # the pipeline's scan order
    assert [name for _, name, _ in client.sent] == scanned and sorted(scanned) == sorted(order)
    assert job.status == JobStatus.COMPLETED and not job.upload_failures
    assert {r["filename"] for r in job.metadata["video_results"]} == set(order)
    assert [s["status"] for s in job.metadata["video_status"]] == ["done"] * 4
    assert job.metadata["video_summary"] == {"total": 4, "links_received": 4, "failed": 0}


@pytest.mark.asyncio
async def test_next_video_starts_immediately_when_the_url_arrives(settings, tmp_path, no_events):
    settings = part4_settings(settings)
    # A 5 minute timeout must not delay anything when the bot answers at once.
    uploader = multi(bots=(DISK,), mode="fallback", timeout_seconds=300)
    client = MultiClient(ok_for_all())
    worker, manager, job = make_run(settings, tmp_path, names(4), client, uploader=uploader)

    started = time.monotonic()
    await worker._process(job)

    assert time.monotonic() - started < 10
    assert job.status == JobStatus.COMPLETED and len(client.sent) == 4


@pytest.mark.asyncio
async def test_a_video_is_only_sent_after_the_previous_link_was_received(settings, tmp_path, no_events):
    settings = part4_settings(settings)
    log: list[str] = []

    async def behaviour(bot, name, sid, handlers):
        log.append(f"sent {name}")
        await asyncio.sleep(0.05)
        log.append(f"reply {name}")
        await emit(handlers, event(url_of(name), sid + 1, reply_to=sid))

    client = MultiClient(behaviour)
    worker, manager, job = make_run(settings, tmp_path, names(3), client)

    await worker._process(job)

    sent_order = [e.split()[1] for e in log if e.startswith("sent")]
    assert log == [x for n in sent_order for x in (f"sent {n}", f"reply {n}")]   # strictly interleaved


@pytest.mark.asyncio
async def test_video_status_is_waiting_for_link_while_the_bot_has_not_replied(settings, tmp_path, no_events):
    settings = part4_settings(settings)
    seen: list[tuple[str, str]] = []
    holder: dict[str, Any] = {}

    async def behaviour(bot, name, sid, handlers):
        status = {s["filename"]: s["status"] for s in holder["job"].metadata["video_status"]}
        seen.append((name, status[name]))
        await emit(handlers, event(url_of(name), sid + 1, reply_to=sid))

    client = MultiClient(behaviour)
    worker, manager, job = make_run(settings, tmp_path, names(2), client)
    holder["job"] = job

    await worker._process(job)

    # The progress message said "waiting for link" once the file was delivered.
    assert any("⏳ Waiting for link" in text for text in job._status_message.edits)
    assert job.status == JobStatus.COMPLETED


# --------------------------------------------------------------------------- #
# 2. Timeout: only that video fails, the others are unaffected
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_silent_bot_fails_only_that_video_after_the_timeout_and_job_continues(settings, tmp_path, no_events):
    settings = part4_settings(settings)
    plan = {(DISK, n): "ok" for n in ("video1.mp4", "video2.mp4", "video4.mp4")}   # video3: silent
    uploader = multi(bots=(DISK,), mode="fallback", timeout_seconds=0.4)
    client = MultiClient(bot_behaviour(plan))
    worker, manager, job = make_run(settings, tmp_path, names(4), client, uploader=uploader, with_image=True)

    started = time.monotonic()
    await worker._process(job)
    elapsed = time.monotonic() - started

    assert elapsed >= 0.4                                                   # the full timeout was waited
    assert [(s["filename"], s["status"]) for s in job.metadata["video_status"]] == [
        ("video1.mp4", "done"), ("video2.mp4", "done"), ("video3.mp4", "failed"), ("video4.mp4", "done")]
    assert [name for _, name, _ in client.sent] == names(4)                # video4 was still sent
    assert job.status == JobStatus.COMPLETED_WITH_ERRORS                   # not FAILED
    assert job.metadata["video_summary"] == {"total": 4, "links_received": 3, "failed": 1}

    final = job._status_message.edits[-1]
    for n in ("video1", "video2", "video4"):
        assert f"{DW}{n}" in final                                          # successful links shown
    assert f"{DW}video3" not in final
    assert "3/4 video3.mp4 → ❌ Link not received (timeout)" in final
    assert "1/4 video1.mp4 → ✅ URL received" in final
    assert final.endswith(
        "📊 Summary:\nImages: 1\nVideos: 4\nVideo links received: 3\nFailed videos: 1"
    )
    assert "Waiting" not in final and "did not reply" not in final


def test_the_timeout_comes_from_video_link_timeout_not_a_hardcoded_value(settings, tmp_path):
    import dataclasses

    configured = dataclasses.replace(settings, video_bots=(DISK,), video_link_timeout_seconds=123)
    worker = PipelineWorker(configured, JobManager(configured.job_dir),
                            imgbb_uploader=FakeImgBB(), telegraph_publisher=FakePublisher())

    assert [u.timeout_seconds for u in worker.video_uploader.uploaders] == [123.0]
    default = dataclasses.replace(settings, video_bots=(DISK,))
    worker = PipelineWorker(default, JobManager(default.job_dir),
                            imgbb_uploader=FakeImgBB(), telegraph_publisher=FakePublisher())
    assert [u.timeout_seconds for u in worker.video_uploader.uploaders] == [900.0]   # 15 minutes


@pytest.mark.asyncio
async def test_progress_logging_covers_send_wait_url_timeout_and_next(settings, tmp_path, no_events, caplog):
    settings = part4_settings(settings)
    plan = {(DISK, "video1.mp4"): "ok"}                                     # video2: silent
    uploader = multi(bots=(DISK,), mode="fallback", timeout_seconds=0.3)
    client = MultiClient(bot_behaviour(plan))
    worker, manager, job = make_run(settings, tmp_path, names(2), client, uploader=uploader)

    with caplog.at_level(logging.INFO):
        await worker._process(job)

    text = caplog.text
    assert "sending video 1/2" in text and "sending video 2/2" in text          # sending
    assert "Waiting for @" in text                                              # waiting for reply
    assert "URL received for video1.mp4" in text                                # URL received
    assert "Timeout: no link from @" in text and "timeout - no URL for video 2/2" in text  # timeout
    assert "moving to next video 2/2" in text                                   # moving to next
    assert "video links received for 1/2 videos" in text


# --------------------------------------------------------------------------- #
# 3. Final status + final post
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_ten_of_ten_links_is_plain_completed_and_the_clean_post_is_unchanged(settings, tmp_path, no_events):
    settings = part4_settings(settings, max_archive_size_mb=50, max_extracted_size_mb=50)
    client = MultiClient(ok_for_all())
    worker, manager, job = make_run(settings, tmp_path, names(10), client)

    await worker._process(job)

    assert job.status == JobStatus.COMPLETED and not job.upload_failures
    final = job._status_message.edits[-1]
    assert "Summary" not in final and "❌" not in final                     # nothing added to the clean post
    assert final.startswith("🎬 Videos\n\n") and final.count("diskwala.com/app/") == 10


@pytest.mark.asyncio
async def test_nine_of_ten_links_is_completed_with_errors_not_failed(settings, tmp_path, no_events):
    settings = part4_settings(settings, max_archive_size_mb=50, max_extracted_size_mb=50)
    plan = {(DISK, n): "ok" for n in names(9)}                              # video10 never answers
    uploader = multi(bots=(DISK,), mode="fallback", timeout_seconds=0.3)
    client = MultiClient(bot_behaviour(plan))
    worker, manager, job = make_run(settings, tmp_path, names(10), client, uploader=uploader, with_image=True)

    await worker._process(job)

    assert job.status == JobStatus.COMPLETED_WITH_ERRORS
    final = job._status_message.edits[-1]
    assert final.count("diskwala.com/app/") == 9
    assert "9/10 video9.mp4 → ✅ URL received" in final
    assert "10/10 video10.mp4 → ❌ Link not received (timeout)" in final
    assert final.endswith(
        "🎬 Videos:\n" + "\n".join(
            f"{i}/10 video{i}.mp4 → ✅ URL received" for i in range(1, 10)
        ) + "\n10/10 video10.mp4 → ❌ Link not received (timeout)\n\n"
        "📊 Summary:\nImages: 1\nVideos: 10\nVideo links received: 9\nFailed videos: 1"
    )
    assert len(final) <= 4096


def test_custom_template_keeps_its_links_and_gets_the_failure_report(tmp_path):
    manager = JobManager(tmp_path / "jobs")
    job = manager.create_job(job_id="j", user_id=1, chat_id=1, archive_name="Album.zip")
    for status in (JobStatus.QUEUED, JobStatus.DOWNLOADING, JobStatus.EXTRACTING,
                   JobStatus.SCANNING, JobStatus.UPLOADING):
        manager.set_status(job.job_id, status)
    manager.add_upload_result(job.job_id, UploadResult(
        media_type="video", filename="a.mp4", provider="video_bot", url=DW + "a",
        size_bytes=10, extra={"provider_name": "diskwala"}))
    manager.set_metadata(job.job_id, "video_status", [
        {"filename": "a.mp4", "status": "done", "error": None},
        {"filename": "b<1>.mp4", "status": "failed",
         "error": "Video bot did not reply with a link within 900s"},
    ])
    manager.complete_with_errors(job.job_id)

    text = ProgressRenderer(final_post_template=r"🎬 Videos\n\n{video_links}").render(job)

    assert text.startswith(f"🎬 Videos\n\n{DW}a\n\n🎬 Videos:\n1/2 a.mp4 → ✅ URL received\n")
    assert "2/2 b&lt;1&gt;.mp4 → ❌ Link not received (timeout)" in text       # escaped, friendly reason
    assert "Video links received: 1" in text and "Failed videos: 1" in text


def test_long_failure_lists_stay_inside_one_telegram_message(tmp_path):
    manager = JobManager(tmp_path / "jobs")
    job = manager.create_job(job_id="j", user_id=1, chat_id=1, archive_name="Album.zip")
    for status in (JobStatus.QUEUED, JobStatus.DOWNLOADING, JobStatus.EXTRACTING,
                   JobStatus.SCANNING, JobStatus.UPLOADING):
        manager.set_status(job.job_id, status)
    statuses = []
    for i in range(1, 121):
        ok = i % 4 != 0
        if ok:
            manager.add_upload_result(job.job_id, UploadResult(
                media_type="video", filename=f"v{i}.mp4", provider="video_bot",
                url=f"{DW}{'x' * 24}{i}", size_bytes=1000 - i, extra={"provider_name": "diskwala"}))
        statuses.append({"filename": f"v{i}.mp4", "status": "done" if ok else "failed",
                         "error": None if ok else "Video bot did not reply with a link within 900s"})
    manager.set_metadata(job.job_id, "video_status", statuses)
    manager.complete_with_errors(job.job_id)

    text = ProgressRenderer().render(job)

    assert len(text) <= ProgressRenderer.MAX_MESSAGE_CHARS
    assert "Videos: 120" in text and "Video links received: 90" in text and "Failed videos: 30" in text
    assert "v4.mp4 → ❌ Link not received (timeout)" in text


# --------------------------------------------------------------------------- #
# 4. Reply based URL extraction
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_only_the_reply_to_the_exact_video_is_accepted(settings, tmp_path, no_events):
    settings = part4_settings(settings)
    client_holder: dict[str, MultiClient] = {}

    async def behaviour(bot, name, sid, handlers):
        previous = [s for _, n, s in client_holder["c"].sent if n != name]
        if name == "video2.mp4":
            # 1) a LATE reply to the earlier (timed-out) video1 with its link,
            if previous:
                await emit(handlers, event(url_of("video1.mp4"), sid + 1, reply_to=previous[0]))
            # 2) chatter that is not a reply at all (newer id, carries a valid link),
            await emit(handlers, event(DW + "RANDOM", sid + 2))
            # 3) a reply to a bot message that is not ours,
            await emit(handlers, event(DW + "OTHER", sid + 3, reply_to=sid + 900))
            await asyncio.sleep(0.02)
            # 4) and finally the real reply to THIS video.
            await emit(handlers, event(url_of(name), sid + 4, reply_to=sid))

    uploader = multi(bots=(DISK,), mode="fallback", timeout_seconds=0.3, require_reply=True)
    client = MultiClient(behaviour)
    client_holder["c"] = client
    worker, manager, job = make_run(settings, tmp_path, names(2), client, uploader=uploader)

    await worker._process(job)

    urls = {r["filename"]: r["url"] for r in job.metadata["video_results"]}
    assert urls == {"video2.mp4": url_of("video2.mp4")}                    # exact video -> exact URL
    assert {s["filename"]: s["status"] for s in job.metadata["video_status"]} == {
        "video1.mp4": "failed", "video2.mp4": "done"}
    assert job.status == JobStatus.COMPLETED_WITH_ERRORS
    assert DW + "RANDOM" not in job._status_message.edits[-1]
    assert DW + "OTHER" not in job._status_message.edits[-1]
    assert DW + "video1" not in job._status_message.edits[-1]


@pytest.mark.asyncio
async def test_link_given_in_a_reply_chain_is_accepted(settings, tmp_path, no_events):
    settings = part4_settings(settings)
    plan = {(DISK, n): "chain" for n in names(2)}                          # placeholder, then link as reply to it
    client = MultiClient(bot_behaviour(plan))
    worker, manager, job = make_run(settings, tmp_path, names(2), client)

    await worker._process(job)

    assert job.status == JobStatus.COMPLETED and len(job.metadata["video_results"]) == 2


# --------------------------------------------------------------------------- #
# 5. Configuration: VIDEO_LINK_TIMEOUT
# --------------------------------------------------------------------------- #


def _env(tmp_path, monkeypatch, lines):
    for key in ("VIDEO_LINK_TIMEOUT", "VIDEO_BOT_TIMEOUT_SECONDS"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("API_ID", "12345")
    monkeypatch.setenv("API_HASH", "0123456789abcdef0123456789abcdef")
    env = tmp_path / ".env"
    env.write_text("\n".join(lines), encoding="utf-8")
    return env


def test_video_link_timeout_defaults_to_900_seconds(tmp_path, monkeypatch):
    assert Settings.from_env(_env(tmp_path, monkeypatch, [])).video_link_timeout_seconds == 900


def test_video_link_timeout_is_read_from_the_environment(tmp_path, monkeypatch):
    settings = Settings.from_env(_env(tmp_path, monkeypatch, ["VIDEO_LINK_TIMEOUT=600"]))
    assert settings.video_link_timeout_seconds == 600
    assert "video_link_timeout_seconds=600" in repr(settings)


def test_video_link_timeout_ignores_the_legacy_bot_timeout(tmp_path, monkeypatch):
    settings = Settings.from_env(_env(tmp_path, monkeypatch, ["VIDEO_BOT_TIMEOUT_SECONDS=1800"]))
    assert settings.video_link_timeout_seconds == 900 and settings.video_bot_timeout_seconds == 1800


@pytest.mark.parametrize("value", ["abc", "5", "99999"])
def test_video_link_timeout_is_validated(tmp_path, monkeypatch, value):
    with pytest.raises(ConfigError, match="VIDEO_LINK_TIMEOUT"):
        Settings.from_env(_env(tmp_path, monkeypatch, [f"VIDEO_LINK_TIMEOUT={value}"]))


def test_env_example_documents_video_link_timeout():
    root = Path(__file__).resolve().parent.parent
    lines = (root / ".env.example").read_text(encoding="utf-8").splitlines()
    assert "VIDEO_LINK_TIMEOUT=900" in [l.strip() for l in lines]
