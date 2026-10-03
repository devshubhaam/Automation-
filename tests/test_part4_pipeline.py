"""Part 4 tests: the final combined pipeline (ZIP -> images + videos -> one message).

Everything is offline. ImgBB, Telegraph and the Telegram video bots are fakes;
the REAL ``MultiVideoBotUploader``, ``PipelineWorker`` and ``ProgressRenderer``
are used, so these tests cover the integration, not just the parts.
"""

from __future__ import annotations

import asyncio
import dataclasses
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from app.config import Settings
from app.job_manager import JobManager, JobStatus
from app.main import PipelineWorker
from app.progress import ProgressRenderer
from app.uploaders import (
    PART2_IMAGE_MAX_BYTES,
    UploadError,
    VideoTimeoutError,
    provider_for_url,
)
from app.uploaders import video_bot as vb

from .conftest import build_zip
from .test_part3_multibot import (
    DISK,
    FLEZEN,
    LIMIT,
    MultiClient,
    emit,
    multi,
    no_events,  # noqa: F401  (fixture)
)
from .test_part3_video import (
    FakeImgBB,
    FakePublisher,
    FakeVideoBot,
    event,
    make_job,
    make_worker,
    media,
    prep,
    sparse,
    write,
)

PLACEHOLDER = "Downloading the file, please wait..."


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #


class StatusMessage:
    """The single Telegram status message: records every edit/reply."""

    def __init__(self, *, fail_edits_with: str | None = None) -> None:
        self.edits: list[str] = []
        self.replies: list[str] = []
        self.edit_kwargs: list[dict[str, Any]] = []
        self.fail_edits_with = fail_edits_with

    async def edit(self, text: str, **kwargs: Any) -> None:
        if self.fail_edits_with and "Processing" in text and "completed" in text.lower():
            raise RuntimeError(self.fail_edits_with)
        self.edits.append(text)
        self.edit_kwargs.append(kwargs)

    async def reply(self, text: str, **kwargs: Any) -> None:
        self.replies.append(text)


class ZipMessage:
    """Source message: ``download_media`` copies the ZIP, ``client`` is the userbot."""

    def __init__(self, source: Path, client: Any) -> None:
        self.source, self.client = source, client

    async def download_media(self, file: str) -> str:
        import shutil

        shutil.copyfile(self.source, file)
        return file


class ExplodingPublisher:
    async def publish(self, title: str, image_urls: list[str]) -> list[dict[str, Any]]:
        raise UploadError("Telegraph createPage failed: ACCESS_TOKEN_INVALID")


def bot_behaviour(plan: dict[tuple[str, str], str]):
    """Play the video bots.

    ``plan[(bot, filename)]`` is one of:
      ``ok``         reply (to the video) with the provider URL
      ``edit``       reply "Downloading the file, please wait..." then EDIT it to the URL
      ``chain``      placeholder reply, then a NEW message replying to the placeholder
      ``reject``     reply that the file cannot be processed
      ``silent``     never answer
    """

    urls = {DISK: "https://www.diskwala.com/app/DW", FLEZEN: "https://flezen.com/s/FZ"}

    async def behaviour(bot, name, sid, handlers):
        action = plan.get((bot, name), "silent")
        url = f"{urls[bot]}-{Path(name).stem}"
        if action == "ok":
            await emit(handlers, event(url, sid + 1, reply_to=sid))
        elif action == "edit":
            await emit(handlers, event(PLACEHOLDER, sid + 1, reply_to=sid))
            await asyncio.sleep(0.05)
            await emit(handlers, event(f"✅ Done\n{url}", sid + 1, reply_to=sid))  # same id = edit
        elif action == "chain":
            await emit(handlers, event(PLACEHOLDER, sid + 1, reply_to=sid))
            await asyncio.sleep(0.02)
            await emit(handlers, event(url, sid + 2, reply_to=sid + 1))
        elif action == "reject":
            await emit(handlers, event("Error: this file type is not supported", sid + 1, reply_to=sid))

    return behaviour


def part4_settings(settings: Settings, **kw: Any) -> Settings:
    kw.setdefault("max_archive_size_mb", 50)
    kw.setdefault("max_extracted_size_mb", 50)
    return dataclasses.replace(settings, **kw)


def run_job_setup(settings, tmp_path, entries, client, *, imgbb=None, telegraph=None, uploader=None):
    worker, manager, imgbb, telegraph, uploader = make_worker(
        settings,
        imgbb=imgbb,
        telegraph=telegraph,
        video=uploader or multi(max_size_bytes=settings.video_max_size_bytes),
    )
    zip_path = build_zip(tmp_path / "album.zip", entries)
    job = manager.create_job(user_id=1, chat_id=1, archive_name="album.zip", message_id=1)
    job._telegram_message = ZipMessage(zip_path, client)
    job._status_message = StatusMessage()
    manager.mark_queued(job.job_id)
    return worker, manager, job, imgbb, telegraph


# --------------------------------------------------------------------------- #
# 1. Mixed ZIP, end to end
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_mixed_zip_end_to_end_with_one_final_message(settings, tmp_path, no_events):
    settings = part4_settings(settings, video_max_size_gb=30_000 / (1024**3))  # 30 KB per video
    plan = {
        (DISK, "a.mp4"): "ok",                 # DiskWala works
        (DISK, "b.mp4"): "reject",             # DiskWala refuses -> Flezen
        (FLEZEN, "b.mp4"): "edit",             # Flezen: placeholder, then edit -> URL
        (DISK, "d.mp4"): "ok",
    }
    client = MultiClient(bot_behaviour(plan), fail_send_for=())
    entries = {
        "pics/1.jpg": b"\xff\xd8small-1",
        "pics/2.png": b"\x89PNGsmall-2",
        "pics/huge.jpg": b"\0" * (PART2_IMAGE_MAX_BYTES + 10),   # skipped: > 2 MiB
        "clips/a.mp4": b"A" * 100,
        "clips/b.mp4": b"B" * 100,
        "clips/big.mp4": b"C" * 40_000,                           # over the (tiny) video limit
        "clips/d.mp4": b"D" * 100,
        "notes.txt": b"ignored",
    }
    worker, manager, job, imgbb, telegraph = run_job_setup(settings, tmp_path, entries, client)

    await worker._process(job)

    # ZIP -> scan: 2 + 1 images, 4 videos.
    assert (job.image_count, job.video_count) == (3, 4)

    # Images: only the eligible ones reached ImgBB; the big one was skipped with a reason.
    assert sorted(p.name for p in imgbb.calls) == ["1.jpg", "2.png"]
    assert [e["filename"] for e in job.metadata["part2_skipped_large_images"]] == ["huge.jpg"]

    # ONE Telegraph article built from the ImgBB URLs only.
    assert len(telegraph.calls) == 1
    assert sorted(telegraph.calls[0][1]) == ["https://imgbb.test/1.jpg", "https://imgbb.test/2.png"]

    # Videos: correct video -> correct URL (and provider).
    mapping = {r["filename"]: r["url"] for r in job.metadata["video_results"]}
    assert mapping == {
        "a.mp4": "https://www.diskwala.com/app/DW-a",
        "b.mp4": "https://flezen.com/s/FZ-b",
        "d.mp4": "https://www.diskwala.com/app/DW-d",
    }
    # big.mp4 never reached any bot.
    assert "big.mp4" not in {name for _, name, _ in client.sent}
    assert [f["filename"] for f in job.metadata["video_failures"]] == ["big.mp4"]

    # Overall: failed video => completed with errors; skipped image alone would not.
    assert job.status == JobStatus.COMPLETED_WITH_ERRORS

    # ONE message, edited in place (no new messages) and ending in the full result.
    status = job._status_message
    assert status.replies == []
    assert all(kw.get("link_preview") is False for kw in status.edit_kwargs)
    final = status.edits[-1]
    assert "https://telegra.ph/album" in final
    assert "a.mp4" in final and "https://www.diskwala.com/app/DW-a" in final
    assert "b.mp4" in final and "https://flezen.com/s/FZ-b" in final
    assert "d.mp4" in final and "https://www.diskwala.com/app/DW-d" in final
    assert "big.mp4" in final and "exceeds the size limit" in final
    assert "huge.jpg" in final and "image limit" in final
    assert "Completed with errors" in final
    assert len(final) <= 4096

    # Each URL is listed under ITS filename (the line after the filename).
    for name, url in mapping.items():
        assert re.search(rf"<b>{re.escape(name)}</b>[^\n]*\n\s*{re.escape(url)}", final)

    # Temporary handlers were all removed; job files cleaned.
    assert client.active_handlers() == 0
    assert not Path(job.extract_dir).exists()


# --------------------------------------------------------------------------- #
# 2. URL detection
# --------------------------------------------------------------------------- #


def test_diskwala_and_flezen_url_detection():
    pattern = re.compile(vb.build_provider_pattern())

    def found(text: str):
        return vb.extract_url(SimpleNamespace(raw_text=text, entities=None, buttons=None), pattern)

    assert found("Link: https://www.diskwala.com/app/6abfae122a52418b24707585 ✅") == (
        "https://www.diskwala.com/app/6abfae122a52418b24707585"
    )
    assert found("https://flezen.com/s/dauv7n9bjlnn77sqlrogow6-ryxopea.") == (
        "https://flezen.com/s/dauv7n9bjlnn77sqlrogow6-ryxopea"
    )
    assert provider_for_url("https://www.diskwala.com/app/x1") == "diskwala"
    assert provider_for_url("https://flezen.com/s/x1") == "flezen"
    # Wrong path for the provider, other domains and the placeholder are NOT links.
    assert found("https://flezen.com/app/x1") is None
    assert found("https://www.diskwala.com/s/x1") is None
    assert found("https://example.com/s/x1") is None
    assert found(PLACEHOLDER) is None


# --------------------------------------------------------------------------- #
# 3. Flezen: the placeholder is not a success; edited / reply messages
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["edit", "chain"])
async def test_flezen_placeholder_is_not_success_final_url_comes_from_edit_or_reply(action, tmp_path, no_events):
    client = MultiClient(bot_behaviour({(FLEZEN, "slow.mp4"): action}))
    uploader = multi(bots=(FLEZEN,), timeout_seconds=2)
    vid = write(tmp_path / "slow.mp4", 10)

    result = await uploader.upload(vid, client=client)

    assert result["url"] == "https://flezen.com/s/FZ-slow"
    assert result["provider_name"] == "flezen"
    assert client.active_handlers() == 0


@pytest.mark.asyncio
async def test_placeholder_alone_is_never_a_success(tmp_path, no_events):
    async def only_placeholder(bot, name, sid, handlers):
        await emit(handlers, event(PLACEHOLDER, sid + 1, reply_to=sid))

    client = MultiClient(only_placeholder)
    uploader = multi(bots=(FLEZEN,), timeout_seconds=0.3)
    vid = write(tmp_path / "v.mp4", 10)

    with pytest.raises(VideoTimeoutError):
        await uploader.upload(vid, client=client)

    assert client.active_handlers() == 0


# --------------------------------------------------------------------------- #
# 4. A failed video does not stop the next video
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_failed_video_continues_with_next_video_and_is_reported(settings, tmp_path, no_events):
    plan = {(DISK, "v1.mp4"): "ok", (FLEZEN, "v3.mp4"): "ok", (DISK, "v3.mp4"): "reject"}
    # v2: both bots silent -> timeout (no fallback after a timeout by default).
    client = MultiClient(bot_behaviour(plan))
    worker, manager, imgbb, telegraph, _ = make_worker(settings, video=multi(timeout_seconds=0.2))
    job = make_job(manager, client=client)
    prep(manager, job)
    job._status_message = StatusMessage()
    vids = [write(tmp_path / n, 10) for n in ("v1.mp4", "v2.mp4", "v3.mp4")]

    await worker._upload_media(job, [], [media(v, "video") for v in vids])
    manager.complete_with_errors(job.job_id)
    await worker._notify(job)

    assert [(s["filename"], s["status"]) for s in job.metadata["video_status"]] == [
        ("v1.mp4", "done"), ("v2.mp4", "failed"), ("v3.mp4", "done")]
    assert {r["filename"]: r["url"] for r in job.metadata["video_results"]} == {
        "v1.mp4": "https://www.diskwala.com/app/DW-v1",
        "v3.mp4": "https://flezen.com/s/FZ-v3",
    }
    final = job._status_message.edits[-1]
    assert "v2.mp4" in final and "did not reply with a link" in final
    assert client.active_handlers() == 0


# --------------------------------------------------------------------------- #
# 5. Telegraph failure does not stop the videos
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_telegraph_failure_does_not_stop_videos_and_is_in_final_message(settings, tmp_path, no_events):
    settings = part4_settings(settings)
    client = MultiClient(bot_behaviour({(DISK, "a.mp4"): "ok", (DISK, "b.mp4"): "ok"}))
    entries = {"1.jpg": b"img", "a.mp4": b"A" * 50, "b.mp4": b"B" * 50}
    worker, manager, job, imgbb, _ = run_job_setup(
        settings, tmp_path, entries, client, telegraph=ExplodingPublisher()
    )

    await worker._process(job)

    assert job.status == JobStatus.COMPLETED_WITH_ERRORS
    assert {r["filename"] for r in job.metadata["video_results"]} == {"a.mp4", "b.mp4"}
    final = job._status_message.edits[-1]
    assert "https://www.diskwala.com/app/DW-a" in final
    assert "https://www.diskwala.com/app/DW-b" in final
    assert "Telegraph article: failed" in final
    assert "ACCESS_TOKEN_INVALID" in final


# --------------------------------------------------------------------------- #
# 6. >1.5 GB rejection
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_over_1_5_gb_video_is_rejected_with_reason_and_next_video_continues(settings, tmp_path, no_events):
    assert settings.video_max_size_bytes == LIMIT
    client = MultiClient(bot_behaviour({(DISK, "ok.mp4"): "ok"}))
    worker, manager, imgbb, telegraph, _ = make_worker(settings, video=multi())
    job = make_job(manager, client=client)
    prep(manager, job)
    job._status_message = StatusMessage()
    huge = sparse(tmp_path / "huge.mp4", LIMIT + 1)
    ok = write(tmp_path / "ok.mp4", 10)

    await worker._upload_media(job, [], [media(huge, "video"), media(ok, "video")])
    manager.complete_with_errors(job.job_id)
    await worker._notify(job)

    assert "huge.mp4" not in {name for _, name, _ in client.sent}  # never sent to any bot
    assert [f["filename"] for f in job.metadata["video_failures"]] == ["huge.mp4"]
    assert [r["filename"] for r in job.metadata["video_results"]] == ["ok.mp4"]
    final = job._status_message.edits[-1]
    assert "huge.mp4" in final and "exceeds the size limit" in final
    assert "https://www.diskwala.com/app/DW-ok" in final


# --------------------------------------------------------------------------- #
# 7. Videos never go to ImgBB / Telegraph (end to end)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_videos_never_reach_imgbb_or_telegraph_in_the_full_pipeline(settings, tmp_path, no_events):
    settings = part4_settings(settings)
    client = MultiClient(bot_behaviour({(DISK, "clip.mp4"): "ok", (DISK, "clip2.mkv"): "ok"}))
    entries = {"p.jpg": b"img", "clip.mp4": b"V" * 30, "clip2.mkv": b"W" * 30}
    worker, manager, job, imgbb, telegraph = run_job_setup(settings, tmp_path, entries, client)

    await worker._process(job)

    assert job.status == JobStatus.COMPLETED
    assert [p.name for p in imgbb.calls] == ["p.jpg"]
    sent_to_telegraph = " ".join(url for _, urls in telegraph.calls for url in urls)
    assert "clip" not in sent_to_telegraph and "diskwala" not in sent_to_telegraph
    assert {name for _, name, _ in client.sent} == {"clip.mp4", "clip2.mkv"}
    assert "Processing completed successfully" in job._status_message.edits[-1]


# --------------------------------------------------------------------------- #
# 8. Final message: limits, fallback
# --------------------------------------------------------------------------- #


def test_final_message_always_fits_into_one_telegram_message(settings):
    manager = JobManager(settings.job_dir)
    job = make_job(manager)
    prep(manager, job)
    manager.set_media_counts(job.job_id, image_count=300, video_count=60)
    from app.job_manager import UploadFailure, UploadResult

    for i in range(300):
        manager.add_upload_result(job.job_id, UploadResult(
            media_type="image", filename=f"image-{i}.jpg", provider="imgbb",
            url=f"https://i.ibb.co/{i:08d}/image-{i}.jpg"))
    manager.add_upload_result(job.job_id, UploadResult(
        media_type="article", filename="album", provider="telegraph_article",
        url="https://telegra.ph/album-10-03"))
    for i in range(40):
        manager.add_upload_result(job.job_id, UploadResult(
            media_type="video", filename=f"a-rather-long-video-name-{i}.mp4", provider="video_bot",
            url=f"https://www.diskwala.com/app/{i:024d}", extra={"provider_name": "diskwala"}))
    for i in range(20):
        manager.add_upload_failure(job.job_id, UploadFailure(
            provider="video_bot", media_type="video", filename=f"bad-{i}.mp4", error="timeout " * 80))
    manager.set_metadata(job.job_id, "part2_skipped_large_images", [
        {"filename": f"big-{i}.jpg", "size_bytes": 5 * 1024 * 1024, "limit_bytes": PART2_IMAGE_MAX_BYTES}
        for i in range(50)])
    manager.complete_with_errors(job.job_id)

    text = ProgressRenderer().render(job)

    assert len(text) <= 4096
    assert "https://telegra.ph/album-10-03" in text          # highest priority survives
    assert "a-rather-long-video-name-0.mp4" in text and "bad-0.mp4" in text
    assert "Completed with errors" in text


def test_small_final_message_shows_everything(settings):
    manager = JobManager(settings.job_dir)
    job = make_job(manager)
    prep(manager, job)
    from app.job_manager import UploadResult

    manager.add_upload_result(job.job_id, UploadResult(
        media_type="article", filename="album", provider="telegraph_article", url="https://telegra.ph/a"))
    manager.add_upload_result(job.job_id, UploadResult(
        media_type="video", filename="v<1>.mp4", provider="video_bot", url="https://flezen.com/s/FZ1",
        extra={"provider_name": "flezen"}))
    manager.complete(job.job_id)

    text = ProgressRenderer().render(job)

    assert "https://telegra.ph/a" in text
    assert "v&lt;1&gt;.mp4" in text and "(Flezen)" in text and "https://flezen.com/s/FZ1" in text
    assert "Completed" in text


@pytest.mark.asyncio
async def test_final_edit_failure_falls_back_to_a_reply(settings, tmp_path):
    worker, manager, *_ = make_worker(settings)
    job = make_job(manager)
    prep(manager, job)
    job._status_message = StatusMessage(fail_edits_with="MESSAGE_TOO_LONG")
    manager.complete(job.job_id)

    await worker._notify(job)

    assert job._status_message.edits == []
    assert len(job._status_message.replies) == 1
    assert "completed" in job._status_message.replies[0].lower()


# --------------------------------------------------------------------------- #
# 9. Cancellation / shutdown
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_shutdown_while_waiting_for_a_bot_cancels_job_and_cleans_handlers(settings, tmp_path, no_events):
    settings = part4_settings(settings)
    client = MultiClient(bot_behaviour({}))  # bot never answers
    entries = {"a.mp4": b"A" * 50}
    worker, manager, job, *_ = run_job_setup(
        settings, tmp_path, entries, client, uploader=multi(timeout_seconds=30)
    )

    task = asyncio.create_task(worker._process(job))
    for _ in range(300):
        if client.active_handlers():
            break
        await asyncio.sleep(0.01)
    assert client.active_handlers() > 0  # waiting for the bot

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert job.status == JobStatus.CANCELLED
    assert job.metadata["cancel_reason"] == "application shutdown"
    assert job.metadata["video_status"][0]["status"] == "cancelled"
    assert client.active_handlers() == 0
    assert "cancelled" in job._status_message.edits[-1].lower()
    assert not Path(job.extract_dir).exists()


@pytest.mark.asyncio
async def test_cancel_request_while_waiting_stops_the_job_safely(settings, tmp_path, no_events):
    settings = part4_settings(settings)
    client = MultiClient(bot_behaviour({}))
    entries = {"1.jpg": b"img", "a.mp4": b"A" * 50, "b.mp4": b"B" * 50}
    worker, manager, job, imgbb, telegraph = run_job_setup(
        settings, tmp_path, entries, client, uploader=multi(timeout_seconds=30)
    )

    task = asyncio.create_task(worker._process(job))
    for _ in range(300):
        if client.active_handlers():
            break
        await asyncio.sleep(0.01)
    manager.cancel(job.job_id)
    await asyncio.wait_for(task, timeout=5)

    assert job.status == JobStatus.CANCELLED
    assert [name for _, name, _ in client.sent] == ["a.mp4"]  # b.mp4 never sent
    assert client.active_handlers() == 0
    final = job._status_message.edits[-1]
    assert "cancelled" in final.lower()
    assert "https://telegra.ph/album" in final  # partial results are kept


@pytest.mark.asyncio
async def test_pipeline_stop_cancels_a_running_worker_job(settings, tmp_path, no_events):
    settings = part4_settings(settings)
    client = MultiClient(bot_behaviour({}))
    worker, manager, job, *_ = run_job_setup(
        settings, tmp_path, {"a.mp4": b"A" * 50}, client, uploader=multi(timeout_seconds=30)
    )
    await worker.submit(job)
    await worker.start()
    for _ in range(300):
        if client.active_handlers():
            break
        await asyncio.sleep(0.01)

    await asyncio.wait_for(worker.stop(), timeout=5)

    assert job.status == JobStatus.CANCELLED
    assert client.active_handlers() == 0


# --------------------------------------------------------------------------- #
# 10. Session safety / configuration / docs
# --------------------------------------------------------------------------- #

APP = Path(__file__).resolve().parent.parent / "app"
ROOT = APP.parent


def test_pipeline_never_touches_the_telegram_session():
    source = (APP / "main.py").read_text(encoding="utf-8")
    for forbidden in ("session_store", "SessionStore", "persist_session", "_reset_dead_session",
                      "delete_session", "StringSession"):
        assert forbidden not in source


def test_authkeyduplicated_fix_is_preserved():
    source = (APP / "telegram_client.py").read_text(encoding="utf-8")
    assert "AuthKeyDuplicatedError" in source
    assert "_reset_dead_session" in source


def test_env_example_documents_the_real_video_configuration():
    text = (ROOT / ".env.example").read_text(encoding="utf-8")
    values = dict(
        line.split("=", 1) for line in text.splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    )
    assert values["VIDEO_BOTS"].strip() == "@DiskWalaFileUploaderBot,@FlezenUploadBot"
    assert values["VIDEO_BOT_TIMEOUT_SECONDS"].strip() == "1800"
    assert values["VIDEO_BOT_SEND_ATTEMPTS"].strip() == "2"
    assert values["VIDEO_MAX_SIZE_GB"].strip() == "1.5"
    for key in ("IMGBB_API_KEY", "TELEGRAPH_ACCESS_TOKEN"):
        assert key in values


def test_env_example_is_read_by_settings(tmp_path, monkeypatch):
    for key in ("VIDEO_BOTS", "VIDEO_BOT_USERNAME", "VIDEO_BOT_TIMEOUT_SECONDS",
                "VIDEO_BOT_SEND_ATTEMPTS", "VIDEO_MAX_SIZE_GB"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("API_ID", "12345")
    monkeypatch.setenv("API_HASH", "0123456789abcdef0123456789abcdef")
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    env = tmp_path / ".env"
    env.write_text(
        "\n".join(
            line for line in example.splitlines()
            if line.split("=", 1)[0] in {"VIDEO_BOTS", "VIDEO_BOT_TIMEOUT_SECONDS",
                                         "VIDEO_BOT_SEND_ATTEMPTS", "VIDEO_MAX_SIZE_GB"}
        ),
        encoding="utf-8",
    )

    settings = Settings.from_env(env)

    assert settings.video_bots == (DISK, FLEZEN)
    assert settings.video_bot_timeout_seconds == 1800
    assert settings.video_bot_send_attempts == 2
    assert settings.video_max_size_bytes == LIMIT
