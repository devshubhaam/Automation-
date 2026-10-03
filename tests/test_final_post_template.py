"""FINAL_POST_TEMPLATE: custom final Telegram post (offline, no Telegram)."""

from __future__ import annotations

import logging

from app.config import Settings
from app.job_manager import JobManager, JobStatus, UploadResult
from app.progress import ProgressRenderer, normalize_final_post_template

TELEGRAPH = "https://telegra.ph/album-10-03"
DISK = "https://www.diskwala.com/app/"
DEFAULT_TEMPLATE = r"📝 Telegraph\n{telegraph_url}\n\n🎬 Videos\n\n{video_links}"


def make_job(tmp_path, archive_name="My Album.zip", job_id="job-1"):
    manager = JobManager(tmp_path / "jobs")
    job = manager.create_job(job_id=job_id, user_id=1, chat_id=1, archive_name=archive_name)
    for status in (JobStatus.QUEUED, JobStatus.DOWNLOADING, JobStatus.EXTRACTING,
                   JobStatus.SCANNING, JobStatus.UPLOADING):
        manager.set_status(job.job_id, status)
    return manager, job


def add_article(manager, job, url=TELEGRAPH):
    manager.add_upload_result(job.job_id, UploadResult(
        media_type="article", filename="album", provider="telegraph_article", url=url))


def add_video(manager, job, name, size, url, provider="diskwala"):
    manager.add_upload_result(job.job_id, UploadResult(
        media_type="video", filename=name, provider="video_bot", url=url,
        size_bytes=size, extra={"provider_name": provider}))


def add_image(manager, job, name="i.jpg", url="https://i.ibb.co/i.jpg"):
    manager.add_upload_result(job.job_id, UploadResult(
        media_type="image", filename=name, provider="imgbb", url=url))


def finished(tmp_path, videos, *, article=True, images=0, **job_kwargs):
    """videos: (filename, size, url[, provider]) in upload-COMPLETION order."""
    manager, job = make_job(tmp_path, **job_kwargs)
    if article:
        add_article(manager, job)
    for video in videos:
        add_video(manager, job, *video)
    for i in range(images):
        add_image(manager, job, f"{i}.jpg", f"https://i.ibb.co/{i}.jpg")
    manager.complete(job.job_id)
    return job


def render(job, template):
    return ProgressRenderer(final_post_template=template).render(job)


ABCD = [
    ("A.mp4", 500, DISK + "A"),
    ("B.mp4", 1200, DISK + "B"),
    ("C.mp4", 500, DISK + "C"),
    ("D.mp4", 200, DISK + "D"),
]


# A / F / G -------------------------------------------------------------------

def test_custom_template_renders_acceptance_example(tmp_path):
    job = finished(tmp_path, ABCD)
    assert render(job, DEFAULT_TEMPLATE) == (
        f"📝 Telegraph\n{TELEGRAPH}\n\n🎬 Videos\n\n"
        f"{DISK}B\n\n{DISK}A\n\n{DISK}C\n\n{DISK}D"
    )


def test_second_example_template_with_title(tmp_path):
    job = finished(tmp_path, ABCD[:2])
    template = "📦 {title}\n\n🔗 Telegraph:\n{telegraph_url}\n\n🎬 Videos:\n{video_links}"
    assert render(job, template) == (
        f"📦 My Album\n\n🔗 Telegraph:\n{TELEGRAPH}\n\n🎬 Videos:\n{DISK}B\n\n{DISK}A"
    )


# B ---------------------------------------------------------------------------

def test_every_supported_variable(tmp_path):
    job = finished(tmp_path, ABCD, images=3)
    template = (
        "{telegraph_url}|{video_links}|{title}|{archive_name}|"
        "{video_count}|{image_count}|{job_id}"
    )
    assert render(job, template) == (
        f"{TELEGRAPH}|{DISK}B\n\n{DISK}A\n\n{DISK}C\n\n{DISK}D|My Album|My Album.zip|4|3|job-1"
    )


def test_missing_telegraph_is_empty_string(tmp_path):
    job = finished(tmp_path, ABCD[:1], article=False)
    assert render(job, "[{telegraph_url}]\n{video_links}") == f"[]\n{DISK}A"


def test_values_are_html_escaped_but_template_text_is_kept(tmp_path):
    job = finished(tmp_path, [("a.mp4", 1, DISK + "X&Y")], archive_name="A<b>&.zip")
    assert render(job, "<b>{title}</b>\n{video_links}") == f"<b>A&lt;b&gt;&amp;</b>\n{DISK}X&amp;Y"


# C / D / E / provider filtering ----------------------------------------------

def test_video_links_are_diskwala_only(tmp_path):
    job = finished(tmp_path, [
        ("secret-name.mp4", 900, DISK + "DW", "diskwala"),
        ("secret-name.mp4", 900, "https://flezen.com/s/FZ", "flezen"),
        ("x.mp4", 800, "https://i.ibb.co/video.mp4", "custom"),
        ("y.mp4", 700, "https://example.com/arbitrary", "custom"),
        ("z.mp4", 600, "https://telegra.ph/other", "custom"),
    ], images=2)
    text = render(job, "{video_links}")
    assert text == DISK + "DW"
    for banned in ("flezen", "ibb.co", "example.com", "telegra.ph", "secret-name", "mp4", "DiskWala", "video_bot"):
        assert banned not in text


def test_imgbb_url_as_video_result_never_in_video_links(tmp_path):
    job = finished(tmp_path, [("v.mp4", 5, "https://i.ibb.co/a.jpg"), ("w.mp4", 4, DISK + "W")], images=1)
    text = render(job, "{video_links}")
    assert "ibb.co" not in text and text == DISK + "W"


# H ---------------------------------------------------------------------------

def test_upload_completion_order_does_not_matter(tmp_path):
    forward = finished(tmp_path / "f", ABCD)
    backward_videos = [ABCD[3], ABCD[2], ABCD[1], ABCD[0]]
    backward = finished(tmp_path / "b", backward_videos)
    assert render(forward, "{video_links}").split("\n\n")[0] == DISK + "B"
    assert render(backward, "{video_links}").split("\n\n")[0] == DISK + "B"
    assert render(backward, "{video_links}").split("\n\n")[-1] == DISK + "D"


def test_equal_sizes_keep_pipeline_order(tmp_path):
    job = finished(tmp_path, [
        ("c.mp4", 10, DISK + "C"), ("a.mp4", 10, DISK + "A"), ("b.mp4", 10, DISK + "B")])
    assert render(job, "{video_links}") == f"{DISK}C\n\n{DISK}A\n\n{DISK}B"


# I ---------------------------------------------------------------------------

def test_empty_template_uses_default_clean_post(tmp_path):
    job = finished(tmp_path, ABCD)
    expected = (
        f"📝 Telegraph\n{TELEGRAPH}\n\n🎬 Videos\n\n"
        f"{DISK}B\n\n{DISK}A\n\n{DISK}C\n\n{DISK}D"
    )
    for template in (None, "", "   ", "\n"):
        assert render(job, template) == expected
    assert ProgressRenderer().render(job) == expected


# J ---------------------------------------------------------------------------

def test_unknown_placeholder_falls_back_without_crashing(tmp_path, caplog):
    job = finished(tmp_path, ABCD[:2])
    with caplog.at_level(logging.WARNING, logger="app.progress"):
        text = render(job, "{telegraph_url}\n{something_invalid}")
    assert text == f"📝 Telegraph\n{TELEGRAPH}\n\n🎬 Videos\n\n{DISK}B\n\n{DISK}A"
    assert any("something_invalid" in r.getMessage() for r in caplog.records)


def test_malformed_or_attribute_placeholders_fall_back(tmp_path):
    job = finished(tmp_path, ABCD[:1])
    default = ProgressRenderer().render(job)
    for bad in ("{telegraph_url", "}{video_links}", "{video_links.__class__}", "{job_id[0]}", "{}", "{0}"):
        assert render(job, bad) == default


def test_literal_braces_are_supported(tmp_path):
    job = finished(tmp_path, ABCD[:1])
    assert render(job, "{{x}} {video_links}") == f"{{x}} {DISK}A"


# K ---------------------------------------------------------------------------

def test_escaped_newlines_become_real_newlines(tmp_path):
    job = finished(tmp_path, ABCD[:2])
    text = render(job, DEFAULT_TEMPLATE)
    assert "\\n" not in text and text.count("\n") == 7
    assert normalize_final_post_template(r"a\nb\r\nc") == "a\nb\nc"
    assert normalize_final_post_template("a\r\nb") == "a\nb"  # real multiline value


def test_settings_reads_template_from_env(tmp_path, monkeypatch):
    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "0123456789abcdef")
    monkeypatch.setenv("FINAL_POST_TEMPLATE", DEFAULT_TEMPLATE)
    assert Settings.from_env(env_file=None).final_post_template == DEFAULT_TEMPLATE
    monkeypatch.delenv("FINAL_POST_TEMPLATE")
    assert Settings.from_env(env_file=None).final_post_template is None


# L / M / N -------------------------------------------------------------------

def many_videos(n, url_len=100):
    # Sizes grow with the index so the expected order is the REVERSE of upload order.
    return [(f"v{i}.mp4", i + 1, f"{DISK}{i:04d}-" + "x" * url_len) for i in range(n)]


def test_length_limit_drops_smallest_videos_first_and_keeps_order(tmp_path):
    videos = many_videos(60)
    job = finished(tmp_path, videos)
    text = render(job, "HEADER\n{telegraph_url}\n{video_links}\nFOOTER {video_count}")

    assert len(text) <= ProgressRenderer.MAX_MESSAGE_CHARS <= 4096
    lines = text.split("\n")
    assert lines[0] == "HEADER" and lines[1] == TELEGRAPH
    kept = [l for l in lines[2:-1] if l]
    expected_all = [u for _, _, u in sorted(videos, key=lambda v: -v[1])]
    assert 0 < len(kept) < 60
    assert kept == expected_all[: len(kept)]          # largest kept, order unchanged
    assert lines[-1] == f"FOOTER {len(kept)}"          # video_count = links really shown


def test_template_without_room_falls_back_to_default_and_never_exceeds_limit(tmp_path):
    job = finished(tmp_path, many_videos(3))
    text = render(job, "x" * 5000 + "{video_links}")
    assert text.startswith("📝 Telegraph\n" + TELEGRAPH)
    assert len(text) <= 4096


def test_telegraph_url_never_removed_to_fit_links(tmp_path):
    job = finished(tmp_path, many_videos(80))
    text = render(job, "{telegraph_url}\n{video_links}")
    assert text.startswith(TELEGRAPH) and len(text) <= 4096


# O ---------------------------------------------------------------------------

def test_duplicate_diskwala_urls_are_deduplicated(tmp_path):
    job = finished(tmp_path, [
        ("a.mp4", 100, DISK + "SAME"), ("a-copy.mp4", 300, DISK + "SAME"), ("b.mp4", 50, DISK + "B")])
    text = render(job, "{video_links}\n{video_count}")
    assert text == f"{DISK}SAME\n\n{DISK}B\n2"


# Blank-line separation of {video_links} -------------------------------------

def test_video_links_one_blank_line_between_links(tmp_path):
    two = finished(tmp_path / "2", [("a.mp4", 2, DISK + "1"), ("b.mp4", 1, DISK + "2")])
    assert render(two, "{video_links}") == f"{DISK}1\n\n{DISK}2"

    three = finished(tmp_path / "3", [
        ("a.mp4", 3, DISK + "1"), ("b.mp4", 2, DISK + "2"), ("c.mp4", 1, DISK + "3")])
    text = render(three, "{video_links}")
    assert text == f"{DISK}1\n\n{DISK}2\n\n{DISK}3"
    assert "\n\n\n" not in text


def test_video_links_single_and_zero_links_have_no_extra_blank_lines(tmp_path):
    one = finished(tmp_path / "1", [("a.mp4", 1, DISK + "ONLY")])
    assert render(one, "{video_links}") == DISK + "ONLY"

    zero = finished(tmp_path / "0", [])
    assert render(zero, "[{video_links}]|{video_count}") == f"[]|0"


def test_blank_line_format_keeps_sorting_stability_and_filtering(tmp_path):
    job = finished(tmp_path, [
        ("a.mp4", 500, DISK + "A"),
        ("b.mp4", 1200, DISK + "B"),
        ("c.mp4", 500, DISK + "C"),
        ("x.mp4", 9999, "https://i.ibb.co/x.jpg", "custom"),
        ("f.mp4", 800, "https://flezen.com/s/F", "flezen"),
    ])
    assert render(job, "{video_links}") == f"{DISK}B\n\n{DISK}A\n\n{DISK}C"
