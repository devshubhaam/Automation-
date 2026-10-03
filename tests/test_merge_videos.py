"""MERGE_VIDEOS tests: lossless multi-video merge before the DiskWala bot.

Three layers, all offline:

* pure helpers (concat list escaping, FFmpeg command, compatibility rules);
* the real :class:`VideoMerger` driving *fake* ``ffmpeg``/``ffprobe`` scripts
  (failure, hang, cancel, oversize, missing binaries) - these exercise the real
  asyncio subprocess code;
* the real :class:`PipelineWorker` with the merger faked or faked-binaries, plus
  REAL FFmpeg tests (skipped when ffmpeg/ffprobe are not installed) that prove
  the output is a stream copy of the sources.
"""

from __future__ import annotations

import asyncio
import collections
import dataclasses
import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from app.config import Settings, resolve_article_title
from app.job_manager import JobManager, JobStatus
from app.main import JobCancelled, PipelineWorker
from app.progress import ProgressRenderer
from app.uploaders import UploadError
from app.video_merge import (
    COMPARED_AUDIO_FIELDS,
    COMPARED_VIDEO_FIELDS,
    FFmpegUnavailableError,
    IncompatibleVideosError,
    MergeCancelledError,
    MergedTooLargeError,
    MergeResult,
    VideoMergeError,
    VideoMerger,
    build_concat_list,
    build_ffmpeg_command,
    check_compatibility,
    choose_output_suffix,
    escape_concat_path,
    parse_probe,
    sanitize_stem,
)

from .conftest import build_zip
from .test_part3_video import (
    FakeImgBB,
    FakePublisher,
    FakeVideoBot,
    make_job,
    media,
    prep,
    write,
)
from .test_part4_pipeline import StatusMessage, ZipMessage

DISKWALA = "https://www.diskwala.com/app/"
HAVE_FFMPEG = bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))
needs_ffmpeg = pytest.mark.skipif(not HAVE_FFMPEG, reason="ffmpeg/ffprobe not installed")


# --------------------------------------------------------------------------- #
# Fake ffprobe / ffmpeg executables (real subprocesses, scripted behaviour)
# --------------------------------------------------------------------------- #

FAKE_FFPROBE = r'''#!{python}
import json, os, sys
args = sys.argv[1:]
path = args[args.index("-i") + 1]
if os.environ.get("FAKE_FFPROBE_FAIL") == os.path.basename(path):
    sys.stderr.write("Invalid data found when processing input\n")
    sys.exit(1)
side = path + ".probe.json"
if os.path.exists(side):
    data = json.load(open(side, encoding="utf-8"))
else:
    data = {{
        "format": {{"format_name": "mov,mp4,m4a,3gp,3g2,mj2", "duration": "10.0"}},
        "streams": [
            {{"index": 0, "codec_type": "video", "codec_name": "h264", "profile": "High",
              "level": 40, "width": 1920, "height": 1080, "pix_fmt": "yuv420p",
              "sample_aspect_ratio": "1:1", "r_frame_rate": "30/1", "disposition": {{"attached_pic": 0}}}},
            {{"index": 1, "codec_type": "audio", "codec_name": "aac", "profile": "LC",
              "sample_rate": "48000", "channels": 2, "channel_layout": "stereo", "disposition": {{}}}},
        ],
    }}
print(json.dumps(data))
'''

FAKE_FFMPEG = r'''#!{python}
import json, os, shlex, signal, sys, time
args = sys.argv[1:]
out = args[-1]
lst = args[args.index("-i") + 1]
content = open(lst, encoding="utf-8").read()
log = os.environ.get("FAKE_FFMPEG_LOG")
if log:
    with open(log, "a", encoding="utf-8") as fh:
        fh.write(json.dumps({{"args": args, "list": content}}) + "\n")
files = [shlex.split(l)[1] for l in content.splitlines() if l.startswith("file ")]
mode = os.environ.get("FAKE_FFMPEG_MODE", "ok")
if mode == "ok":
    with open(out, "wb") as fh:
        for f in files:
            fh.write(open(f, "rb").read())
elif mode == "fail":
    open(out, "wb").write(b"partial")
    sys.stderr.write("Error while decoding stream #0:0: Invalid data found\n")
    sys.exit(1)
elif mode in ("hang", "hang_ignore_term"):
    open(out, "wb").write(b"partial")
    if mode == "hang_ignore_term":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    open(os.environ["FAKE_FFMPEG_PID"], "w").write(str(os.getpid()))
    time.sleep(120)
elif mode == "big":
    with open(out, "wb") as fh:
        fh.truncate(int(os.environ["FAKE_FFMPEG_SIZE"]))
elif mode == "nooutput":
    pass
'''


def _script(path: Path, template: str) -> str:
    path.write_text(template.format(python=sys.executable), encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return str(path)


@pytest.fixture()
def fake_tools(tmp_path, monkeypatch):
    """Fake ffmpeg/ffprobe + a log of every ffmpeg invocation."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "ffmpeg.log"
    monkeypatch.setenv("FAKE_FFMPEG_LOG", str(log))
    monkeypatch.setenv("FAKE_FFMPEG_MODE", "ok")
    monkeypatch.setenv("FAKE_FFMPEG_PID", str(tmp_path / "ffmpeg.pid"))
    tools = type("Tools", (), {})()
    tools.ffmpeg = _script(bin_dir / "ffmpeg", FAKE_FFMPEG)
    tools.ffprobe = _script(bin_dir / "ffprobe", FAKE_FFPROBE)
    tools.log = log
    tools.pid_file = tmp_path / "ffmpeg.pid"
    tools.calls = lambda: [json.loads(l) for l in log.read_text().splitlines()] if log.exists() else []
    tools.merger = lambda **kw: VideoMerger(
        ffmpeg_bin=tools.ffmpeg, ffprobe_bin=tools.ffprobe, poll_interval=0.05, terminate_grace=1.0, **kw
    )
    return tools


def make_sources(directory: Path, names: list[str]) -> list[Path]:
    directory.mkdir(parents=True, exist_ok=True)
    paths = []
    for i, name in enumerate(names):
        path = directory / name
        path.write_bytes(bytes([65 + i % 26]) * (10 + i))  # distinct content per source
        paths.append(path)
    return paths


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


async def wait_for_file(path: Path, timeout: float = 10.0) -> None:
    for _ in range(int(timeout / 0.02)):
        if path.exists() and path.read_text().strip():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"{path.name} never appeared")


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #

TRICKY_NAMES = [
    "plain.mp4",
    "with space.mp4",
    "it's a 'quote'.mp4",
    'dq "double".mp4',
    "[brackets] (parens) {braces}.mp4",
    "ünïcödé 視頻 🎬.mp4",
    "dollar $HOME `tick` ;semi & amp | pipe.mp4",
    "back\\slash.mp4",
    "-starts-with-dash.mp4",
]


def test_escape_concat_path_quotes_single_quotes():
    assert escape_concat_path("/a/b c.mp4") == "'/a/b c.mp4'"
    assert escape_concat_path("/a/it's.mp4") == "'/a/it'\\''s.mp4'"


def test_concat_list_keeps_order_and_absolute_paths(tmp_path):
    paths = [tmp_path / n for n in ("b.mp4", "a.mp4", "c.mp4")]
    lines = build_concat_list(paths).splitlines()
    assert lines[0] == "ffconcat version 1.0"
    assert lines[1:] == [f"file '{p}'" for p in paths]  # NOT sorted


def test_concat_list_rejects_line_breaks(tmp_path):
    with pytest.raises(VideoMergeError, match="line break"):
        build_concat_list([tmp_path / "bad\nname.mp4"])


@pytest.mark.parametrize("name", TRICKY_NAMES)
def test_concat_list_round_trips_tricky_names(tmp_path, name):
    import shlex

    line = build_concat_list([tmp_path / name]).splitlines()[1]
    assert shlex.split(line) == ["file", str(tmp_path / name)]


def test_ffmpeg_command_is_stream_copy_only(tmp_path):
    command = build_ffmpeg_command("ffmpeg", tmp_path / "l.txt", tmp_path / "o.mp4", [0, 1])
    assert command[0] == "ffmpeg"
    assert command[command.index("-f") + 1] == "concat"
    assert command[command.index("-safe") + 1] == "0"
    assert command[command.index("-c") + 1] == "copy"
    assert command.count("-c") == 1
    assert command[-1] == str(tmp_path / "o.mp4")
    forbidden = {
        "libx264", "libx265", "h264", "hevc", "aac", "libmp3lame", "-crf", "-b:v", "-b:a",
        "-preset", "-vf", "-af", "-filter_complex", "-q:v", "-qscale", "-vcodec", "-acodec",
        "-r", "-s", "-pix_fmt", "-vn", "-an",
    }
    assert forbidden.isdisjoint(command)


def test_ffmpeg_command_maps_explicit_streams_and_bounds_disk(tmp_path):
    command = build_ffmpeg_command("ffmpeg", tmp_path / "l", tmp_path / "o", [0, 2], max_output_bytes=1000)
    maps = [command[i + 1] for i, v in enumerate(command) if v == "-map"]
    assert maps == ["0:0", "0:2"]
    assert command[command.index("-fs") + 1] == "1001"


def test_choose_output_suffix():
    assert choose_output_suffix([Path("a.MP4"), Path("b.mp4")], "mov,mp4") == ".mp4"
    assert choose_output_suffix([Path("a.mov"), Path("b.mp4")], "mov,mp4,m4a") == ".mp4"
    assert choose_output_suffix([Path("a.webm"), Path("b.mkv")], "matroska,webm") == ".mkv"


def test_sanitize_stem():
    assert sanitize_stem("a/b\\c:d") == "a b c d"
    assert sanitize_stem("..") == "merged"
    assert sanitize_stem("") == "merged"
    assert len(sanitize_stem("x" * 500)) <= 80


# ---- compatibility ---------------------------------------------------------


def probe_json(**video: Any) -> dict[str, Any]:
    audio = video.pop("audio", [{}])
    fmt = video.pop("format_name", "mov,mp4,m4a,3gp,3g2,mj2")
    v = {
        "index": 0, "codec_type": "video", "codec_name": "h264", "profile": "High", "level": 40,
        "width": 1920, "height": 1080, "pix_fmt": "yuv420p", "sample_aspect_ratio": "1:1",
        "r_frame_rate": "30/1", "disposition": {"attached_pic": 0},
    }
    v.update(video)
    streams = [v]
    for i, extra in enumerate(audio, start=1):
        a = {"index": i, "codec_type": "audio", "codec_name": "aac", "profile": "LC",
             "sample_rate": "48000", "channels": 2, "channel_layout": "stereo", "disposition": {}}
        a.update(extra)
        streams.append(a)
    return {"format": {"format_name": fmt, "duration": "5.0"}, "streams": streams}


def probes(*datas: dict[str, Any]):
    return [parse_probe(Path(f"/x/video{i + 1}.mp4"), d) for i, d in enumerate(datas)]


def test_identical_streams_are_compatible():
    check_compatibility(probes(probe_json(), probe_json(), probe_json()))


def test_compared_fields_cover_the_required_properties():
    for field in ("codec_name", "width", "height", "pix_fmt", "r_frame_rate"):
        assert field in COMPARED_VIDEO_FIELDS
    for field in ("codec_name", "sample_rate", "channels", "channel_layout"):
        assert field in COMPARED_AUDIO_FIELDS


@pytest.mark.parametrize(
    "bad, key",
    [
        (probe_json(codec_name="hevc"), "codec_name"),
        (probe_json(width=1280, height=720), "width"),
        (probe_json(height=720), "height"),
        (probe_json(pix_fmt="yuv444p"), "pix_fmt"),
        (probe_json(r_frame_rate="25/1"), "r_frame_rate"),
        (probe_json(profile="Main"), "profile"),
        (probe_json(sample_aspect_ratio="4:3"), "sample_aspect_ratio"),
        (probe_json(audio=[{"codec_name": "mp3"}]), "codec_name"),
        (probe_json(audio=[{"sample_rate": "44100"}]), "sample_rate"),
        (probe_json(audio=[{"channels": 1, "channel_layout": "mono"}]), "channels"),
    ],
)
def test_incompatible_property_is_detected_and_named(bad, key):
    with pytest.raises(IncompatibleVideosError, match=key) as info:
        check_compatibility(probes(probe_json(), bad))
    assert "video1.mp4" in str(info.value) and "video2.mp4" in str(info.value)


def test_incompatible_container_audio_count_and_missing_video():
    with pytest.raises(IncompatibleVideosError, match="container"):
        check_compatibility(probes(probe_json(), probe_json(format_name="matroska,webm")))
    with pytest.raises(IncompatibleVideosError, match="audio stream count"):
        check_compatibility(probes(probe_json(), probe_json(audio=[])))
    with pytest.raises(IncompatibleVideosError, match="audio stream count"):
        check_compatibility(probes(probe_json(), probe_json(audio=[{}, {}])))
    with pytest.raises(IncompatibleVideosError, match="no video stream"):
        check_compatibility(probes(probe_json(), {"format": {"format_name": "x"}, "streams": []}))


def test_cover_art_is_not_treated_as_a_video_stream():
    with_cover = probe_json()
    with_cover["streams"].append(
        {"index": 5, "codec_type": "video", "codec_name": "mjpeg", "width": 100, "height": 100,
         "disposition": {"attached_pic": 1}}
    )
    check_compatibility(probes(probe_json(), with_cover))
    assert [i for i, _ in probes(with_cover)[0].video] == [0]


# --------------------------------------------------------------------------- #
# VideoMerger + fake ffmpeg/ffprobe
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_merge_uses_stream_copy_and_preserves_the_given_order(tmp_path, fake_tools):
    # Deliberately NOT alphabetical: the merger must never re-sort.
    sources = make_sources(tmp_path / "src", ["video3.mp4", "video1.mp4", "video2.mp4"])

    result = await fake_tools.merger().merge(sources, tmp_path / "out", output_stem="album_merged")

    (call,) = fake_tools.calls()
    assert call["args"][call["args"].index("-c") + 1] == "copy"
    listed = [line for line in call["list"].splitlines() if line.startswith("file ")]
    assert listed == [f"file '{p.resolve()}'" for p in sources]
    assert result.output_path.read_bytes() == b"".join(p.read_bytes() for p in sources)
    assert result.size_bytes == result.output_path.stat().st_size
    assert result.output_path.name == "album_merged.mp4" and result.source_count == 3
    # Only the merged file is left behind (no concat list).
    assert [p.name for p in (tmp_path / "out").iterdir()] == ["album_merged.mp4"]


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [2, 5, 10, 20, 50])
async def test_any_number_of_videos_gives_exactly_one_output(tmp_path, fake_tools, count):
    sources = make_sources(tmp_path / "src", [f"video{i}.mp4" for i in range(1, count + 1)])

    result = await fake_tools.merger().merge(sources, tmp_path / "out")

    assert result.source_count == count
    assert len(fake_tools.calls()) == 1                      # ONE ffmpeg run
    assert len(list((tmp_path / "out").iterdir())) == 1       # ONE output file
    listed = [l for l in fake_tools.calls()[0]["list"].splitlines() if l.startswith("file ")]
    assert len(listed) == count
    assert result.size_bytes == sum(p.stat().st_size for p in sources)


@pytest.mark.asyncio
async def test_tricky_file_names_are_handled(tmp_path, fake_tools):
    sources = make_sources(tmp_path / "src", TRICKY_NAMES)

    result = await fake_tools.merger().merge(sources, tmp_path / "out")

    assert result.output_path.read_bytes() == b"".join(p.read_bytes() for p in sources)


@pytest.mark.asyncio
async def test_line_break_in_a_name_goes_through_a_symlink_alias(tmp_path, fake_tools):
    sources = make_sources(tmp_path / "src", ["ok1.mp4", "line\nbreak.mp4", "ok3.mp4"])

    result = await fake_tools.merger().merge(sources, tmp_path / "out")

    assert result.output_path.read_bytes() == b"".join(p.read_bytes() for p in sources)
    assert [p.name for p in (tmp_path / "out").iterdir()] == [result.output_path.name]  # alias removed


@pytest.mark.asyncio
async def test_incompatible_sources_are_rejected_not_reencoded(tmp_path, fake_tools):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4", "video3.mp4"])
    (tmp_path / "src" / "video2.mp4.probe.json").write_text(json.dumps(probe_json(width=1280, height=720)))

    with pytest.raises(IncompatibleVideosError, match="width"):
        await fake_tools.merger().merge(sources, tmp_path / "out")

    assert fake_tools.calls() == []                          # ffmpeg never started: no re-encode fallback
    assert not (tmp_path / "out").exists() or list((tmp_path / "out").iterdir()) == []


@pytest.mark.asyncio
async def test_unreadable_source_is_a_probe_failure(tmp_path, fake_tools, monkeypatch):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4"])
    monkeypatch.setenv("FAKE_FFPROBE_FAIL", "video2.mp4")

    with pytest.raises(VideoMergeError, match="video2.mp4") as info:
        await fake_tools.merger().merge(sources, tmp_path / "out")

    assert info.value.reason == "probe_failed" and fake_tools.calls() == []


@pytest.mark.asyncio
async def test_ffmpeg_failure_removes_partial_output(tmp_path, fake_tools, monkeypatch):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4"])
    monkeypatch.setenv("FAKE_FFMPEG_MODE", "fail")

    with pytest.raises(VideoMergeError, match="FFmpeg failed") as info:
        await fake_tools.merger().merge(sources, tmp_path / "out")

    assert info.value.reason == "ffmpeg_failed"
    assert list((tmp_path / "out").iterdir()) == []          # partial file + concat list gone


@pytest.mark.asyncio
async def test_ffmpeg_that_writes_nothing_is_a_failure(tmp_path, fake_tools, monkeypatch):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4"])
    monkeypatch.setenv("FAKE_FFMPEG_MODE", "nooutput")

    with pytest.raises(VideoMergeError, match="no output"):
        await fake_tools.merger().merge(sources, tmp_path / "out")


@pytest.mark.asyncio
async def test_missing_ffmpeg_is_handled_safely(tmp_path, fake_tools):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4"])
    merger = VideoMerger(ffmpeg_bin=str(tmp_path / "no-such-ffmpeg"), ffprobe_bin=fake_tools.ffprobe)

    with pytest.raises(FFmpegUnavailableError, match="ffmpeg is not installed"):
        await merger.merge(sources, tmp_path / "out")


@pytest.mark.asyncio
async def test_missing_ffprobe_is_handled_safely(tmp_path, fake_tools):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4"])
    merger = VideoMerger(ffmpeg_bin=fake_tools.ffmpeg, ffprobe_bin=str(tmp_path / "no-such-ffprobe"))

    with pytest.raises(FFmpegUnavailableError, match="ffprobe is not installed"):
        await merger.merge(sources, tmp_path / "out")
    assert fake_tools.calls() == []


@pytest.mark.asyncio
async def test_non_executable_binary_is_reported_as_unavailable(tmp_path, fake_tools):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4"])
    broken = tmp_path / "broken-ffmpeg"
    broken.write_text("#!/nonexistent/interpreter\n")
    broken.chmod(0o755)
    merger = VideoMerger(ffmpeg_bin=str(broken), ffprobe_bin=fake_tools.ffprobe)

    with pytest.raises(FFmpegUnavailableError):
        await merger.merge(sources, tmp_path / "out")


@pytest.mark.asyncio
async def test_cancel_terminates_ffmpeg_and_removes_partial_output(tmp_path, fake_tools, monkeypatch):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4"])
    monkeypatch.setenv("FAKE_FFMPEG_MODE", "hang")
    cancel = {"flag": False}

    task = asyncio.create_task(
        fake_tools.merger().merge(sources, tmp_path / "out", should_cancel=lambda: cancel["flag"])
    )
    await wait_for_file(fake_tools.pid_file)
    pid = int(fake_tools.pid_file.read_text())
    assert pid_alive(pid)
    cancel["flag"] = True

    with pytest.raises(MergeCancelledError):
        await asyncio.wait_for(task, timeout=10)

    assert not pid_alive(pid)                                # no orphan ffmpeg
    assert list((tmp_path / "out").iterdir()) == []          # partial file removed


@pytest.mark.asyncio
async def test_cancel_kills_ffmpeg_that_ignores_sigterm(tmp_path, fake_tools, monkeypatch):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4"])
    monkeypatch.setenv("FAKE_FFMPEG_MODE", "hang_ignore_term")
    cancel = {"flag": False}
    merger = fake_tools.merger()
    merger.terminate_grace = 0.3

    task = asyncio.create_task(merger.merge(sources, tmp_path / "out", should_cancel=lambda: cancel["flag"]))
    await wait_for_file(fake_tools.pid_file)
    pid = int(fake_tools.pid_file.read_text())
    cancel["flag"] = True

    with pytest.raises(MergeCancelledError):
        await asyncio.wait_for(task, timeout=10)

    assert not pid_alive(pid)
    assert list((tmp_path / "out").iterdir()) == []


@pytest.mark.asyncio
async def test_task_cancellation_leaves_no_orphan_ffmpeg(tmp_path, fake_tools, monkeypatch):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4"])
    monkeypatch.setenv("FAKE_FFMPEG_MODE", "hang")

    task = asyncio.create_task(fake_tools.merger().merge(sources, tmp_path / "out"))
    await wait_for_file(fake_tools.pid_file)
    pid = int(fake_tools.pid_file.read_text())

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    for _ in range(100):
        if not pid_alive(pid):
            break
        await asyncio.sleep(0.05)
    assert not pid_alive(pid)
    assert list((tmp_path / "out").iterdir()) == []


@pytest.mark.asyncio
async def test_merge_timeout_stops_ffmpeg(tmp_path, fake_tools, monkeypatch):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4"])
    monkeypatch.setenv("FAKE_FFMPEG_MODE", "hang")
    merger = fake_tools.merger(merge_timeout=0.5)

    with pytest.raises(VideoMergeError, match="did not finish"):
        await merger.merge(sources, tmp_path / "out")

    assert not pid_alive(int(fake_tools.pid_file.read_text()))
    assert list((tmp_path / "out").iterdir()) == []


@pytest.mark.asyncio
async def test_cancel_before_start_runs_nothing(tmp_path, fake_tools):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4"])

    with pytest.raises(MergeCancelledError):
        await fake_tools.merger().merge(sources, tmp_path / "out", should_cancel=lambda: True)

    assert fake_tools.calls() == []


@pytest.mark.asyncio
async def test_oversized_merge_is_discarded(tmp_path, fake_tools, monkeypatch):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4"])
    monkeypatch.setenv("FAKE_FFMPEG_MODE", "big")
    monkeypatch.setenv("FAKE_FFMPEG_SIZE", "5000")

    with pytest.raises(MergedTooLargeError, match="size limit"):
        await fake_tools.merger().merge(sources, tmp_path / "out", max_output_bytes=1000)

    assert list((tmp_path / "out").iterdir()) == []
    # ffmpeg was told to stop writing right past the limit (bounded disk use).
    args = fake_tools.calls()[0]["args"]
    assert args[args.index("-fs") + 1] == "1001"


@pytest.mark.asyncio
async def test_merged_file_exactly_at_the_limit_is_accepted(tmp_path, fake_tools, monkeypatch):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4"])
    monkeypatch.setenv("FAKE_FFMPEG_MODE", "big")
    monkeypatch.setenv("FAKE_FFMPEG_SIZE", "1000")

    result = await fake_tools.merger().merge(sources, tmp_path / "out", max_output_bytes=1000)

    assert result.size_bytes == 1000


@pytest.mark.asyncio
async def test_not_enough_disk_space_is_reported_before_merging(tmp_path, fake_tools, monkeypatch):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4"])
    usage = collections.namedtuple("usage", "total used free")(100, 99, 1)
    monkeypatch.setattr(shutil, "disk_usage", lambda path: usage)

    with pytest.raises(VideoMergeError, match="free disk space") as info:
        await fake_tools.merger().merge(sources, tmp_path / "out")

    assert info.value.reason == "disk_space" and fake_tools.calls() == []


@pytest.mark.asyncio
async def test_fewer_than_two_sources_is_refused(tmp_path, fake_tools):
    sources = make_sources(tmp_path / "src", ["video1.mp4"])
    with pytest.raises(VideoMergeError, match="At least two"):
        await fake_tools.merger().merge(sources, tmp_path / "out")


@pytest.mark.asyncio
async def test_output_never_lands_in_the_source_directory(tmp_path, fake_tools):
    sources = make_sources(tmp_path / "src", ["video1.mp4", "video2.mp4"])
    before = sorted(p.name for p in (tmp_path / "src").iterdir())

    await fake_tools.merger().merge(sources, tmp_path / "out")

    assert sorted(p.name for p in (tmp_path / "src").iterdir()) == before


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #


def _env_settings(monkeypatch, **env: str) -> Settings:
    monkeypatch.setenv("API_ID", "12345")
    monkeypatch.setenv("API_HASH", "0123456789abcdef0123456789abcdef")
    monkeypatch.delenv("MERGE_VIDEOS", raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return Settings.from_env(None)


def test_merge_videos_defaults_to_false(monkeypatch, settings):
    assert settings.merge_videos is False
    assert _env_settings(monkeypatch).merge_videos is False


@pytest.mark.parametrize("raw, expected", [("true", True), ("TRUE", True), ("false", False), ("False", False), ("", False)])
def test_merge_videos_env_values(monkeypatch, raw, expected):
    assert _env_settings(monkeypatch, MERGE_VIDEOS=raw).merge_videos is expected


def test_merge_videos_is_documented():
    root = Path(__file__).resolve().parent.parent
    assert "MERGE_VIDEOS=false" in (root / ".env.example").read_text(encoding="utf-8")
    readme = (root / "README.md").read_text(encoding="utf-8")
    assert "### Merge Videos" in readme and "-c copy" in readme
    docker = (root / "Dockerfile").read_text(encoding="utf-8")
    assert "ffmpeg" in docker


# --------------------------------------------------------------------------- #
# PipelineWorker integration
# --------------------------------------------------------------------------- #


class FakeMerger:
    """Stands in for VideoMerger inside the worker (records the call)."""

    def __init__(self, *, size: int | None = None, error: BaseException | None = None) -> None:
        self.size, self.error = size, error
        self.calls: list[dict[str, Any]] = []

    async def merge(self, sources, output_dir, *, output_stem="merged", max_output_bytes=None, should_cancel=None):
        self.calls.append(
            {"sources": [Path(s) for s in sources], "output_dir": Path(output_dir), "stem": output_stem,
             "max": max_output_bytes}
        )
        if self.error is not None:
            raise self.error
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        out = output_dir / f"{output_stem}.mp4"
        size = self.size if self.size is not None else sum(Path(s).stat().st_size for s in sources)
        with open(out, "wb") as handle:
            handle.truncate(size)
        return MergeResult(out, size, len(sources), ("ffmpeg", "-c", "copy"), "mov,mp4,m4a,3gp,3g2,mj2")


class DiskWalaBot:
    """Video bot fake that answers with a DiskWala URL per uploaded file."""

    def __init__(self) -> None:
        self.calls: list[Path] = []

    async def upload(self, path: Path, *, client=None, should_cancel=None) -> dict[str, Any]:
        self.calls.append(Path(path))
        return {
            "provider": "video_bot",
            "bot": "DiskWalaFileUploaderBot",
            "provider_name": "diskwala",
            "url": f"{DISKWALA}{Path(path).stem}",
        }


def merge_settings(settings: Settings, **kw: Any) -> Settings:
    kw.setdefault("merge_videos", True)
    kw.setdefault("max_archive_size_mb", 50)
    kw.setdefault("max_extracted_size_mb", 50)
    return dataclasses.replace(settings, **kw)


def make_merge_worker(settings, *, merger=None, video=None, imgbb=None, telegraph=None):
    manager = JobManager(settings.job_dir)
    imgbb, telegraph = imgbb or FakeImgBB(), telegraph or FakePublisher()
    video = video or DiskWalaBot()
    kwargs = {"video_merger": merger} if merger is not None else {}
    worker = PipelineWorker(
        settings, manager, imgbb_uploader=imgbb, telegraph_publisher=telegraph, video_uploader=video, **kwargs
    )
    return worker, manager, imgbb, telegraph, video


def video_files(tmp_path: Path, names: list[str], size: int = 10):
    directory = tmp_path / "extracted"
    directory.mkdir(parents=True, exist_ok=True)
    return [media(write(directory / n, size + i), "video") for i, n in enumerate(names)]


def finish(manager, job):
    manager.complete_with_errors(job.job_id) if job.upload_failures else manager.complete(job.job_id)


def render(job, template: str | None) -> str:
    return ProgressRenderer(final_post_template=template).render(job)


@pytest.mark.asyncio
async def test_merge_disabled_keeps_the_current_behaviour(settings, tmp_path):
    merger = FakeMerger()
    worker, manager, _, _, video = make_merge_worker(settings, merger=merger, video=FakeVideoBot())
    job = make_job(manager)
    prep(manager, job)
    videos = video_files(tmp_path, ["v3.mp4", "v1.mp4", "v2.mp4"])

    await worker._upload_media(job, [], videos)

    assert merger.calls == []                                           # no merge
    assert video.calls == [m.path for m in videos]                      # each video, in order
    assert [r.filename for r in job.upload_results] == ["v3.mp4", "v1.mp4", "v2.mp4"]
    assert "video_merge" not in job.metadata
    assert job.video_count == 0 and job.upload_failures == []


@pytest.mark.asyncio
async def test_merge_enabled_with_zero_videos_changes_nothing(settings, tmp_path):
    settings = merge_settings(settings)
    merger = FakeMerger()
    worker, manager, imgbb, telegraph, video = make_merge_worker(settings, merger=merger)
    job = make_job(manager)
    prep(manager, job)
    img = write(tmp_path / "a.jpg", 10)

    await worker._upload_media(job, [media(img, "image")], [])

    assert merger.calls == [] and video.calls == []
    assert job.metadata["video_processing"] == "not_required"
    assert imgbb.calls == [img]
    assert telegraph.calls == [("album", ["https://imgbb.test/a.jpg"])]   # Telegraph/images untouched


@pytest.mark.asyncio
async def test_merge_enabled_with_one_video_sends_the_original(settings, tmp_path):
    settings = merge_settings(settings)
    merger = FakeMerger()
    worker, manager, _, _, video = make_merge_worker(settings, merger=merger)
    job = make_job(manager)
    prep(manager, job)
    (only,) = video_files(tmp_path, ["solo.mp4"])

    await worker._upload_media(job, [], [only])

    assert merger.calls == []                       # no unnecessary merge
    assert video.calls == [only.path]               # the ORIGINAL file
    assert [r.filename for r in job.upload_results] == ["solo.mp4"]
    assert "video_merge" not in job.metadata


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [2, 5, 10, 20, 50])
async def test_n_videos_are_merged_into_one_upload_and_one_result(settings, tmp_path, count):
    settings = merge_settings(settings, video_max_size_gb=1.5)
    merger = FakeMerger()
    worker, manager, _, _, video = make_merge_worker(settings, merger=merger)
    job = make_job(manager)
    prep(manager, job)
    names = [f"video{i}.mp4" for i in range(1, count + 1)]
    videos = video_files(tmp_path, names)

    await worker._upload_media(job, [], videos)

    # ONE merge over ALL sources, in the pipeline's order.
    assert len(merger.calls) == 1
    assert merger.calls[0]["sources"] == [m.path for m in videos]
    assert merger.calls[0]["max"] == settings.video_max_size_bytes
    # The merged file lives in the job's own work directory.
    assert merger.calls[0]["output_dir"] == Path(job.extract_dir).parent / "merged"
    # ONE upload (the merged file), ONE result, ONE URL.
    assert len(video.calls) == 1 and video.calls[0].parent == merger.calls[0]["output_dir"]
    assert not set(video.calls) & {m.path for m in videos}
    (result,) = job.upload_results
    assert result.size_bytes == sum(m.path.stat().st_size for m in videos)   # merged size retained
    assert result.url == f"{DISKWALA}{Path(video.calls[0]).stem}"            # one DiskWala URL
    assert result.extra["merged_from"] == count
    assert job.upload_failures == []
    assert job.video_count == 1
    assert job.metadata["video_merge"]["status"] == "done"
    assert job.metadata["video_merge"]["source_count"] == count
    assert [s["status"] for s in job.metadata["video_status"]] == ["done"]


@pytest.mark.asyncio
async def test_merge_preserves_the_pipeline_order_not_alphabetical(settings, tmp_path):
    settings = merge_settings(settings)
    merger = FakeMerger()
    worker, manager, *_ = make_merge_worker(settings, merger=merger)
    job = make_job(manager)
    prep(manager, job)
    videos = video_files(tmp_path, ["z.mp4", "a.mp4", "m.mp4"])     # scanner order, not sorted by name

    await worker._upload_media(job, [], videos)

    assert [p.name for p in merger.calls[0]["sources"]] == ["z.mp4", "a.mp4", "m.mp4"]


@pytest.mark.asyncio
async def test_incompatible_videos_fail_safely_without_any_upload(settings, tmp_path):
    settings = merge_settings(settings)
    error = IncompatibleVideosError("video stream 0: 'width' differs (a.mp4: 1920, b.mp4: 1280)")
    merger = FakeMerger(error=error)
    worker, manager, _, _, video = make_merge_worker(settings, merger=merger)
    job = make_job(manager)
    prep(manager, job)

    await worker._upload_media(job, [], video_files(tmp_path, ["a.mp4", "b.mp4", "c.mp4"]))

    assert video.calls == []                                           # nothing uploaded, not even singles
    (failure,) = job.upload_failures
    assert failure.provider == "video_bot" and failure.media_type == "video"
    assert "not re-encoded" in failure.error and "width" in failure.error
    assert failure.extra["merge_reason"] == "incompatible"
    assert job.upload_results == []
    assert job.metadata["video_merge"]["status"] == "failed"
    assert job.metadata["video_status"][0]["status"] == "failed"
    assert job.video_count == 1
    finish(manager, job)
    assert job.status == JobStatus.COMPLETED_WITH_ERRORS


@pytest.mark.asyncio
async def test_ffmpeg_failure_marks_the_video_failed_and_does_not_upload(settings, tmp_path):
    settings = merge_settings(settings)
    merger = FakeMerger(error=VideoMergeError("FFmpeg failed: boom", reason="ffmpeg_failed"))
    worker, manager, _, telegraph, video = make_merge_worker(settings, merger=merger)
    job = make_job(manager)
    prep(manager, job)
    img = write(tmp_path / "a.jpg", 10)

    await worker._upload_media(job, [media(img, "image")], video_files(tmp_path, ["a.mp4", "b.mp4"]))

    assert video.calls == []
    assert [f.error for f in job.upload_failures] == ["Video merge failed: FFmpeg failed: boom"]
    # The rest of the job (images/Telegraph) still finished.
    assert telegraph.calls == [("album", ["https://imgbb.test/a.jpg"])]
    finish(manager, job)
    assert job.status == JobStatus.COMPLETED_WITH_ERRORS


@pytest.mark.asyncio
async def test_unexpected_merger_exception_does_not_crash_the_worker(settings, tmp_path):
    settings = merge_settings(settings)
    worker, manager, _, _, video = make_merge_worker(settings, merger=FakeMerger(error=RuntimeError("kaboom")))
    job = make_job(manager)
    prep(manager, job)

    await worker._upload_media(job, [], video_files(tmp_path, ["a.mp4", "b.mp4"]))

    assert video.calls == []
    assert "RuntimeError: kaboom" in job.upload_failures[0].error


@pytest.mark.asyncio
async def test_missing_ffmpeg_in_the_worker_is_a_clean_failure(settings, tmp_path):
    settings = merge_settings(settings)
    merger = VideoMerger(ffmpeg_bin=str(tmp_path / "missing-ffmpeg"), ffprobe_bin=str(tmp_path / "missing-ffprobe"))
    worker, manager, _, _, video = make_merge_worker(settings, merger=merger)
    job = make_job(manager)
    prep(manager, job)

    await worker._upload_media(job, [], video_files(tmp_path, ["a.mp4", "b.mp4"]))

    assert video.calls == []
    assert "not installed" in job.upload_failures[0].error
    finish(manager, job)
    assert job.status == JobStatus.COMPLETED_WITH_ERRORS


@pytest.mark.asyncio
async def test_cancelled_merge_raises_job_cancelled_and_uploads_nothing(settings, tmp_path):
    settings = merge_settings(settings)
    worker, manager, _, _, video = make_merge_worker(settings, merger=FakeMerger(error=MergeCancelledError()))
    job = make_job(manager)
    prep(manager, job)

    with pytest.raises(JobCancelled):
        await worker._upload_media(job, [], video_files(tmp_path, ["a.mp4", "b.mp4"]))

    assert video.calls == [] and job.upload_failures == []
    assert job.metadata["video_merge"]["status"] == "cancelled"


@pytest.mark.asyncio
async def test_merged_file_over_the_limit_is_not_uploaded(settings, tmp_path):
    settings = merge_settings(settings, video_max_size_gb=0.00001)      # ~10 KB
    merger = FakeMerger(error=MergedTooLargeError("Merged video exceeds the size limit (> 10737 bytes)"))
    worker, manager, _, _, video = make_merge_worker(settings, merger=merger)
    job = make_job(manager)
    prep(manager, job)

    await worker._upload_media(job, [], video_files(tmp_path, ["a.mp4", "b.mp4"]))

    assert video.calls == []
    assert "size limit" in job.upload_failures[0].error
    assert job.metadata["video_merge"]["reason"] == "too_large"


@pytest.mark.asyncio
async def test_existing_size_check_still_guards_a_merger_that_returns_too_much(settings, tmp_path):
    settings = merge_settings(settings, video_max_size_gb=0.00001)      # ~10 KB
    merger = FakeMerger(size=50_000)                                    # merger did not enforce the limit
    worker, manager, _, _, video = make_merge_worker(settings, merger=merger)
    job = make_job(manager)
    prep(manager, job)

    await worker._upload_media(job, [], video_files(tmp_path, ["a.mp4", "b.mp4"]))

    assert video.calls == []
    assert "exceeds the size limit" in job.upload_failures[0].error
    assert job.metadata["video_status"][0]["status"] == "failed"


@pytest.mark.asyncio
async def test_real_merger_oversize_in_worker(settings, tmp_path, fake_tools, monkeypatch):
    settings = merge_settings(settings, video_max_size_gb=0.00001)
    monkeypatch.setenv("FAKE_FFMPEG_MODE", "big")
    monkeypatch.setenv("FAKE_FFMPEG_SIZE", "50000")
    worker, manager, _, _, video = make_merge_worker(settings, merger=fake_tools.merger())
    job = make_job(manager)
    prep(manager, job)

    await worker._upload_media(job, [], video_files(tmp_path, ["a.mp4", "b.mp4"]))

    assert video.calls == []
    assert "size limit" in job.upload_failures[0].error
    assert list((Path(job.extract_dir).parent / "merged").iterdir()) == []


@pytest.mark.asyncio
async def test_real_merger_ffmpeg_failure_in_worker_never_uploads_the_broken_file(
    settings, tmp_path, fake_tools, monkeypatch
):
    settings = merge_settings(settings)
    monkeypatch.setenv("FAKE_FFMPEG_MODE", "fail")
    worker, manager, _, _, video = make_merge_worker(settings, merger=fake_tools.merger())
    job = make_job(manager)
    prep(manager, job)

    await worker._upload_media(job, [], video_files(tmp_path, ["a.mp4", "b.mp4"]))

    assert video.calls == []
    assert "FFmpeg failed" in job.upload_failures[0].error
    assert list((Path(job.extract_dir).parent / "merged").iterdir()) == []


# ---- full pipeline (ZIP -> extract -> scan -> merge -> one DiskWala URL -> post) ----

POST_TEMPLATE = (
    "𝗘𝘅𝗰𝗹𝘂𝘀𝗶𝘃𝗲 𝗩𝗶𝗱𝗲𝗼𝘀 😍\n\n𝗣𝗵𝗼𝘁𝗼𝘀 👉 {telegraph_url}\n\n𝗩𝗶𝗱𝗲𝗼𝘀 👇\n\n{video_links}"
)


def zip_job(settings, tmp_path, entries, *, merger=None, video=None, imgbb=None, telegraph=None):
    worker, manager, imgbb, telegraph, video = make_merge_worker(
        settings, merger=merger, video=video, imgbb=imgbb, telegraph=telegraph
    )
    zip_path = build_zip(tmp_path / "album.zip", entries)
    job = manager.create_job(user_id=1, chat_id=1, archive_name="album.zip", message_id=1)
    job._telegram_message = ZipMessage(zip_path, None)
    job._status_message = StatusMessage()
    manager.mark_queued(job.job_id)
    return worker, manager, job, imgbb, telegraph, video


def ten_video_entries(extra: dict[str, bytes] | None = None) -> dict[str, bytes]:
    entries = {f"clips/video{i}.mp4": bytes([64 + i]) * (100 + i) for i in range(1, 11)}
    entries.update(extra or {})
    return entries


@pytest.mark.asyncio
async def test_ten_videos_zip_gives_one_diskwala_url_in_the_final_post(settings, tmp_path):
    settings = merge_settings(settings, final_post_template=POST_TEMPLATE)
    merger = FakeMerger()
    entries = ten_video_entries({"pics/1.jpg": b"\xff\xd8one", "pics/2.png": b"\x89PNGtwo"})
    worker, manager, job, imgbb, telegraph, video = zip_job(settings, tmp_path, entries, merger=merger)

    await worker._process(job)

    # 10 source videos -> one merge -> ONE upload -> ONE result.
    assert job.video_count == 1 and job.image_count == 2
    assert len(merger.calls) == 1 and len(merger.calls[0]["sources"]) == 10
    assert [p.name for p in merger.calls[0]["sources"]] == [f"video{i}.mp4" for i in range(1, 11)]  # natural order
    assert len(video.calls) == 1
    video_results = [r for r in job.upload_results if r.media_type == "video"]
    assert len(video_results) == 1 and video_results[0].size_bytes == sum(100 + i for i in range(1, 11))
    # ImgBB / Telegraph never saw a video.
    assert sorted(p.name for p in imgbb.calls) == ["1.jpg", "2.png"]
    assert telegraph.calls == [("album", ["https://imgbb.test/1.jpg", "https://imgbb.test/2.png"])]
    assert not any(".mp4" in str(c) for c in imgbb.calls + telegraph.calls)

    # The final post holds exactly ONE DiskWala link and no source video.
    final = job._status_message.edits[-1]
    assert final == (
        "𝗘𝘅𝗰𝗹𝘂𝘀𝗶𝘃𝗲 𝗩𝗶𝗱𝗲𝗼𝘀 😍\n\n𝗣𝗵𝗼𝘁𝗼𝘀 👉 https://telegra.ph/album\n\n𝗩𝗶𝗱𝗲𝗼𝘀 👇\n\n"
        f"{video_results[0].url}"
    )
    assert final.count("diskwala.com") == 1
    assert job.status == JobStatus.COMPLETED
    # Temporary files (including the merged file) are cleaned as usual.
    assert not Path(job.extract_dir).parent.exists()


@pytest.mark.asyncio
async def test_final_post_video_links_is_exactly_one_diskwala_url(settings, tmp_path):
    settings = merge_settings(settings, final_post_template="{video_links}|{video_count}")
    entries = ten_video_entries()
    worker, manager, job, *_ = zip_job(settings, tmp_path, entries, merger=FakeMerger(), video=DiskWalaBot())

    await worker._process(job)

    final = job._status_message.edits[-1]
    assert final == f"{DISKWALA}album_merged|1"
    assert final.count("diskwala.com") == 1 and "video1" not in final


@pytest.mark.asyncio
async def test_keep_job_files_keeps_the_merged_file(settings, tmp_path):
    settings = merge_settings(settings, keep_job_files=True)
    worker, manager, job, *_ = zip_job(settings, tmp_path, ten_video_entries(), merger=FakeMerger(), video=DiskWalaBot())

    await worker._process(job)

    assert [p.name for p in (Path(job.extract_dir).parent / "merged").iterdir()] == ["album_merged.mp4"]


@pytest.mark.asyncio
async def test_non_merge_mode_post_formatting_and_ordering_are_unchanged(settings, tmp_path):
    settings = merge_settings(settings, merge_videos=False, final_post_template="{video_links}")
    entries = {"a.mp4": b"A" * 100, "b.mp4": b"B" * 300, "c.mp4": b"C" * 200}
    worker, manager, job, *_ = zip_job(settings, tmp_path, entries, merger=FakeMerger(), video=DiskWalaBot())

    await worker._process(job)

    # Size-descending, blank line between links, all three individual DiskWala URLs.
    assert job._status_message.edits[-1] == f"{DISKWALA}b\n\n{DISKWALA}c\n\n{DISKWALA}a"


@pytest.mark.asyncio
async def test_non_merge_mode_uploads_each_video_in_scanner_order(settings, tmp_path):
    settings = merge_settings(settings, merge_videos=False)
    entries = {"v10.mp4": b"x", "v2.mp4": b"x", "v1.mp4": b"x"}
    merger = FakeMerger()
    worker, manager, job, _, _, video = zip_job(settings, tmp_path, entries, merger=merger, video=DiskWalaBot())

    await worker._process(job)

    assert [p.name for p in video.calls] == ["v1.mp4", "v2.mp4", "v10.mp4"]   # natural order, as before
    assert merger.calls == []


@pytest.mark.asyncio
async def test_title_handling_is_identical_with_and_without_merge(settings, tmp_path):
    results = {}
    for merging in (False, True):
        sub = tmp_path / str(merging)
        sub.mkdir()
        cfg = merge_settings(settings, merge_videos=merging, final_post_template="{title}|{archive_name}|{telegraph_url}")
        entries = ten_video_entries({"pics/1.jpg": b"\xff\xd8one"})
        worker, manager, job, imgbb, telegraph, video = zip_job(
            cfg, sub, entries, merger=FakeMerger(), video=DiskWalaBot()
        )
        await worker._process(job)
        results[merging] = (telegraph.calls[0][0], job._status_message.edits[-1])

    assert results[True] == results[False]                              # Telegraph title + {title} unchanged
    assert results[True][0] == resolve_article_title(None, "album.zip")


@pytest.mark.asyncio
async def test_progress_shows_a_merging_status_and_one_video(settings, tmp_path):
    settings = merge_settings(settings)
    seen: list[str] = []

    class SpyMerger(FakeMerger):
        async def merge(self, sources, output_dir, **kw):
            seen.append(job_holder["job"]._status_message.edits[-1])
            return await super().merge(sources, output_dir, **kw)

    job_holder: dict[str, Any] = {}
    worker, manager, job, *_ = zip_job(settings, tmp_path, ten_video_entries(), merger=SpyMerger())
    job_holder["job"] = job

    await worker._process(job)

    assert "Merging videos (no re-encoding)" in seen[0] and "(10 videos)" in seen[0]
    assert job.video_count == 1


# ---- full pipeline with the REAL merger and fake binaries: cancellation ----


@pytest.mark.asyncio
async def test_startup_logs_an_error_but_does_not_crash_when_ffmpeg_is_missing(settings, monkeypatch, caplog):
    settings = merge_settings(settings)
    monkeypatch.setattr(shutil, "which", lambda name: None)
    worker, *_ = make_merge_worker(settings, merger=FakeMerger())

    await worker.start()
    await worker.stop()

    assert "MERGE_VIDEOS=true but ffmpeg and ffprobe not found" in caplog.text


@pytest.mark.asyncio
async def test_cancel_during_merge_stops_ffmpeg_cleans_up_and_never_uploads(
    settings, tmp_path, fake_tools, monkeypatch
):
    settings = merge_settings(settings, keep_job_files=True)      # keep files so we can inspect the work dir
    monkeypatch.setenv("FAKE_FFMPEG_MODE", "hang")
    worker, manager, job, imgbb, telegraph, video = zip_job(
        settings, tmp_path, ten_video_entries({"pics/1.jpg": b"\xff\xd8one"}), merger=fake_tools.merger()
    )

    task = asyncio.create_task(worker._process(job))
    await wait_for_file(fake_tools.pid_file)
    pid = int(fake_tools.pid_file.read_text())
    assert pid_alive(pid)
    manager.cancel(job.job_id)
    await asyncio.wait_for(task, timeout=15)

    assert job.status == JobStatus.CANCELLED                          # existing terminal state
    assert not pid_alive(pid)                                         # FFmpeg terminated, no orphan
    assert video.calls == []                                          # nothing sent to DiskWala
    assert list((Path(job.extract_dir).parent / "merged").iterdir()) == []   # partial output removed
    assert "cancelled" in job._status_message.edits[-1].lower()


@pytest.mark.asyncio
async def test_application_shutdown_during_merge_leaves_no_orphan(settings, tmp_path, fake_tools, monkeypatch):
    settings = merge_settings(settings)
    monkeypatch.setenv("FAKE_FFMPEG_MODE", "hang")
    worker, manager, job, imgbb, telegraph, video = zip_job(
        settings, tmp_path, ten_video_entries(), merger=fake_tools.merger()
    )

    task = asyncio.create_task(worker._process(job))
    await wait_for_file(fake_tools.pid_file)
    pid = int(fake_tools.pid_file.read_text())
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    for _ in range(100):
        if not pid_alive(pid):
            break
        await asyncio.sleep(0.05)
    assert not pid_alive(pid) and video.calls == []
    assert job.status == JobStatus.CANCELLED


@pytest.mark.asyncio
async def test_incompatible_zip_end_to_end_uploads_nothing(settings, tmp_path, fake_tools):
    settings = merge_settings(settings, final_post_template="{video_links}")
    entries = {"v1.mp4": b"A" * 20, "v2.mp4": b"B" * 20, "pics/1.jpg": b"\xff\xd8one"}

    class Marking(VideoMerger):
        """v2 is 720p while v1 is 1080p (the fake ffprobe reads a sidecar next to the video)."""

        async def _probe(self, ffprobe, path):
            if path.name == "v2.mp4":
                Path(str(path) + ".probe.json").write_text(json.dumps(probe_json(width=1280, height=720)))
            return await super()._probe(ffprobe, path)

    merger = Marking(ffmpeg_bin=fake_tools.ffmpeg, ffprobe_bin=fake_tools.ffprobe, poll_interval=0.05)
    worker, manager, job, imgbb, telegraph, video = zip_job(settings, tmp_path, entries, merger=merger)

    await worker._process(job)

    assert fake_tools.calls() == []                                    # never re-encoded / merged
    assert video.calls == []                                           # never uploaded individually
    assert job.status == JobStatus.COMPLETED_WITH_ERRORS
    assert telegraph.calls                                             # image workflow untouched
    assert job.upload_failures and "not re-encoded" in job.upload_failures[0].error


# --------------------------------------------------------------------------- #
# Real FFmpeg: prove the merge is a stream copy
# --------------------------------------------------------------------------- #


def run(*args: str) -> str:
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def make_clip(path: Path, *, seconds: float = 1.0, size: str = "160x120", rate: int = 25,
              freq: int = 440, audio: bool = True, pix_fmt: str = "yuv420p") -> Path:
    cmd = ["ffmpeg", "-v", "error", "-y", "-f", "lavfi", "-i", f"testsrc=size={size}:rate={rate}:duration={seconds}"]
    if audio:
        cmd += ["-f", "lavfi", "-i", f"sine=frequency={freq}:sample_rate=48000:duration={seconds}"]
    cmd += ["-c:v", "libx264", "-pix_fmt", pix_fmt, "-g", "12"]
    if audio:
        cmd += ["-c:a", "aac", "-ac", "2", "-shortest"]
    subprocess.run([*cmd, str(path)], check=True, capture_output=True)
    return path


def packet_sizes(path: Path, stream: str) -> list[int]:
    out = run("ffprobe", "-v", "error", "-select_streams", stream, "-show_entries", "packet=size", "-of", "csv=p=0", str(path))
    return [int(line.strip(",")) for line in out.split() if line.strip(",")]


def frame_md5s(path: Path) -> list[str]:
    """MD5 of every DECODED video frame (a re-encode would change these)."""
    out = run("ffmpeg", "-v", "error", "-i", str(path), "-map", "0:v:0", "-f", "framemd5", "-")
    return [line.split(",")[-1].strip() for line in out.splitlines() if line and not line.startswith("#")]


def duration_of(path: Path) -> float:
    return float(run("ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path)))


def codec_of(path: Path, stream: str) -> str:
    return run("ffprobe", "-v", "error", "-select_streams", stream, "-show_entries", "stream=codec_name", "-of", "csv=p=0", str(path)).strip()


@needs_ffmpeg
@pytest.mark.asyncio
async def test_real_merge_is_lossless_stream_copy(tmp_path):
    clips = [make_clip(tmp_path / f"video{i}.mp4", freq=300 + 100 * i, seconds=1 + i * 0.5) for i in (1, 2, 3)]

    result = await VideoMerger().merge(clips, tmp_path / "out", output_stem="merged")

    merged = result.output_path
    assert merged.is_file() and result.size_bytes == merged.stat().st_size
    assert abs(duration_of(merged) - sum(duration_of(c) for c in clips)) < 0.3
    assert codec_of(merged, "v:0") == "h264" and codec_of(merged, "a:0") == "aac"

    # Lossless: every decoded video frame equals the source frame (a re-encode would change them),
    # and the audio packets are the source packets, in order.
    assert frame_md5s(merged) == [h for clip in clips for h in frame_md5s(clip)]
    assert packet_sizes(merged, "a:0") == [s for clip in clips for s in packet_sizes(clip, "a:0")]
    assert "-c" in result.command and result.command[result.command.index("-c") + 1] == "copy"


@needs_ffmpeg
@pytest.mark.asyncio
async def test_real_merge_follows_the_given_order(tmp_path):
    a = make_clip(tmp_path / "a.mp4", seconds=1.0, size="160x120")
    b = make_clip(tmp_path / "b.mp4", seconds=2.0, size="160x120")

    forward = await VideoMerger().merge([a, b], tmp_path / "o1", output_stem="ab")
    backward = await VideoMerger().merge([b, a], tmp_path / "o2", output_stem="ba")

    assert frame_md5s(forward.output_path) == frame_md5s(a) + frame_md5s(b)
    assert frame_md5s(backward.output_path) == frame_md5s(b) + frame_md5s(a)


@needs_ffmpeg
@pytest.mark.asyncio
async def test_real_merge_of_twelve_videos_in_natural_order(tmp_path):
    base = make_clip(tmp_path / "base.mp4", seconds=0.5)
    names = [f"video{i}.mp4" for i in range(1, 13)]
    for name in names:
        shutil.copy(base, tmp_path / name)
    from app.media_scanner import scan_directory

    scanned = scan_directory(tmp_path)
    ordered = [m.path for m in scanned.videos if m.filename in names]
    assert [p.name for p in ordered][:3] == ["video1.mp4", "video2.mp4", "video3.mp4"]   # natural, not video1,video10

    result = await VideoMerger().merge(ordered, tmp_path / "out")

    assert result.source_count == 12
    assert abs(duration_of(result.output_path) - 12 * duration_of(base)) < 0.5
    assert len(list((tmp_path / "out").iterdir())) == 1


@needs_ffmpeg
@pytest.mark.asyncio
async def test_real_incompatible_clips_are_rejected_without_output(tmp_path):
    hd = make_clip(tmp_path / "hd.mp4", size="320x240")
    sd = make_clip(tmp_path / "sd.mp4", size="160x120")

    with pytest.raises(IncompatibleVideosError, match="width"):
        await VideoMerger().merge([hd, sd], tmp_path / "out")

    assert not (tmp_path / "out").exists() or list((tmp_path / "out").iterdir()) == []


@needs_ffmpeg
@pytest.mark.asyncio
async def test_real_different_frame_rate_and_silent_clips_are_rejected(tmp_path):
    base = make_clip(tmp_path / "base.mp4")
    other_fps = make_clip(tmp_path / "fps.mp4", rate=30)
    silent = make_clip(tmp_path / "silent.mp4", audio=False)

    with pytest.raises(IncompatibleVideosError, match="r_frame_rate"):
        await VideoMerger().merge([base, other_fps], tmp_path / "o1")
    with pytest.raises(IncompatibleVideosError, match="audio stream count"):
        await VideoMerger().merge([base, silent], tmp_path / "o2")


@needs_ffmpeg
@pytest.mark.asyncio
async def test_real_merge_with_awkward_file_names(tmp_path):
    base = make_clip(tmp_path / "base.mp4", seconds=0.5)
    clips = []
    for i, name in enumerate(["it's [1] & $x.mp4", "ünï 視頻 (2).mp4", 'say "hi" `3`.mp4']):
        target = tmp_path / name
        shutil.copy(base, target)
        clips.append(target)

    result = await VideoMerger().merge(clips, tmp_path / "out")

    assert abs(duration_of(result.output_path) - 3 * duration_of(base)) < 0.3


@needs_ffmpeg
@pytest.mark.asyncio
async def test_real_corrupt_source_fails_cleanly(tmp_path):
    good = make_clip(tmp_path / "good.mp4")
    bad = tmp_path / "bad.mp4"
    bad.write_bytes(b"this is not a video")

    with pytest.raises(VideoMergeError, match="bad.mp4") as info:
        await VideoMerger().merge([good, bad], tmp_path / "out")

    assert info.value.reason == "probe_failed"


@needs_ffmpeg
@pytest.mark.asyncio
async def test_real_oversize_merge_is_discarded(tmp_path):
    clips = [make_clip(tmp_path / f"c{i}.mp4", seconds=2) for i in (1, 2)]
    limit = clips[0].stat().st_size  # smaller than the merged result

    with pytest.raises(MergedTooLargeError):
        await VideoMerger().merge(clips, tmp_path / "out", max_output_bytes=limit)

    assert list((tmp_path / "out").iterdir()) == []


@needs_ffmpeg
@pytest.mark.asyncio
async def test_real_end_to_end_zip_with_real_ffmpeg_one_diskwala_url(settings, tmp_path):
    clips_dir = tmp_path / "make"
    clips_dir.mkdir()
    base = make_clip(clips_dir / "base.mp4", seconds=0.5)
    entries = {f"clips/video{i}.mp4": base.read_bytes() for i in range(1, 11)}
    entries["pics/1.jpg"] = b"\xff\xd8one"
    settings = merge_settings(settings, final_post_template=POST_TEMPLATE, video_max_size_gb=1.5,
                              max_archive_size_mb=50, max_extracted_size_mb=50)
    worker, manager, job, imgbb, telegraph, video = zip_job(
        settings, tmp_path, entries, merger=VideoMerger(), video=DiskWalaBot()
    )
    seen: dict[str, Any] = {}
    original_upload = video.upload

    async def spying_upload(path, **kw):
        seen["duration"] = duration_of(path)
        seen["size"] = Path(path).stat().st_size
        return await original_upload(path, **kw)

    video.upload = spying_upload

    await worker._process(job)

    assert job.status == JobStatus.COMPLETED
    assert len(video.calls) == 1
    assert abs(seen["duration"] - 10 * duration_of(base)) < 0.5
    (result,) = [r for r in job.upload_results if r.media_type == "video"]
    assert result.size_bytes == seen["size"]
    final = job._status_message.edits[-1]
    assert final.count("diskwala.com") == 1 and final.endswith(result.url)
