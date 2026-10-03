"""Lossless multi-video merge (FFmpeg concat demuxer + stream copy).

Used by the pipeline when ``MERGE_VIDEOS=true`` and a ZIP contains two or more
videos. The merge is a *remux*: every stream is copied (``-c copy``), nothing is
re-encoded, so audio/video quality is bit-for-bit what the sources contained.

Stream copy cannot make incompatible files compatible, so before merging every
source is inspected with FFprobe and the merge is **refused** (never silently
re-encoded) when the sources differ in a property that matters for a copy-merge
(codec, resolution, pixel format, frame rate, audio layout, ...).

Design notes
------------
* Everything is asynchronous subprocess work (``asyncio.create_subprocess_exec``)
  with argument lists - no shell, so odd file names are safe.
* Nothing is buffered in RAM: FFmpeg reads/writes files directly and only a
  bounded tail of its stderr is kept for error messages.
* The caller's order is preserved exactly. This module never sorts.
* Cancellation: ``should_cancel`` is polled while FFmpeg runs; on cancellation
  (or task cancellation / timeout) FFmpeg is terminated, then killed if needed,
  and the partial output is deleted.
* No global state: everything lives on the :class:`VideoMerger` instance.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

__all__ = [
    "COMPARED_AUDIO_FIELDS",
    "COMPARED_VIDEO_FIELDS",
    "DISK_SAFETY_MARGIN_BYTES",
    "FFmpegUnavailableError",
    "IncompatibleVideosError",
    "MergeCancelledError",
    "MergeResult",
    "MergedTooLargeError",
    "ProbeInfo",
    "VideoMergeError",
    "VideoMerger",
    "build_concat_list",
    "build_ffmpeg_command",
    "check_compatibility",
    "choose_output_suffix",
    "escape_concat_path",
    "parse_probe",
    "sanitize_stem",
]

logger = logging.getLogger("app.video_merge")

#: Properties that must be identical for a copy-merge to be trusted.
COMPARED_VIDEO_FIELDS: tuple[str, ...] = (
    "codec_name",
    "width",
    "height",
    "pix_fmt",
    "r_frame_rate",
    "sample_aspect_ratio",
    "field_order",
    "profile",
    "level",
    "color_range",
    "color_space",
    "color_transfer",
    "color_primaries",
)
COMPARED_AUDIO_FIELDS: tuple[str, ...] = (
    "codec_name",
    "profile",
    "sample_rate",
    "channels",
    "channel_layout",
)

#: Free disk space kept in reserve on top of the expected output size.
DISK_SAFETY_MARGIN_BYTES = 16 * 1024 * 1024

_STDERR_TAIL_LINES = 12
_MESSAGE_LIMIT = 500


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class VideoMergeError(Exception):
    """The merge could not be completed. ``reason`` is a stable machine code."""

    reason = "merge_failed"

    def __init__(self, message: str, *, reason: str | None = None) -> None:
        super().__init__(message)
        if reason:
            self.reason = reason


class FFmpegUnavailableError(VideoMergeError):
    """ffmpeg or ffprobe is not installed / not executable."""

    reason = "ffmpeg_unavailable"


class IncompatibleVideosError(VideoMergeError):
    """The sources cannot be merged with stream copy (and are NOT re-encoded)."""

    reason = "incompatible"


class MergedTooLargeError(VideoMergeError):
    """The merged file is larger than the configured limit (it was deleted)."""

    reason = "too_large"


class MergeCancelledError(Exception):
    """Cancellation was requested while merging (partial output was removed)."""


# --------------------------------------------------------------------------- #
# Result / probe data
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ProbeInfo:
    """What FFprobe reported about one source (only what the merge needs)."""

    path: Path
    format_name: str
    #: ``(stream_index, {field: value})`` for real video streams (no cover art).
    video: tuple[tuple[int, Mapping[str, Any]], ...]
    audio: tuple[tuple[int, Mapping[str, Any]], ...]
    duration: Optional[float] = None


@dataclass(frozen=True)
class MergeResult:
    """A successful merge."""

    output_path: Path
    size_bytes: int
    source_count: int
    command: tuple[str, ...]
    container: str
    sources: tuple[str, ...] = field(default_factory=tuple)


# --------------------------------------------------------------------------- #
# Pure helpers (unit-testable without FFmpeg)
# --------------------------------------------------------------------------- #


def _clip(text: str, limit: int = _MESSAGE_LIMIT) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def sanitize_stem(value: str, *, default: str = "merged", max_length: int = 80) -> str:
    """A safe file-name stem: no separators, control characters or dots-only."""
    cleaned = re.sub(r"[\x00-\x1f\x7f/\\:*?\"<>|]+", " ", str(value or ""))
    cleaned = " ".join(cleaned.split()).strip(" .")
    cleaned = cleaned[:max_length].strip(" .")
    return cleaned or default


def escape_concat_path(path: str | Path) -> str:
    """Quote a path for an FFmpeg concat list file.

    Inside single quotes everything is literal except the quote itself, which
    is written ``'\\''`` (close quote, escaped quote, reopen quote).
    """
    return "'" + str(path).replace("'", "'\\''") + "'"


def build_concat_list(paths: Sequence[str | Path]) -> str:
    """Concat-demuxer list file content, one ``file '<abs path>'`` per source."""
    lines = ["ffconcat version 1.0"]
    for path in paths:
        text = str(path)
        if "\n" in text or "\r" in text or "\x00" in text:
            raise VideoMergeError(
                "A source path contains a line break/NUL and cannot be listed safely",
                reason="invalid_input",
            )
        lines.append(f"file {escape_concat_path(text)}")
    return "\n".join(lines) + "\n"


def choose_output_suffix(sources: Sequence[Path], format_name: str) -> str:
    """Container extension for the merged file.

    All sources share a container family (checked beforehand). Identical source
    extensions are kept; mixed ones fall back to ``.mkv`` / ``.mp4`` by family.
    """
    suffixes = {Path(p).suffix.lower() for p in sources}
    if len(suffixes) == 1:
        only = next(iter(suffixes))
        if only:
            return only
    family = {part.strip() for part in format_name.split(",")}
    if "matroska" in family:
        return ".mkv"
    if "mov" in family or "mp4" in family:
        return ".mp4"
    first = Path(sources[0]).suffix.lower() if sources else ""
    return first or ".mkv"


def build_ffmpeg_command(
    ffmpeg_bin: str,
    concat_list: Path,
    output: Path,
    stream_indexes: Sequence[int],
    *,
    max_output_bytes: int | None = None,
) -> list[str]:
    """The merge command: concat demuxer in, **stream copy** out.

    No codec, bitrate, quality, filter or scaling option is ever added - the
    only codec option is ``-c copy``. ``-fs`` (file size limit) only bounds disk
    use; a file that hits it is discarded by the caller.
    """
    command = [
        ffmpeg_bin,
        "-hide_banner",
        "-nostdin",
        "-nostats",
        "-loglevel",
        "error",
        "-y",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        str(concat_list),
    ]
    for index in stream_indexes:
        command += ["-map", f"0:{int(index)}"]
    command += ["-c", "copy"]
    if max_output_bytes is not None and max_output_bytes > 0:
        command += ["-fs", str(int(max_output_bytes) + 1)]
    command.append(str(output))
    return command


def _num(value: Any) -> Any:
    """Normalise numeric strings so ``"1920"`` and ``1920`` compare equal."""
    if isinstance(value, str) and re.fullmatch(r"-?\d+", value):
        return int(value)
    return value


def _stream_fields(stream: Mapping[str, Any], fields: Sequence[str]) -> dict[str, Any]:
    return {name: _num(stream.get(name)) for name in fields}


def parse_probe(path: Path, data: Mapping[str, Any]) -> ProbeInfo:
    """Build a :class:`ProbeInfo` from ``ffprobe -print_format json`` output."""
    streams = data.get("streams") or []
    fmt = data.get("format") or {}

    video: list[tuple[int, Mapping[str, Any]]] = []
    audio: list[tuple[int, Mapping[str, Any]]] = []
    for position, stream in enumerate(streams):
        kind = stream.get("codec_type")
        index = int(stream.get("index", position))
        disposition = stream.get("disposition") or {}
        if kind == "video":
            if int(disposition.get("attached_pic", 0) or 0) == 1:
                continue  # cover art is not part of the programme
            video.append((index, _stream_fields(stream, COMPARED_VIDEO_FIELDS)))
        elif kind == "audio":
            audio.append((index, _stream_fields(stream, COMPARED_AUDIO_FIELDS)))

    duration: Optional[float]
    try:
        duration = float(fmt.get("duration")) if fmt.get("duration") is not None else None
    except (TypeError, ValueError):
        duration = None

    return ProbeInfo(
        path=Path(path),
        format_name=str(fmt.get("format_name") or ""),
        video=tuple(video),
        audio=tuple(audio),
        duration=duration,
    )


def check_compatibility(probes: Sequence[ProbeInfo]) -> None:
    """Raise :class:`IncompatibleVideosError` unless a copy-merge is safe.

    Every source is compared with the first one; the error names the properties
    that differ (first few) and the two files involved.
    """
    if len(probes) < 2:
        return
    ref = probes[0]
    if not ref.video:
        raise IncompatibleVideosError(f"{ref.path.name}: no video stream found")

    for other in probes[1:]:
        name = other.path.name
        if not other.video:
            raise IncompatibleVideosError(f"{name}: no video stream found")

        if other.format_name != ref.format_name:
            raise IncompatibleVideosError(
                f"container differs ({ref.path.name}: {ref.format_name or 'unknown'}, "
                f"{name}: {other.format_name or 'unknown'})"
            )
        if len(other.video) != len(ref.video):
            raise IncompatibleVideosError(
                f"video stream count differs ({ref.path.name}: {len(ref.video)}, "
                f"{name}: {len(other.video)})"
            )
        if len(other.audio) != len(ref.audio):
            raise IncompatibleVideosError(
                f"audio stream count differs ({ref.path.name}: {len(ref.audio)}, "
                f"{name}: {len(other.audio)})"
            )

        for kind, ref_streams, other_streams, fields in (
            ("video", ref.video, other.video, COMPARED_VIDEO_FIELDS),
            ("audio", ref.audio, other.audio, COMPARED_AUDIO_FIELDS),
        ):
            for slot, ((_, want), (_, got)) in enumerate(zip(ref_streams, other_streams)):
                diffs = [
                    f"'{key}' differs ({ref.path.name}: {want.get(key)!s}, {name}: {got.get(key)!s})"
                    for key in fields
                    if want.get(key) != got.get(key)
                ]
                if diffs:
                    more = f" (+{len(diffs) - 3} more)" if len(diffs) > 3 else ""
                    raise IncompatibleVideosError(
                        f"{kind} stream {slot}: " + "; ".join(diffs[:3]) + more
                    )


# --------------------------------------------------------------------------- #
# Merger
# --------------------------------------------------------------------------- #


CancelCheck = Callable[[], bool]


class VideoMerger:
    """Merge any number (>= 2) of compatible videos into one, losslessly."""

    def __init__(
        self,
        *,
        ffmpeg_bin: str = "ffmpeg",
        ffprobe_bin: str = "ffprobe",
        probe_timeout: float = 60.0,
        merge_timeout: float = 3600.0,
        poll_interval: float = 0.25,
        terminate_grace: float = 5.0,
    ) -> None:
        self.ffmpeg_bin = ffmpeg_bin
        self.ffprobe_bin = ffprobe_bin
        self.probe_timeout = probe_timeout
        self.merge_timeout = merge_timeout
        self.poll_interval = poll_interval
        self.terminate_grace = terminate_grace

    # ------------------------------------------------------------------ API

    async def merge(
        self,
        sources: Sequence[Path],
        output_dir: Path,
        *,
        output_stem: str = "merged",
        max_output_bytes: int | None = None,
        should_cancel: CancelCheck | None = None,
    ) -> MergeResult:
        """Merge ``sources`` (in the given order) into ONE file in ``output_dir``.

        Raises :class:`VideoMergeError` subclasses on failure and
        :class:`MergeCancelledError` on cancellation. On every failure path the
        partial output file is removed.
        """
        if len(sources) < 2:
            raise VideoMergeError("At least two videos are required to merge", reason="invalid_input")

        ffmpeg = self._require(self.ffmpeg_bin, "ffmpeg")
        ffprobe = self._require(self.ffprobe_bin, "ffprobe")

        ordered = [Path(p).resolve() for p in sources]  # absolute, order kept
        for path in ordered:
            if not path.is_file():
                raise VideoMergeError(f"Source video does not exist: {path.name}", reason="invalid_input")

        probes: list[ProbeInfo] = []
        for path in ordered:
            self._checkpoint(should_cancel)
            probes.append(await self._probe(ffprobe, path))
        self._checkpoint(should_cancel)

        check_compatibility(probes)

        total_bytes = sum(p.stat().st_size for p in ordered)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        self._check_disk_space(output_dir, total_bytes, max_output_bytes)

        suffix = choose_output_suffix(ordered, probes[0].format_name)
        output = (output_dir / f"{sanitize_stem(output_stem)}{suffix}").resolve()
        concat_list = output_dir / "concat_list.txt"
        aliases: list[Path] = []

        try:
            listed = [self._listable(p, i, output_dir, aliases) for i, p in enumerate(ordered)]
            concat_list.write_text(build_concat_list(listed), encoding="utf-8")
            if output.exists():
                output.unlink()

            indexes = [i for i, _ in probes[0].video] + [i for i, _ in probes[0].audio]
            command = build_ffmpeg_command(
                ffmpeg, concat_list, output, indexes, max_output_bytes=max_output_bytes
            )
            logger.info(
                "Merging %d videos with stream copy -> %s", len(ordered), output.name
            )
            logger.debug("ffmpeg command: %s", command)

            await self._run_ffmpeg(command, should_cancel)

            if not output.is_file():
                raise VideoMergeError("FFmpeg finished but produced no output file", reason="ffmpeg_failed")
            size = output.stat().st_size
            if size <= 0:
                raise VideoMergeError("FFmpeg produced an empty output file", reason="ffmpeg_failed")
            if max_output_bytes is not None and size > max_output_bytes:
                raise MergedTooLargeError(
                    f"Merged video exceeds the size limit (> {max_output_bytes} bytes; "
                    f"sources total {total_bytes} bytes)"
                )
        except BaseException:
            self._remove(output)
            raise
        finally:
            self._remove(concat_list)
            for alias in aliases:
                self._remove(alias)

        return MergeResult(
            output_path=output,
            size_bytes=size,
            source_count=len(ordered),
            command=tuple(command),
            container=probes[0].format_name,
            sources=tuple(p.name for p in ordered),
        )

    # ------------------------------------------------------------ internals

    @staticmethod
    def _checkpoint(should_cancel: CancelCheck | None) -> None:
        if should_cancel is not None and should_cancel():
            raise MergeCancelledError()

    @staticmethod
    def _require(binary: str, label: str) -> str:
        resolved = shutil.which(binary)
        if resolved is None:
            raise FFmpegUnavailableError(
                f"{label} is not installed or not on PATH; videos cannot be merged"
            )
        return resolved

    @staticmethod
    def _remove(path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            logger.warning("Could not remove temporary file %s", path.name, exc_info=True)

    @staticmethod
    def _check_disk_space(directory: Path, total_bytes: int, limit: int | None) -> None:
        expected = total_bytes if limit is None else min(total_bytes, limit + 1)
        try:
            free = shutil.disk_usage(directory).free
        except OSError:  # pragma: no cover - unknown filesystem: do not block
            return
        needed = expected + DISK_SAFETY_MARGIN_BYTES
        if free < needed:
            raise VideoMergeError(
                f"Not enough free disk space to merge ({free} bytes free, about {needed} needed)",
                reason="disk_space",
            )

    @staticmethod
    def _listable(path: Path, index: int, work_dir: Path, aliases: list[Path]) -> Path:
        """The path written to the concat list.

        Line breaks cannot be expressed in a concat list, so such (very rare)
        files are exposed through a symlink with a plain name. Nothing is copied.
        """
        text = str(path)
        if "\n" not in text and "\r" not in text and "\x00" not in text:
            return path
        alias = work_dir / f"part_{index:05d}{path.suffix.lower()}"
        try:
            if alias.exists() or alias.is_symlink():
                alias.unlink()
            os.symlink(path, alias)
        except OSError as exc:
            raise VideoMergeError(
                f"Cannot reference {path.name!r} safely: {exc}", reason="invalid_input"
            ) from exc
        aliases.append(alias)
        return alias.absolute()

    async def _spawn(self, argv: Sequence[str], *, stdout: int) -> asyncio.subprocess.Process:
        try:
            return await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=stdout,
                stderr=asyncio.subprocess.PIPE,
            )
        except (FileNotFoundError, PermissionError) as exc:
            raise FFmpegUnavailableError(
                f"{Path(argv[0]).name} could not be started: {exc.strerror or exc}"
            ) from exc

    async def _stop(self, proc: asyncio.subprocess.Process) -> None:
        """Terminate, then kill, then reap ``proc`` (never leaves an orphan)."""
        if proc.returncode is not None:
            return
        try:
            proc.terminate()
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(proc.wait(), timeout=self.terminate_grace)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except ProcessLookupError:
                return
            await proc.wait()

    @staticmethod
    async def _drain(stream: asyncio.StreamReader | None, tail: deque[str]) -> None:
        """Keep only the last few stderr lines (bounded memory)."""
        if stream is None:
            return
        while True:
            line = await stream.readline()
            if not line:
                return
            tail.append(line.decode("utf-8", "replace").rstrip())

    async def _probe(self, ffprobe: str, path: Path) -> ProbeInfo:
        argv = [
            ffprobe,
            "-v",
            "error",
            "-hide_banner",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            "-i",
            str(path),
        ]
        proc = await self._spawn(argv, stdout=asyncio.subprocess.PIPE)
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=self.probe_timeout)
        except asyncio.TimeoutError:
            await self._stop(proc)
            raise VideoMergeError(f"ffprobe timed out on {path.name}", reason="probe_failed")
        except BaseException:
            # Cancellation while probing: never leave ffprobe running.
            if proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
            raise

        if proc.returncode != 0:
            detail = _clip(err.decode("utf-8", "replace")) or f"exit code {proc.returncode}"
            raise VideoMergeError(f"Could not read {path.name}: {detail}", reason="probe_failed")
        try:
            data = json.loads(out.decode("utf-8", "replace") or "{}")
        except ValueError as exc:
            raise VideoMergeError(f"ffprobe returned invalid output for {path.name}", reason="probe_failed") from exc
        return parse_probe(path, data)

    async def _run_ffmpeg(self, command: Sequence[str], should_cancel: CancelCheck | None) -> None:
        proc = await self._spawn(command, stdout=asyncio.subprocess.DEVNULL)
        tail: deque[str] = deque(maxlen=_STDERR_TAIL_LINES)
        drain = asyncio.ensure_future(self._drain(proc.stderr, tail))
        waiter = asyncio.ensure_future(proc.wait())
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.merge_timeout
        cancelled = False
        timed_out = False

        try:
            while True:
                done, _ = await asyncio.wait({waiter}, timeout=self.poll_interval)
                if done:
                    break
                if should_cancel is not None and should_cancel():
                    cancelled = True
                    break
                if loop.time() >= deadline:
                    timed_out = True
                    break
        except BaseException:
            # Task cancelled (shutdown) or unexpected error: no orphan FFmpeg.
            if proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
            raise
        finally:
            if cancelled or timed_out:
                await self._stop(proc)
            waiter.cancel()
            try:
                await asyncio.wait_for(drain, timeout=2)
            except (asyncio.TimeoutError, Exception):  # noqa: BLE001 - best effort
                drain.cancel()

        if cancelled:
            raise MergeCancelledError()
        if timed_out:
            raise VideoMergeError(
                f"FFmpeg did not finish within {int(self.merge_timeout)} s and was stopped",
                reason="ffmpeg_failed",
            )
        if proc.returncode != 0:
            detail = _clip(" | ".join(tail)) or f"exit code {proc.returncode}"
            raise VideoMergeError(f"FFmpeg failed: {detail}", reason="ffmpeg_failed")
