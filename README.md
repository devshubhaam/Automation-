# Telegram Media Processor

A **Telegram userbot** (Telethon, running on your own account). Send it a ZIP
archive; it downloads and safely extracts it, scans it for images and videos and
processes them like this:

```
ZIP ─► download ─► safe extract ─► scan
                                    │
      images ≤ 2 MiB ─► ImgBB ─► image URLs ─► ONE Telegraph article (images only)
      images > 2 MiB ─► skipped (metadata["part2_skipped_large_images"], not an error)
                                    │
      each video (≤ 1.5 GB), one after another
         ─► VIDEO_BOTS (@DiskWalaFileUploaderBot, then @FlezenUploadBot if needed)
         ─► URL taken from the bot's reply to THAT video ─► stored against that filename
                                    │
      ONE final Telegram message (the progress message, edited in place):
      Telegraph URL · video URLs with filenames · failed/skipped files with reasons · status
```

* Videos are **never** uploaded to ImgBB and **never** sent to Telegraph.
* Telegraph receives **only ImgBB image URLs** (`TelegraphPublisher.publish(title, image_urls)`).
  If there are no uploaded images (e.g. a ZIP with only videos) no article is created.
* The progress message is one Telegram message, edited in place, ending in
  `COMPLETED`, `COMPLETED_WITH_ERRORS`, `FAILED` or `CANCELLED` (see section 3).
* A Telegraph problem never stops the videos, and one failing video never stops the next one.

---

## 1. Components

| Module | Responsibility |
| --- | --- |
| `app/config.py` | Reads `.env` into one frozen `Settings` object. Secrets are redacted in `repr`. |
| `app/telegram_client.py` | Userbot client, owner restriction, `/ping /start /status /cancel`. Session stored in MongoDB when `MONGODB_URI` is set. |
| `app/login_bot.py` | Small Telegram bot (`BOT_TOKEN`) that performs the QR `/login` of the userbot (owner only). |
| `app/session_store.py` | MongoDB storage of the Telethon `StringSession`. |
| `app/archive_processor.py` | ZIP validation and hardened, streamed extraction. |
| `app/media_scanner.py` | Recursive image/video detection, natural ordering. |
| `app/job_manager.py` | `Job` model, status machine, results/failures/metadata bookkeeping. **No queue.** |
| `app/main.py` | `PipelineWorker` (owns the only `asyncio.Queue`), upload pipeline, health server, entry point. |
| `app/uploaders/imgbb.py` | ImgBB upload (images only, ≤ 2 MiB). |
| `app/uploaders/telegraph.py` | Telegraph `createPage` with `<img>` nodes (image URLs only). |
| `app/uploaders/video_bot.py` | `MultiVideoBotUploader`: sends a video to the `VIDEO_BOTS` (in order) and returns the link they reply with. |
| `app/progress.py` | Renders the progress message and the final result message (always ≤ 4096 characters). |

### Job status

```
RECEIVED ► QUEUED ► DOWNLOADING ► EXTRACTING ► SCANNING ► UPLOADING ► COMPLETED
                                                                     └► COMPLETED_WITH_ERRORS
        any active state ► FAILED        any active state ► CANCELLED
```

`UPLOADING` covers ImgBB, the Telegraph article **and** the video-bot stage.
`COMPLETED_WITH_ERRORS` means at least one upload failed (including a video that
timed out or was over the size limit). Terminal states never change again.

---

## 2. Videos in detail

For each video, one after another and independently:

1. check cancellation, 2. check the file exists, 3. check the size (`VIDEO_MAX_SIZE_GB`,
default **1.5 GB = 1536 MiB**; larger videos are **not** sent to any bot and are recorded as
a failure with the reason), 4. send it to the first bot in `VIDEO_BOTS`, 5. wait for that
bot's link, 6. store the link against that video, 7. continue with the next one.

**Bots and links.** `VIDEO_BOTS=@DiskWalaFileUploaderBot,@FlezenUploadBot`. Only known
provider links are accepted:

| Bot | Link |
| --- | --- |
| `@DiskWalaFileUploaderBot` | `https://www.diskwala.com/app/<id>` |
| `@FlezenUploadBot` | `https://flezen.com/s/<id>` |

Other links in the bot's chat (ads, help links) are ignored. `VIDEO_URL_PATTERN` can add one
more accepted format.

**Fallback (no duplicate uploads).** The next bot is tried only if the file never reached
the bot (delivery failed after `VIDEO_BOT_SEND_ATTEMPTS`) or the bot answered that it cannot
process the file. A **timeout is not followed by another bot** by default, because the first
bot may still be processing the video (set `VIDEO_BOT_FALLBACK_ON_TIMEOUT=true` to change
this). If every bot fails, the video is reported as failed with every bot's reason.

**Flezen is slow.** The first reply `Downloading the file, please wait...` is **not** a
result. For the whole `VIDEO_BOT_TIMEOUT_SECONDS` (default 1800 s) the bot's *new and edited*
messages are watched until the final `https://flezen.com/s/...` link appears — as an edit of
that reply or as a new reply to it. If it never appears, the video fails with a timeout.

**Mapping video → URL.** Videos are sent strictly one at a time. A bot message is only
accepted for the current video if it is newer than the message we sent and is a Telegram
*reply* to that exact message (or to the bot's own reply to it, or an edit of such a
reply). A late answer for an earlier (timed-out) video can therefore never be attached to
the next one.

**Memory.** The file path is given to Telethon, which streams it from disk in small
parts. The video is never read into RAM, so 100 MB – 1.4 GB videos are fine.

**Timeout and retry (per video).** A timeout is **not** retried on the same bot (re-sending
would duplicate work). Only a failed *delivery* of the file is retried, up to
`VIDEO_BOT_SEND_ATTEMPTS` (default 2).

**Temporary event handlers** are registered per attempt and always removed (success,
timeout, failure, cancellation, shutdown).

**One failure never affects the others.** The failed video is recorded and the next one
runs; successful results are kept.

### Merge Videos

```env
MERGE_VIDEOS=false
```

| Value | Behaviour |
| --- | --- |
| `false` (default) | Current behaviour, unchanged: every video is sent to the video bots on its own. |
| `true` | A ZIP with **2 or more videos** is merged into **one** video before it is sent to the video bot(s). |

With `MERGE_VIDEOS=true`:

```
ZIP → extract → scan → videos in the pipeline's existing order → FFprobe compatibility check
    → FFmpeg stream-copy merge → ONE merged video → ONE DiskWala upload → ONE URL
```

- Works for **any number of videos** (2, 5, 10, 50, …); the count comes from the extracted ZIP.
- **Order** is the pipeline's existing deterministic order (natural sort of the path in the ZIP:
  `video2` before `video10`). It is never changed by upload time, size or response time.
- Compatible videos are merged with FFmpeg's concat demuxer and **stream copy**
  (`ffmpeg -f concat -safe 0 -i list.txt -map … -c copy out`). Nothing is re-encoded, so
  there is **no quality degradation** in the merge. Stream copy is lossless with respect to the
  already-encoded streams; it does **not** make incompatible source formats compatible.
- Before merging, every source is inspected with FFprobe. These must be identical across all
  videos: container family, number of video/audio streams, and per stream — video: codec,
  width, height, pixel format, frame rate, sample aspect ratio, field order, profile, level,
  colour tags; audio: codec, profile, sample rate, channels, channel layout.
- **Incompatible videos are rejected, never silently re-encoded.** The merge is reported as a
  failed video (the reason names the property and the two files, e.g. `'width' differs
  (video1.mp4: 1920, video3.mp4: 1280)`), **nothing is sent to DiskWala** — not the merged file
  and not the individual videos — and the job finishes as *completed with errors*. Images and the
  Telegraph article are unaffected.
- The merged file must respect `VIDEO_MAX_SIZE_GB`. If it is larger it is deleted and **not**
  uploaded (reported as a failure). FFmpeg is also told to stop writing right past the limit, so
  an oversized merge cannot fill the disk. Normal (non-merge) size-limit behaviour is unchanged.
- **Exactly one video result** is produced (with the merged file's `size_bytes`), so
  `{video_links}` contains **one** DiskWala URL instead of one per source video.
- **Exactly one video** in the ZIP is never merged: the original is sent as before. **No videos**:
  the image/Telegraph behaviour is unchanged. Images and Telegraph are never affected.
- The merged file is written to `<job dir>/merged/` and removed with the job's other files
  (unless `KEEP_JOB_FILES=true`). FFmpeg reads and writes files directly — nothing is held in
  RAM — but the merged file needs about as much free disk space as all the videos together
  (the merge fails with a clear message when there is not enough).
- **Cancellation** terminates FFmpeg (then kills it if needed), removes the partial file, uploads
  nothing and ends the job as `CANCELLED`. A missing `ffmpeg`/`ffprobe`, a corrupt source or an
  FFmpeg error is a failed merge, never a crash, and a partial file is never uploaded.

Requires `ffmpeg` and `ffprobe` (the Dockerfile installs them). Check with `ffmpeg -version` and
`ffprobe -version`.

> The compatibility check compares what FFprobe reports. Files made by the same encoder with the
> same settings (e.g. parts of one recording) are the intended input; if a player has trouble with
> a merged file whose sources differed in ways FFprobe cannot see, re-encode them yourself first.

### Result metadata (`job.metadata`, included in `job.to_dict()`)

| Key | Content |
| --- | --- |
| `detected_images`, `detected_videos` | Everything found by the scanner |
| `part2_skipped_large_images` | Images > 2 MiB that were skipped |
| `imgbb_results` | `[{filename, relative_path, url}]` for successful ImgBB uploads |
| `telegraph_articles` | Article URL(s), only if images were uploaded |
| `video_processing` | `"video_bot"`, `"not_required"` (or `"video_merge"` while merging / when the merge failed) |
| `video_status` | Per video: `{filename, relative_path, status, url, error}` (`pending`, `processing`, `merging`, `done`, `failed`, `cancelled`) |
| `video_results` | `[{filename, relative_path, url}]` — the filename → URL mapping |
| `video_merge` | Only with `MERGE_VIDEOS=true` and 2+ videos: `{status, mode, source_count, sources, output, size_bytes, error, reason}` (`status`: `merging`, `done`, `failed`, `cancelled`) |
| `video_failures` | `[{filename, relative_path, error}]` |

`job.upload_results` / `job.upload_failures` hold the same information as structured
records (`provider` is `imgbb`, `telegraph_article` or `video_bot`).

---

## 3. Final message

The progress message is edited in place (no extra messages, link previews off). When the job
ends it contains:

* the overall status (`Completed`, `Completed with errors`, `Failed`, `Cancelled`) and counts,
* the **Telegraph article URL** (or why it failed),
* every successful **video: filename → URL** (with the provider),
* every **failed** file and every **skipped** image with its reason
  (e.g. `exceeds the size limit`, `timeout`, `over the 2 MiB image limit`),
* the ImgBB links only if there is room (they are also inside the Telegraph article).

It is cut down automatically to stay below Telegram's 4096-character limit (ImgBB links first;
the Telegraph URL and video URLs last). If editing the final message fails, a copy is sent as
a reply.

## 4. Cancellation and shutdown

`/cancel <job_id>` only sets `cancel_requested`. The worker checks it between stages, between
images, between videos, while a video is being sent to the bot and while waiting for the
bot's link (about every second), then stops safely and marks the job `CANCELLED`. The
worker task itself is never killed. A queued job that is cancelled is skipped.

On shutdown (SIGTERM/redeploy) the pipeline stops **before** the Telegram client
disconnects: a running job is marked `CANCELLED` with `cancel_reason = "application shutdown"`,
its temporary video-bot handlers are removed and the status message is edited one last time.
The MongoDB session is not touched by the pipeline; the existing `/login` and the
`AuthKeyDuplicatedError` handling are unchanged.

## 5. Queue

`PipelineWorker` owns the only processing queue (`asyncio.Queue`). `JobManager` is
bookkeeping only. `/status` shows the real `PipelineWorker` queue size.

## 6. Security

Owner-only processing; path-traversal, symlink/special-file and ZIP-bomb protection
(`MAX_ARCHIVE_SIZE_MB`, `MAX_EXTRACTED_SIZE_MB`, `MAX_FILES_PER_ARCHIVE`); the Telegram
session is stored in MongoDB (or a local file); `API_HASH`, `BOT_TOKEN`, `MONGODB_URI`,
`IMGBB_API_KEY`, the Telegraph token and session strings are never logged or shown in
progress messages. `.env` and `*.session` are git-ignored.

The ZIP limits apply to the archive. The 1.5 GB limit applies to **each video**.

---

## 7. Configuration

Copy `.env.example` to `.env`. Required: `API_ID`, `API_HASH` (plus `BOT_TOKEN`/`BOT_OWNER_ID` for `/login`).

| Variable | Default | Meaning |
| --- | --- | --- |
| `API_ID`, `API_HASH` | — | Telegram API credentials (required) |
| `BOT_TOKEN`, `BOT_OWNER_ID` | — | Login bot for the QR `/login` |
| `MONGODB_URI`, `MONGODB_DATABASE`, `MONGODB_COLLECTION` | — / `telegram_media_processor` / `sessions` | Persistent session storage |
| `IMGBB_API_KEY` | — | ImgBB key; without it every image upload fails with a clear error |
| `TELEGRAPH_ACCESS_TOKEN` | *(anonymous account per start)* | Telegraph account token |
| `TELEGRAPH_AUTHOR_NAME` | — | Author shown on the article |
| `VIDEO_BOTS` | — | Comma-separated bots, tried in order: `@DiskWalaFileUploaderBot,@FlezenUploadBot`. Without it every video fails with a clear error. (`VIDEO_BOT_USERNAME` is the deprecated single-bot form) |
| `VIDEO_BOT_TIMEOUT_SECONDS` | `1800` | Per-video wait for the final link after delivery (10–7200) |
| `VIDEO_BOT_SEND_ATTEMPTS` | `2` | Delivery attempts per video (1–5) |
| `VIDEO_MAX_SIZE_GB` | `1.5` | Per-video size limit (also the limit for the merged video when `MERGE_VIDEOS=true`) |
| `MERGE_VIDEOS` | `false` | `true`: merge 2+ videos of a ZIP into one (FFmpeg stream copy, no re-encoding) before the video bot — see *Merge Videos* |
| `VIDEO_BOT_FALLBACK_ON_TIMEOUT` | `false` | Try the next bot after a timeout (may duplicate an upload) |
| `VIDEO_BOT_REQUIRE_REPLY` | `true` | Only accept bot messages that reply to the sent video |
| `VIDEO_URL_PATTERN` | — | Optional extra regex; DiskWala (`/app/`) and Flezen (`/s/`) links are built in |
| `MAX_ARCHIVE_SIZE_MB` / `MAX_EXTRACTED_SIZE_MB` / `MAX_FILES_PER_ARCHIVE` | `500` / `2000` / `10000` | ZIP safety limits |
| `DOWNLOAD_DIR`, `JOB_DIR`, `LOG_DIR`, `SESSION_DIR` | `./data/...` | Storage paths |
| `KEEP_JOB_FILES` | `false` | Keep `archive/`, `extracted/` (and `merged/`) after a job |
| `WORKER_COUNT` | `1` | Pipeline workers (1–4) |
| `LOG_LEVEL`, `LOG_MAX_BYTES`, `LOG_BACKUP_COUNT` | `INFO`, `5242880`, `5` | Logging |

Disk: the ZIP and its extracted files exist on disk at the same time, so a 500 MB ZIP
needs roughly 1 GB or more of free space on the instance.

## 8. Koyeb deployment

1. Deploy the repository (Dockerfile build). The container runs `python -m app.main`
   and serves a health endpoint on port 8000.
2. Set the environment variables from section 6 as Koyeb secrets/env — at least
   `API_ID`, `API_HASH`, `BOT_TOKEN`, `BOT_OWNER_ID`, `MONGODB_URI`, `IMGBB_API_KEY`,
   `VIDEO_BOTS` (and `TELEGRAPH_ACCESS_TOKEN`).
3. Open your login bot in Telegram and send `/login`, then scan the QR code. The session
   is saved in MongoDB, so redeploys do not require a new login.
4. Send a ZIP to your Saved Messages. Start with a small ZIP (one image, one short video)
   before a large one.

## 9. Commands

| Command | Reply |
| --- | --- |
| `/ping` | Pong |
| `/status` | Job counts, real pipeline queue size and the latest jobs |
| `/cancel <job_id>` | Requests cancellation of that job |

## 10. Tests

```bash
pip install -r requirements.txt
python -m compileall app
python -m pytest -q
```

All tests are offline: ImgBB, Telegraph, Telegram and the video bots are replaced by
fakes. `tests/test_part3_video.py` / `tests/test_part3_multibot.py` cover the video flow
(routing, mapping, fallback, size limit, timeout/retry, cancellation, metadata).
`tests/test_part4_pipeline.py` covers the combined pipeline: mixed ZIP end to end, video →
URL mapping, DiskWala/Flezen URL detection, edited/reply messages (Flezen placeholder), a
failed video and a Telegraph failure not stopping the rest, the 1.5 GB limit, videos never
reaching ImgBB/Telegraph, the 4096-character final message, shutdown/cancellation and the
`.env.example` values.
`scripts/smoke_startup.py` is a startup/config smoke test.

`tests/test_merge_videos.py` covers `MERGE_VIDEOS`: pure helpers (concat list escaping, the FFmpeg command is `-c copy` only, compatibility rules), the real merger driving fake `ffmpeg`/`ffprobe` scripts (failure, hang, cancel, oversize, missing binaries, tricky file names), the worker/pipeline integration (one upload, one URL, final post, ordering, cleanup) and — when `ffmpeg` is installed — real merges that verify decoded video frames and audio packets are identical to the sources.
