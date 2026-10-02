# Telegram Media Processor — PART 1 (Foundation)

A **Telegram userbot** (MTProto, via Telethon, running on *your own* Telegram account)
that receives a ZIP archive, downloads it, extracts it **safely**, recursively scans
the contents for images and videos, and reports the result in a single, continuously
edited Telegram progress message.

> **Scope of PART 1.** The pipeline stops after media detection and reporting.
> ImgBB upload, Telegraph articles, the video bot and final media-link generation are
> **not implemented here** — they belong to PART 2–5.

---

## 1. Architecture

```
Telegram (your account)
        │  ZIP document
        ▼
telegram_client.py  ── owner check ──► job created + progress message
        │
        ▼  asyncio queue
main.py :: PipelineWorker   (1 worker, jobs run sequentially)
        │
        ├─ DOWNLOADING  →  client.download_media() → <job>/archive/media.zip
        ├─ EXTRACTING   →  validate_archive() + safe_extract() → <job>/extracted/
        ├─ SCANNING     →  scan_directory() (rglob-style walk, natural sort)
        ├─ COMPLETED    →  progress message updated + cleanup
        └─ FAILED / CANCELLED → safe cleanup + user-facing error
```

| Module | Responsibility |
| --- | --- |
| `app/config.py` | Loads `.env`, validates and converts every setting into one frozen `Settings` object. No `os.getenv()` anywhere else. |
| `app/logging_config.py` | Console + **rotating** file handler. Never logs secrets. |
| `app/utils.py` | Pure helpers: natural sort, path-traversal guards, byte formatting. |
| `app/archive_processor.py` | ZIP validation and hardened, streamed extraction. |
| `app/media_scanner.py` | Recursive image/video classification, natural ordering, relative paths. |
| `app/job_manager.py` | `Job` model, explicit status machine, in-memory registry, queue, cleanup. |
| `app/progress.py` | Renders the single progress message (send once, edit thereafter). |
| `app/telegram_client.py` | Telethon client, authentication, owner restriction, commands. |
| `app/main.py` | Wires everything together, `PipelineWorker`, entry point. |

### Job status machine

```
RECEIVED ─► QUEUED ─► DOWNLOADING ─► EXTRACTING ─► SCANNING ─► COMPLETED
                              └────────────┴────────────┴────────► FAILED
                              └────────────┴────────────┴────────► CANCELLED
```

Terminal states (`COMPLETED`, `FAILED`, `CANCELLED`) cannot transition further; an
illegal transition raises `JobStateError`. `ARCHIVE`/`extracted` directories are
**per job**, so jobs can never interfere with each other.

### Security properties

* **Path traversal** — every member name is normalised and resolved; absolute paths,
  `..` segments, drive letters (`C:\`) and UNC-style paths are rejected **before** any
  byte is written.
* **Symlinks / special files** — detected via `external_attr >> 16` with `stat.S_ISLNK`
  and skipped (validation rejects them outright).
* **ZIP bomb** — the declared uncompressed size *and* the bytes actually streamed are
  both counted against `MAX_EXTRACTED_SIZE_MB`; extraction aborts mid-stream and the
  partial file is removed.
* **File count** — `MAX_FILES_PER_ARCHIVE` (default 10 000) is enforced during
  validation *and* during extraction.
* **Owner restriction** — only messages sent by the account itself (or its Saved
  Messages) are processed. The owner id comes from `get_me()`, never hardcoded.
* **Cleanup** — only `<job>/archive` and `<job>/extracted` are ever deleted; the path
  is re-verified to be inside the job root before deletion.
* **Secrets** — the session file is never printed or logged, `.gitignore` excludes it,
  and `Settings.__repr__` redacts `API_HASH`.

---

## 2. Requirements

* Python **3.11+**
* A Telegram account and its `api_id` / `api_hash` from <https://my.telegram.org>
  → *API development tools*
* Dependencies: `telethon`, `python-dotenv` (`pytest` for the test suite)

---

## 3. Telegram API setup

1. Go to <https://my.telegram.org> and log in with your phone number.
2. Open **API development tools**, create an application (any name/short name).
3. Copy `api_id` (a number) and `api_hash` (32 hex characters).

---

## 4. Installation

```bash
git clone <your-repo> telegram-media-processor
cd telegram-media-processor

python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

---

## 5. `.env` configuration

```bash
cp .env.example .env
$EDITOR .env
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `API_ID` | — (**required**) | Telegram API id |
| `API_HASH` | — (**required**) | Telegram API hash |
| `SESSION_NAME` | `media_processor` | Telethon session file name |
| `SESSION_DIR` | `./data/sessions` | Where the `.session` file lives (persist it!) |
| `TELEGRAM_PHONE` | *(empty)* | Optional; otherwise Telethon prompts |
| `OWNER_ID` | *(empty)* | Optional; must equal your own account id |
| `DOWNLOAD_DIR` | `./data/downloads` | Staging for downloads |
| `JOB_DIR` | `./data/jobs` | Job directories |
| `LOG_DIR` | `./data/logs` | Log output |
| `MAX_ARCHIVE_SIZE_MB` | `500` | Maximum accepted ZIP size |
| `MAX_EXTRACTED_SIZE_MB` | `2000` | Maximum total extracted bytes |
| `MAX_FILES_PER_ARCHIVE` | `10000` | Maximum number of files |
| `KEEP_JOB_FILES` | `false` | `true` keeps `archive/` + `extracted/` after a job |
| `WORKER_COUNT` | `1` | Concurrent workers (1–4) |
| `LOG_LEVEL` | `INFO` | `DEBUG`…`CRITICAL` |
| `LOG_MAX_BYTES` | `5242880` | Rotate after this many bytes |
| `LOG_BACKUP_COUNT` | `5` | Rotated files to keep |

`Settings.from_env()` raises `ConfigError` with a clear message when a required value
is missing or malformed — the process exits with code 2 and a hint to copy `.env.example`.

---

## 6. First login

```bash
python -m app.main
```

Telethon will ask, **once**:

1. phone number (if `TELEGRAM_PHONE` is not set),
2. the login code Telegram sends you,
3. your 2FA password, if enabled.

`data/sessions/media_processor.session` is created. **Never share or commit it** — it is
equivalent to a full login. To move the session between machines, copy the `.session`
file into the persistent `SESSION_DIR` (or the Docker volume) with `chmod 600`.

Log lines look like:

```
2026-10-02 19:00:01 INFO app.main Starting Telegram Media Processor (PART 1)
2026-10-02 19:00:05 INFO app.telegram_client Telegram authentication successful (owner_id=...)
2026-10-02 19:00:05 INFO app.main Userbot is running. Send a ZIP archive or use /ping, /status, /cancel.
```

---

## 7. Running locally

```bash
source .venv/bin/activate
python -m app.main
```

Restart the process — **no new login is requested**, because the session is reused.

---

## 8. Docker deployment

```bash
docker build -t telegram-media-processor:part1 .

# First run (interactive so you can type the login code):
docker run -it --rm --name tmp-bot --env-file .env \
  -v "$(pwd)/data:/data" \
  telegram-media-processor:part1

# Afterwards, detached:
docker run -d --name tmp-bot --restart unless-stopped --env-file .env \
  -v "$(pwd)/data:/data" \
  telegram-media-processor:part1

docker logs -f tmp-bot
```

* `/data` is a **volume** holding sessions, downloads, jobs and logs → a container
  restart never forces a new Telegram login.
* `SESSION_DIR`, `DOWNLOAD_DIR`, `JOB_DIR`, `LOG_DIR` are redirected into `/data`.
* Secrets arrive only through `--env-file`; the image contains no credentials.

---

## 9. Supported input

Send a **ZIP** to your own Saved Messages (or any chat you message yourself in):

```
media.zip
└── MyFolder/
    ├── images/   001.jpg  002.png  003.webp
    ├── videos/   001.mp4  002.mkv
    └── other/    document.pdf
```

Expected report: `Images: 3 · Videos: 2 · Ignored: 1`.

Nested directories of any depth are scanned. Unsupported documents (`.jpg`, `.mp4`,
`.pdf`, `.txt` …) are answered with *“❌ Unsupported input.”* and never downloaded.

| Images | `.jpg` `.jpeg` `.png` `.webp` `.gif` `.bmp` |
| --- | --- |
| **Videos** | `.mp4` `.mkv` `.webm` `.mov` `.avi` `.m4v` |

Extension matching is **case-insensitive** (`image.JPG`, `clip.MkV` both work).
Anything else is categorised as *ignored* and never fails the job.

---

## 10. Commands

| Command | Reply |
| --- | --- |
| `/ping` | `🏓 Pong` — confirms the client is alive |
| `/status` | Current/last job id, status and image/video/ignored counts, or `ℹ️ No active job.` |
| `/cancel` | `🛑 Cancellation requested.` — cancels the running job and cleans up its temporary files |

---

## 11. Progress messages

One message per job, edited in place:

```
⏳ New job          →  📥 Downloading ZIP...  →  📦 Extracting archive...
   JOB-20261002-185200-A8F3

🔍 Scanning files...  →  ✅ Scan complete
                          Job: JOB-20261002-185200-A8F3
                          🖼 Images: 3
                          🎬 Videos: 2
                          📄 Ignored: 4
                          PART 1 pipeline completed successfully.
```

Errors are user-friendly — a traceback is written to the log, never to Telegram:

```
❌ Processing failed
Stage: ZIP extraction
Reason:
The archive contains an unsafe file path.
Job: JOB-20261002-185200-A8F3
```

---

## 12. Testing

```bash
# Unit tests (no credentials, no network, no Telegram)
python -m pytest -v

# Offline end-to-end integration on the §52 acceptance archive + negative cases
python scripts/offline_integration.py

# Startup / config / logging smoke test (dummy credentials)
python scripts/smoke_startup.py
```

Test files: `tests/test_utils.py`, `tests/test_archive_processor.py`,
`tests/test_media_scanner.py`, `tests/test_job_manager.py`,
`tests/test_config_progress.py`. Fixtures in `tests/conftest.py` build every ZIP
programmatically into `tmp_path`.

---

## 13. Troubleshooting

| Symptom | Fix |
| --- | --- |
| `Configuration error: Missing required environment variable: API_ID` | Copy `.env.example` to `.env` and fill in `API_ID`/`API_HASH` |
| Login code never arrives | Check `TELEGRAM_PHONE` format (`+15551234567`); code arrives in the Telegram app, not SMS |
| `database is locked` on the session | Only one process may use a session file at a time |
| Login requested on every restart | `SESSION_DIR` is not persistent — mount a volume / keep the directory |
| `❌ ZIP file is too large` | Raise `MAX_ARCHIVE_SIZE_MB`, or split the archive |
| `Maximum extracted size exceeded` | Raise `MAX_EXTRACTED_SIZE_MB`, or the archive really is a bomb |
| `archive contains unsafe member(s)` | The ZIP contains `..`, an absolute path or a symlink — it was rejected on purpose |
| No response to a ZIP | Confirm the message is sent **from your own account**; other senders are ignored by design |
| `flood wait` while editing progress | Harmless — the update is skipped and logged |

---

## 14. Security notes

* `.gitignore` excludes `.env`, `*.session`, `*.session-journal`, `jobs/`, `logs/`,
  `temp/`, `downloads/`.
* Logs never contain `API_HASH`, session contents, `.env` values, login codes or 2FA
  passwords; the smoke test asserts this.
* Extracted content is never executed or opened as a program.
* Cleanup only ever touches the current job's own directory.
* Only the owner's own messages are processed.

---

## 15. PART 1 limitations

* **Not implemented:** ImgBB upload, Telegraph article creation, sending videos to
  another bot, waiting for video URLs, combined final links. The pipeline ends at
  media detection + reporting.
* Media detection is **extension-based** (with an optional `detector=` hook already in
  `scan_directory()` for future MIME/signature checks).
* Job state is **in-memory**; `metadata.json` is written per job so a database can be
  layered on later without changing callers.
* Jobs run **sequentially** (one worker by default) — `WORKER_COUNT` can raise this to 4.
* `KEEP_JOB_FILES=true` retains `archive/` and `extracted/` for debugging.

---

## 16. Layout

```
telegram-media-processor/
├── app/{__init__,main,config,logging_config,telegram_client,
│        job_manager,archive_processor,media_scanner,progress,utils}.py
├── tests/{__init__,conftest,test_utils,test_archive_processor,
│          test_media_scanner,test_job_manager,test_config_progress}.py
├── scripts/{offline_integration,smoke_startup}.py
├── data/{downloads,jobs,logs}/
├── .env.example
├── .gitignore
├── requirements.txt
├── Dockerfile
└── README.md
```
