#!/usr/bin/env python3
"""Apply the Part 2 overlay to the existing Part 1 repository."""
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parent
BACKUP = ROOT / ".part2-backup"

def backup(path: Path):
    target = BACKUP / path.relative_to(ROOT)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, target)

def replace_once(path, old, new):
    p=ROOT/path
    t=p.read_text(encoding="utf-8")
    if old not in t:
        raise SystemExit(f"Anchor not found in {path}; no changes made to that file")
    backup(p); p.write_text(t.replace(old,new,1), encoding="utf-8")

replace_once("app/config.py",
'''    mongodb_collection: str = "sessions"\n''',
'''    mongodb_collection: str = "sessions"\n\n    # Part 2 image hosting\n    imgbb_api_key: Optional[str] = None\n''')
replace_once("app/config.py",
'''            mongodb_collection=_get_str(\n                "MONGODB_COLLECTION",\n                default="sessions",\n            ) or "sessions",\n''',
'''            mongodb_collection=_get_str(\n                "MONGODB_COLLECTION",\n                default="sessions",\n            ) or "sessions",\n\n            # Part 2\n            imgbb_api_key=_get_str(\n                "IMGBB_API_KEY",\n                default=None,\n            ),\n''')

p=ROOT/"requirements.txt"; t=p.read_text(encoding="utf-8")
if "httpx>=" not in t:
    backup(p); p.write_text(t.rstrip()+"\nhttpx>=0.27.0,<1.0.0\n",encoding="utf-8")

p=ROOT/"app/job_manager.py"; t=p.read_text(encoding="utf-8"); backup(p)
t=t.replace("""    JobStatus.COMPLETED,
    JobStatus.FAILED,
    JobStatus.CANCELLED,
}""", """    JobStatus.COMPLETED,
    JobStatus.COMPLETED_WITH_ERRORS,
    JobStatus.FAILED,
    JobStatus.CANCELLED,
}""", 1)
t=t.replace('''    SCANNING = "SCANNING"\n    COMPLETED = "COMPLETED"\n''','''    SCANNING = "SCANNING"\n    UPLOADING = "UPLOADING"\n    COMPLETED = "COMPLETED"\n    COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"\n''',1)
t=t.replace('''    JobStatus.SCANNING: {\n        JobStatus.COMPLETED,\n        JobStatus.CANCELLED,\n        JobStatus.FAILED,\n    },\n    JobStatus.COMPLETED: set(),\n''','''    JobStatus.SCANNING: {\n        JobStatus.UPLOADING,\n        JobStatus.COMPLETED,\n        JobStatus.CANCELLED,\n        JobStatus.FAILED,\n    },\n    JobStatus.UPLOADING: {\n        JobStatus.COMPLETED,\n        JobStatus.COMPLETED_WITH_ERRORS,\n        JobStatus.CANCELLED,\n        JobStatus.FAILED,\n    },\n    JobStatus.COMPLETED: set(),\n    JobStatus.COMPLETED_WITH_ERRORS: set(),\n''',1)
t=t.replace('''    ignored_files: int = 0\n\n    status_message_id:''','''    ignored_files: int = 0\n\n    upload_results: list[dict[str, Any]] = field(default_factory=list)\n    upload_failures: list[dict[str, str]] = field(default_factory=list)\n\n    status_message_id:''',1)
t=t.replace('''            "ignored_files": self.ignored_files,\n            "status_message_id": self.status_message_id,\n''','''            "ignored_files": self.ignored_files,\n            "upload_results": self.upload_results,\n            "upload_failures": self.upload_failures,\n            "status_message_id": self.status_message_id,\n''',1)
t=t.replace('''            ignored_files=int(\n                payload.get("ignored_files", 0) or 0\n            ),\n            status_message_id=''','''            ignored_files=int(\n                payload.get("ignored_files", 0) or 0\n            ),\n            upload_results=list(payload.get("upload_results", []) or []),\n            upload_failures=list(payload.get("upload_failures", []) or []),\n            status_message_id=''',1)
anchor='''    def set_status_message(\n        self,\n        job_id: str,\n        message_id: int,\n    ) -> Job:\n'''
insert='''    def add_upload_result(self, job_id: str, result: dict[str, Any]) -> Job:\n        job=self.get(job_id); job.upload_results.append(dict(result)); job.write_metadata(); return job\n\n    def add_upload_failure(self, job_id: str, *, media_type: str, filename: str, error: str) -> Job:\n        job=self.get(job_id); job.upload_failures.append({"media_type":str(media_type),"filename":str(filename),"error":str(error)}); job.write_metadata(); return job\n\n'''
if anchor not in t: raise SystemExit("Anchor not found in app/job_manager.py")
t=t.replace(anchor,insert+anchor,1); p.write_text(t,encoding="utf-8")

p=ROOT/"app/main.py"; t=p.read_text(encoding="utf-8"); backup(p)
t=t.replace('from .media_scanner import scan_directory\n','from .media_scanner import scan_directory\nfrom .uploaders import ImgBBUploader, TelegraphUploader\n',1)
t=t.replace('''        self._stopping = False\n''','''        self._stopping = False\n        self.imgbb = ImgBBUploader(settings.imgbb_api_key)\n        self.telegraph = TelegraphUploader()\n''',1)
t=t.replace('''    async def _process(self, job) -> None:\n        """Run download -> validate -> extract -> scan."""\n''','''    async def _notify(self, job, text: str) -> None:\n        message=getattr(job, "_status_message", None)\n        if message is None: return\n        try: await message.edit(text)\n        except Exception: logger.debug("Could not update progress message for %s", job.job_id, exc_info=True)\n\n    async def _process(self, job) -> None:\n        """Run download -> validate -> extract -> scan -> upload."""\n''',1)
old='''            self.job_manager.set_status(\n                job_id,\n                JobStatus.COMPLETED,\n            )\n\n            logger.info(\n                "Job %s: processing completed "\n                "(images=%s videos=%s ignored=%s)",\n                job_id,\n                result.image_count,\n                result.video_count,\n                result.ignored_count,\n            )\n'''
new='''            self.job_manager.set_status(job_id, JobStatus.UPLOADING)\n            await self._notify(job, f"📤 Uploading media...\\n\\nJob: {job_id}\\n🖼 Images: {result.image_count}\\n🎬 Videos: {result.video_count}\\n📄 Ignored: {result.ignored_count}")\n            total_uploads=result.image_count+result.video_count\n            completed_uploads=0\n            for media in result.images:\n                if job.cancel_requested: self.job_manager.cancel(job_id); return\n                try:\n                    upload=await self.imgbb.upload(media.path)\n                    upload.update({"media_type":"image","filename":media.filename,"relative_path":media.relative_path,"size_bytes":media.size_bytes})\n                    self.job_manager.add_upload_result(job_id,upload); completed_uploads+=1\n                    await self._notify(job,f"📤 Uploading media...\\n\\nJob: {job_id}\\n🖼 Image {completed_uploads}/{total_uploads} uploaded")\n                except Exception as exc:\n                    self.job_manager.add_upload_failure(job_id,media_type="image",filename=media.filename,error=f"{type(exc).__name__}: {exc}")\n                    logger.warning("Job %s: ImgBB upload failed for %s: %s",job_id,media.filename,exc)\n            for media in result.videos:\n                if job.cancel_requested: self.job_manager.cancel(job_id); return\n                try:\n                    upload=await self.telegraph.upload(media.path)\n                    upload.update({"media_type":"video","filename":media.filename,"relative_path":media.relative_path,"size_bytes":media.size_bytes})\n                    self.job_manager.add_upload_result(job_id,upload); completed_uploads+=1\n                    await self._notify(job,f"📤 Uploading media...\\n\\nJob: {job_id}\\n🎬 Video {completed_uploads}/{total_uploads} uploaded")\n                except Exception as exc:\n                    self.job_manager.add_upload_failure(job_id,media_type="video",filename=media.filename,error=f"{type(exc).__name__}: {exc}")\n                    logger.warning("Job %s: Telegraph upload failed for %s: %s",job_id,media.filename,exc)\n            final_status=JobStatus.COMPLETED_WITH_ERRORS if job.upload_failures else JobStatus.COMPLETED\n            self.job_manager.set_status(job_id,final_status)\n            lines=["⚠️ Part 2 completed with upload errors." if job.upload_failures else "✅ Part 2 completed.","",f"Job: {job_id}",f"🖼 Images: {result.image_count}",f"🎬 Videos: {result.video_count}",f"📄 Ignored: {result.ignored_count}",f"📤 Uploaded: {len(job.upload_results)}"]\n            for item in job.upload_results: lines.append(f"\\n{item.get('media_type','media')} — {item.get('filename','file')}\\n{item.get('url')}")\n            if job.upload_failures:\n                lines.append("\\n❌ Failed uploads:")\n                for item in job.upload_failures: lines.append(f"- {item['media_type']} — {item['filename']}: {item['error']}")\n            await self._notify(job,"\\n".join(lines))\n            logger.info("Job %s: Part 2 completed (images=%s videos=%s ignored=%s uploaded=%s failed=%s)",job_id,result.image_count,result.video_count,result.ignored_count,len(job.upload_results),len(job.upload_failures))\n'''
if old not in t: raise SystemExit("completion block not found in main.py")
t=t.replace(old,new,1); p.write_text(t,encoding="utf-8")

p=ROOT/"app/telegram_client.py"; t=p.read_text(encoding="utf-8"); backup(p)
old='''            # PipelineWorker needs the actual Telethon\n            # message to download the document later.\n            job._telegram_message = message\n\n            self.job_manager.enqueue(\n                job.job_id\n            )\n\n            await self.pipeline.submit(\n                job\n            )\n'''
new='''            # PipelineWorker needs the actual Telethon\n            # message to download the document later.\n            job._telegram_message = message\n            self.job_manager.enqueue(job.job_id)\n            status_message = await message.reply(\n                "📦 ZIP received.\\n\\n"\n                f"Job ID: `{job.job_id}`\\n"\n                f"File: `{filename}`\\n\\n"\n                "📥 Processing started..."\n            )\n            job._status_message = status_message\n            self.job_manager.set_status_message(job.job_id, status_message.id)\n            await self.pipeline.submit(job)\n'''
if old not in t: raise SystemExit("submit block not found in telegram_client.py")
t=t.replace(old,new,1)
old='''        await message.reply(\n            "📦 ZIP received.\\n\\n"\n            f"Job ID: `{job.job_id}`\\n"\n            f"File: `{filename}`\\n\\n"\n            "Processing started."\n        )\n'''
if old not in t: raise SystemExit("final reply block not found in telegram_client.py")
t=t.replace(old,"",1); p.write_text(t,encoding="utf-8")

p=ROOT/"app/progress.py"; t=p.read_text(encoding="utf-8"); backup(p)
t=t.replace('    JobStatus.SCANNING: "🔍 Scanning files...",\n','    JobStatus.SCANNING: "🔍 Scanning files...",\n    JobStatus.UPLOADING: "📤 Uploading media...",\n',1)
t=t.replace('    JobStatus.COMPLETED: "✅ Scan complete",\n','    JobStatus.COMPLETED: "✅ Part 2 complete",\n    JobStatus.COMPLETED_WITH_ERRORS: "⚠️ Part 2 completed with upload errors",\n',1)
t=t.replace('    if job.status == JobStatus.COMPLETED:\n','    if job.status in {JobStatus.COMPLETED, JobStatus.COMPLETED_WITH_ERRORS}:\n',1)
t=t.replace('        parts.extend(["", "PART 1 pipeline completed successfully."])\n','        if job.upload_results:\n            parts.extend(["", f"📤 Uploaded: {len(job.upload_results)}"])\n            for item in job.upload_results[:20]:\n                parts.append(f"{item.get(\"media_type\", \"media\")} — {item.get(\"filename\", \"file\")}: {item.get(\"url\", \"\")}")\n        if job.upload_failures:\n            parts.extend(["", f"❌ Failed uploads: {len(job.upload_failures)}"])\n        parts.extend(["", "PART 2 pipeline completed."])\n',1); p.write_text(t,encoding="utf-8")

print("Part 2 patch applied successfully. Run pytest before deployment.")
