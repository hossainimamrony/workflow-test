import threading
import time
import uuid
import os
import traceback
from datetime import datetime, timedelta
from pathlib import Path
import json
import subprocess
import tempfile
import contextlib
from urllib.parse import urlencode
from urllib.request import urlopen, Request

from django.db import OperationalError, ProgrammingError
from django.db.models import Q
from django.utils import timezone

from .models import ReelRenderJob, ReelRun


class ReelRenderService:
    _lock = threading.Lock()
    _process_lock = threading.Lock()
    _active_processes: dict[str, subprocess.Popen] = {}
    _max_parallel_jobs = max(1, int(os.environ.get("REAL_FOOTAGE_MAX_PARALLEL_JOBS", "20") or "20"))
    _job_slots = threading.BoundedSemaphore(_max_parallel_jobs)
    _workflow_root = Path(__file__).resolve().parent / "workflow_engine"
    _cli_path = _workflow_root / "src" / "cli.mjs"
    _bridge_path = _workflow_root / "scripts" / "django-workflow-bridge.mjs"
    _runs_root = _workflow_root / "runs"
    _workflow_package_json = _workflow_root / "package.json"
    _workflow_env_file = _workflow_root / ".env"
    _workflow_node_modules = _workflow_root / "node_modules"
    _node_bin = os.environ.get("NODE_BIN", "node")
    _PHASE_PERCENT = {
        "queued": 2,
        "download": 18,
        "frames": 42,
        "analyze": 68,
        "prepare": 74,
        "compose": 90,
        "voiceover": 95,
        "publish": 98,
        "thumbnail": 96,
        "done": 100,
        "error": 100,
    }
    _FORWARDED_ENV_KEYS = (
        "GEMINI_API_KEY",
        "GEMINI_MODEL",
        "GEMINI_IMAGE_MODEL",
        "THUMBNAIL_GEMINI_MODEL",
        "ELEVEN_LABS_API_KEY",
        "ELEVENLAB_VOICE_ID",
        "ELEVEN_LABS_VOICE_ID",
    )
    _COMMAND_TIMEOUT_SECONDS = {
        "script-draft": int(os.environ.get("REAL_FOOTAGE_TIMEOUT_SCRIPT_DRAFT_SEC", "420") or "420"),
        "voiceover-draft": int(os.environ.get("REAL_FOOTAGE_TIMEOUT_SCRIPT_DRAFT_SEC", "420") or "420"),
        "prepare": int(os.environ.get("REAL_FOOTAGE_TIMEOUT_PREPARE_SEC", "900") or "900"),
        "compose": int(os.environ.get("REAL_FOOTAGE_TIMEOUT_COMPOSE_SEC", "1800") or "1800"),
        "run": int(os.environ.get("REAL_FOOTAGE_TIMEOUT_RUN_SEC", "2400") or "2400"),
    }

    @classmethod
    def _env_flag(cls, key: str, default: bool = False) -> bool:
        raw = str(os.environ.get(key, "") or "").strip().lower()
        if not raw:
            return bool(default)
        if raw in {"1", "true", "yes", "y", "on"}:
            return True
        if raw in {"0", "false", "no", "n", "off"}:
            return False
        return bool(default)

    @classmethod
    def uses_external_worker(cls) -> bool:
        default_for_env = cls._is_pythonanywhere()
        return cls._env_flag("REAL_FOOTAGE_USE_EXTERNAL_WORKER", default=default_for_env)

    @classmethod
    def _initial_run_report(cls, *, command: str, run_id: str) -> dict:
        return {
            "pipeline": {
                "download": {"done": False},
                "frames": {"done": False},
                "prepare": {"done": False},
                "analyze": {"done": False},
                "render": {"done": False},
            },
            "stats": {"downloads": 0, "frames": 0, "analyzed": 0, "planned": 0},
            "runDir": str((cls._runs_root / run_id)),
            "command": command,
            "voiceoverDraft": {"variants": []},
            "voiceoverStatus": "",
            "hasVoiceover": False,
            "videos": [],
        }

    @staticmethod
    def _is_pythonanywhere() -> bool:
        keys = ("PYTHONANYWHERE_SITE", "PYTHONANYWHERE_DOMAIN", "PYTHONANYWHERE_HOME")
        return any(str(os.environ.get(key, "") or "").strip() for key in keys)

    @classmethod
    def _recover_stale_jobs(cls) -> None:
        now = timezone.now()
        running_cutoff = now - timedelta(minutes=30)
        queued_cutoff = now - timedelta(minutes=10)
        stale_qs = ReelRenderJob.objects.filter(
            Q(status="running", created_at__lt=running_cutoff)
            | Q(status="queued", created_at__lt=queued_cutoff)
        )
        for stale in stale_qs:
            command = str((stale.payload or {}).get("command", "")).strip().lower()
            if stale.status == "queued":
                if cls.uses_external_worker():
                    stale.error = stale.error or (
                        "Job stayed queued and never started. External worker mode is enabled, but no worker "
                        "picked up this job. Start/verify the queue worker process and retry."
                    )
                elif cls._is_pythonanywhere():
                    stale.error = stale.error or (
                        "Job stayed queued and never started. PythonAnywhere web workers do not run background threads "
                        "reliably for this workflow. Use an Always-on task / queue worker, then retry."
                    )
                else:
                    stale.error = stale.error or "Job stayed queued too long and was marked failed."
            else:
                stale.error = stale.error or "Job marked stale after server restart/timeout."
            if command in {"script-draft", "voiceover-draft"} and not stale.error:
                stale.error = "Script draft timed out before completion."
            stale.status = "failed"
            stale.finished_at = now
            stale.save(update_fields=["status", "error", "finished_at"])
            run_id = str((stale.payload or {}).get("runId", "")).strip()
            if run_id:
                ReelRun.objects.filter(run_id=run_id).update(status="failed")

    @classmethod
    def start_job(cls, payload: dict) -> ReelRenderJob:
        with cls._lock:
            try:
                cls._recover_stale_jobs()
                normalized_payload = dict(payload or {})
                command = str(normalized_payload.get("command", "prepare")).strip().lower() or "prepare"
                resume_run_id = str(normalized_payload.get("resumeRunId", "")).strip()
                run_id = resume_run_id or f"{datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')}-{uuid.uuid4().hex[:4]}"
                normalized_payload["runId"] = run_id
                existing_run = ReelRun.objects.filter(run_id=run_id).first()
                existing_report = (
                    dict(existing_run.report)
                    if existing_run and isinstance(existing_run.report, dict)
                    else {}
                )
                if existing_report:
                    existing_report.setdefault("runDir", str((cls._runs_root / run_id)))
                    existing_report["command"] = command or str(existing_report.get("command", "")).strip()
                run_report = existing_report or cls._initial_run_report(command=command, run_id=run_id)
                listing_title = str(normalized_payload.get("listingTitle", "")).strip() or (
                    existing_run.listing_title if existing_run else ""
                )
                stock_id = str(normalized_payload.get("stockId", "")).strip() or (
                    existing_run.stock_id if existing_run else ""
                )
                car_description = str(normalized_payload.get("carDescription", "")).strip() or (
                    existing_run.car_description if existing_run else ""
                )
                listing_price = str(normalized_payload.get("listingPrice", "")).strip() or (
                    existing_run.listing_price if existing_run else ""
                )
                ReelRun.objects.update_or_create(
                    run_id=run_id,
                    defaults={
                        "listing_title": listing_title,
                        "stock_id": stock_id,
                        "car_description": car_description,
                        "listing_price": listing_price,
                        "status": "queued",
                        "report": run_report,
                    },
                )
                job = ReelRenderJob.objects.create(
                    job_id=f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}",
                    command=normalized_payload.get("command", "prepare"),
                    status="queued",
                    payload=normalized_payload,
                    result={"runId": run_id, "progress": {"phase": "queued", "label": "Waiting in queue...", "percent": 2}},
                )
            except (OperationalError, ProgrammingError) as exc:
                raise RuntimeError("Database schema is not up to date. Run migrations.") from exc

        if cls.uses_external_worker():
            cls._append_log(
                job,
                "Queued for external worker processing. Run the queue worker (Always-on task) to start this job.",
            )
        else:
            threading.Thread(target=cls._run_job, args=(job.id,), daemon=True).start()
        return job

    @classmethod
    def _run_job(cls, db_id: int) -> None:
        cls._job_slots.acquire()
        try:
            job = ReelRenderJob.objects.get(id=db_id)
            if job.status != "queued":
                return
            job.status = "running"
            job.started_at = timezone.now()
            job.save(update_fields=["status", "started_at"])

            try:
                source_url = str(job.payload.get("url", "")).strip()
                command = str(job.payload.get("command", "run")).strip().lower() or "run"
                if command in {"run", "prepare", "download"} and not str(job.payload.get("resumeRunId", "")).strip():
                    if not source_url:
                        raise RuntimeError("Missing source URL. Please provide a valid listing/album URL.")
                resume_run_id = str(job.payload.get("resumeRunId", "")).strip()
                run_id = str(job.payload.get("runId", "")).strip() or resume_run_id or datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
                run_dir = cls._runs_root / run_id
                ReelRun.objects.filter(run_id=run_id).update(status="running")

                if command in {"script-draft", "voiceover-draft"}:
                    cls._update_progress(job, "voiceover", "Generating script options...")
                elif command == "voiceover-apply":
                    cls._update_progress(job, "voiceover", "Applying approved voice-over...")
                elif command in {"compose", "end-scene-rerender"}:
                    cls._update_progress(job, "compose", "Starting compose...")
                else:
                    cls._update_progress(job, "download", "Starting workflow process...")
                if source_url:
                    cls._append_log(job, f"Source URL: {source_url}")
                cls._append_log(job, f"Command: {command}")
                cls._append_log(job, f"Run directory: {run_dir}")

                bridge_result = cls._run_bridge_workflow(job, command=command, run_dir=run_dir, run_id=run_id)
                report = bridge_result.get("report") if isinstance(bridge_result, dict) else None
                if not isinstance(report, dict):
                    report = cls._build_run_report_from_outputs(run_dir, command)
                bridge_run_dir = str((bridge_result or {}).get("runDir", "")).strip() if isinstance(bridge_result, dict) else ""
                report_run_dir = str((report or {}).get("runDir", "")).strip() if isinstance(report, dict) else ""
                resolved_run_dir = bridge_run_dir or report_run_dir
                if resolved_run_dir:
                    try:
                        resolved_path = Path(resolved_run_dir).resolve()
                        if resolved_path.parent == cls._runs_root.resolve():
                            run_id = resolved_path.name
                            run_dir = resolved_path
                    except Exception:
                        pass

                run_defaults = {
                    "listing_title": str(job.payload.get("listingTitle", "")).strip(),
                    "stock_id": str(job.payload.get("stockId", "")).strip(),
                    "car_description": str(job.payload.get("carDescription", "")).strip(),
                    "listing_price": str(job.payload.get("listingPrice", "")).strip(),
                    "status": "completed",
                    "report": report,
                }
                run, _created = ReelRun.objects.update_or_create(
                    run_id=run_id,
                    defaults=run_defaults,
                )

                cls._update_progress(job, "done", "Completed.")
                cls._append_log(job, f"Run created: {run.run_id}")
                job.status = "completed"
                result = job.result or {}
                result["runId"] = run.run_id
                job.result = result
                job.finished_at = timezone.now()
                job.save(update_fields=["status", "result", "finished_at"])
            except Exception as exc:  # noqa: BLE001
                cls._append_log(job, f"ERROR: {exc}")
                cls._update_progress(job, "error", "Failed.")
                job.status = "failed"
                job.error = str(exc)
                job.finished_at = timezone.now()
                job.save(update_fields=["status", "error", "finished_at"])
                run_id = str(job.payload.get("runId", "")).strip()
                if run_id:
                    existing = ReelRun.objects.filter(run_id=run_id).first()
                    report = dict((existing.report if existing else None) or {})
                    report["error"] = str(exc)
                    report["errorType"] = exc.__class__.__name__
                    report["failedAt"] = timezone.now().isoformat()
                    report["traceback"] = traceback.format_exc()
                    report["lastLogs"] = list((job.result or {}).get("logs", []))[-30:]
                    ReelRun.objects.update_or_create(
                        run_id=run_id,
                        defaults={
                            "listing_title": str(job.payload.get("listingTitle", "")).strip(),
                            "stock_id": str(job.payload.get("stockId", "")).strip(),
                            "car_description": str(job.payload.get("carDescription", "")).strip(),
                            "listing_price": str(job.payload.get("listingPrice", "")).strip(),
                            "status": "failed",
                            "report": report,
                        },
                    )
        finally:
            cls._job_slots.release()

    @classmethod
    def run_next_queued_job(cls) -> bool:
        with cls._lock:
            cls._recover_stale_jobs()
            job = ReelRenderJob.objects.filter(status="queued").order_by("created_at").first()
            if not job:
                return False
            db_id = job.id
        cls._run_job(db_id)
        return True

    @classmethod
    def _append_log(cls, job: ReelRenderJob, message: str) -> None:
        result = job.result or {}
        logs = list(result.get("logs", []))
        logs.append({"at": timezone.now().isoformat(), "message": str(message)})
        result["logs"] = logs[-250:]
        job.result = result
        job.save(update_fields=["result"])

    @classmethod
    def _run_bridge_workflow(cls, job: ReelRenderJob, *, command: str, run_dir: Path, run_id: str) -> dict:
        if not cls._bridge_path.exists():
            raise RuntimeError(f"Workflow bridge not found at: {cls._bridge_path}")
        cls._ensure_node_runtime_ready()

        payload = dict(job.payload or {})
        payload["command"] = command
        resume_run_id = str(payload.get("resumeRunId", "")).strip()
        if resume_run_id:
            payload["resumeRunId"] = resume_run_id
        else:
            payload.pop("resumeRunId", None)
            payload["outDir"] = str(run_dir)
        payload["url"] = str(payload.get("url", "")).strip()
        payload["urls"] = payload.get("urls") or ([payload["url"]] if payload["url"] else [])
        payload["headless"] = not bool(payload.get("headful", False))

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".json", delete=False) as tmp:
            tmp.write(json.dumps(payload))
            payload_file = tmp.name

        cmd = [
            cls._node_bin,
            str(cls._bridge_path),
            "--payload",
            payload_file,
        ]
        proc = subprocess.Popen(
            cmd,
            cwd=str(cls._workflow_root),
            env=cls._workflow_subprocess_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        timeout_seconds = int(cls._COMMAND_TIMEOUT_SECONDS.get(command, int(os.environ.get("REAL_FOOTAGE_TIMEOUT_DEFAULT_SEC", "1200") or "1200")))
        timed_out = {"value": False}
        timed_out_reason = {
            "message": (
                f"Workflow timed out after {timeout_seconds}s during '{command}'. "
                "Likely causes: Chromium/Playwright blocked, ffmpeg unavailable, low server CPU/RAM, or album access challenge."
            )
        }

        def _kill_if_timeout() -> None:
            if proc.poll() is not None:
                return
            timed_out["value"] = True
            cls._append_log(job, timed_out_reason["message"])
            with contextlib.suppress(Exception):
                proc.terminate()
            time.sleep(3)
            if proc.poll() is None:
                with contextlib.suppress(Exception):
                    proc.kill()

        watchdog = threading.Timer(timeout_seconds, _kill_if_timeout)
        watchdog.daemon = True
        watchdog.start()
        with cls._process_lock:
            cls._active_processes[job.job_id] = proc
        result_payload = None
        bridge_errors: list[str] = []
        raw_output_tail: list[str] = []
        try:
            stream = proc.stdout
            if stream is not None:
                for raw_line in stream:
                    line = str(raw_line).rstrip("\r\n")
                    if not line:
                        continue
                    raw_output_tail.append(line)
                    if len(raw_output_tail) > 60:
                        raw_output_tail = raw_output_tail[-60:]
                    if line.startswith("[LOG] "):
                        cls._append_log(job, line[6:])
                        cls._infer_progress_from_log(job, line[6:])
                        continue
                    if line.startswith("[PROGRESS] "):
                        try:
                            progress = json.loads(line[11:])
                            if isinstance(progress, dict):
                                cls._update_progress(
                                    job,
                                    str(progress.get("phase", "")),
                                    str(progress.get("label", "")),
                                )
                        except Exception:
                            cls._append_log(job, line)
                        continue
                    if line.startswith("[RESULT] "):
                        try:
                            parsed = json.loads(line[9:])
                            if isinstance(parsed, dict):
                                result_payload = parsed
                        except Exception:
                            cls._append_log(job, "Failed parsing bridge result payload.")
                        continue
                    if line.startswith("[ERROR] "):
                        error_line = line[8:].strip()
                        if error_line:
                            bridge_errors.append(error_line)
                        cls._append_log(job, line[8:])
                        continue
                    cls._append_log(job, line)
            proc.wait()
        finally:
            with contextlib.suppress(Exception):
                watchdog.cancel()
            with cls._process_lock:
                cls._active_processes.pop(job.job_id, None)
            try:
                Path(payload_file).unlink(missing_ok=True)
            except Exception:
                pass

        if proc.returncode != 0:
            if timed_out["value"]:
                raise RuntimeError(timed_out_reason["message"])
            if bridge_errors:
                raise RuntimeError(bridge_errors[-1])
            # Bubble up the most useful recent bridge output so we don't lose root-cause details.
            tail_preview = " | ".join(raw_output_tail[-8:]).strip()
            if tail_preview:
                raise RuntimeError(f"Pipeline bridge exited with code {proc.returncode}. Details: {tail_preview}")
            raise RuntimeError(f"Pipeline bridge exited with code {proc.returncode}.")
        return result_payload or {}

    @classmethod
    def generate_thumbnail(
        cls,
        *,
        run_id: str,
        title: str,
        subtitle: str,
        reference_image_data_url: str,
        price: str = "",
    ) -> dict:
        if not cls._bridge_path.exists():
            raise RuntimeError(f"Workflow bridge not found at: {cls._bridge_path}")
        cls._ensure_node_runtime_ready()

        payload = {
            "command": "thumbnail",
            "resumeRunId": str(run_id or "").strip(),
            "title": str(title or "").strip(),
            "subtitle": str(subtitle or "").strip(),
            "referenceImageDataUrl": str(reference_image_data_url or "").strip(),
            "price": str(price or "").strip(),
            "headless": True,
        }

        if not payload["resumeRunId"]:
            raise RuntimeError("Run ID is required.")
        if not payload["title"]:
            raise RuntimeError("title is required.")
        if not payload["subtitle"]:
            raise RuntimeError("subtitle is required.")
        if not payload["referenceImageDataUrl"]:
            raise RuntimeError("referenceImageDataUrl is required.")

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".json", delete=False) as tmp:
            tmp.write(json.dumps(payload))
            payload_file = tmp.name

        cmd = [
            cls._node_bin,
            str(cls._bridge_path),
            "--payload",
            payload_file,
        ]
        proc = subprocess.Popen(
            cmd,
            cwd=str(cls._workflow_root),
            env=cls._workflow_subprocess_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

        result_payload = None
        bridge_errors: list[str] = []
        try:
            stream = proc.stdout
            if stream is not None:
                for raw_line in stream:
                    line = str(raw_line).rstrip("\r\n")
                    if not line:
                        continue
                    if line.startswith("[RESULT] "):
                        try:
                            parsed = json.loads(line[9:])
                            if isinstance(parsed, dict):
                                result_payload = parsed
                        except Exception:
                            pass
                        continue
                    if line.startswith("[ERROR] "):
                        error_line = line[8:].strip()
                        if error_line:
                            bridge_errors.append(error_line)
                        continue
            proc.wait()
        finally:
            try:
                Path(payload_file).unlink(missing_ok=True)
            except Exception:
                pass

        if proc.returncode != 0:
            if bridge_errors:
                raise RuntimeError(bridge_errors[-1])
            raise RuntimeError(f"Pipeline bridge exited with code {proc.returncode}.")

        if not isinstance(result_payload, dict):
            raise RuntimeError("Thumbnail generation did not return a valid result payload.")

        image_path = str(result_payload.get("imagePath", "")).strip()
        image_mime_type = str(result_payload.get("imageMimeType", "")).strip()
        if not image_path:
            raise RuntimeError("Thumbnail generation did not return imagePath.")

        return {
            "runDir": str(result_payload.get("runDir", "")).strip(),
            "imagePath": image_path,
            "imageMimeType": image_mime_type or "image/png",
        }

    @classmethod
    def _update_progress(cls, job: ReelRenderJob, phase: str, label: str) -> None:
        result = job.result or {}
        result["progress"] = {
            "phase": phase,
            "label": label,
            "percent": int(cls._PHASE_PERCENT.get(phase, 0)),
        }
        job.result = result
        job.save(update_fields=["result"])

    @classmethod
    def _start_pipeline_process(cls, job: ReelRenderJob, *, command: str, run_dir: Path) -> subprocess.Popen:
        if not cls._cli_path.exists():
            raise RuntimeError(f"Workflow CLI not found at: {cls._cli_path}")
        cls._ensure_node_runtime_ready()

        payload = job.payload or {}
        cmd = [
            cls._node_bin,
            str(cls._cli_path),
            command,
            "--url",
            str(payload.get("url", "")).strip(),
            "--out",
            str(run_dir),
        ]

        max_clips = payload.get("maxClips")
        if isinstance(max_clips, int) and max_clips > 0:
            cmd += ["--max-clips", str(max_clips)]

        if bool(payload.get("compose")):
            cmd.append("--compose")
        if bool(payload.get("headful")):
            cmd.append("--headful")

        return subprocess.Popen(
            cmd,
            cwd=str(cls._workflow_root),
            env=cls._workflow_subprocess_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

    @classmethod
    def _workflow_subprocess_env(cls) -> dict:
        env = dict(os.environ)
        for key in cls._FORWARDED_ENV_KEYS:
            value = str(os.environ.get(key, "") or "").strip()
            if value:
                env[key] = value
        return env

    @classmethod
    def _ensure_node_runtime_ready(cls) -> None:
        if not cls._workflow_package_json.exists():
            raise RuntimeError(f"workflow_engine package.json not found: {cls._workflow_package_json}")
        if not cls._workflow_node_modules.exists():
            raise RuntimeError(
                "workflow_engine/node_modules is missing. "
                "Run: `cd apps/real_footage_reels/workflow_engine && npm ci` on the server."
            )

    @staticmethod
    def _is_placeholder_secret(value: str) -> bool:
        normalized = str(value or "").strip().lower()
        placeholders = {
            "",
            "your_gemini_api_key_here",
            "your_api_key_here",
            "paste_your_gemini_api_key_here",
            "replace_with_your_gemini_api_key",
            "replace_me",
            "changeme",
        }
        return normalized in placeholders

    @classmethod
    def workflow_debug_status(cls) -> dict:
        env_key = str(os.environ.get("GEMINI_API_KEY", "") or "").strip()
        env_has_gemini = not cls._is_placeholder_secret(env_key)
        file_has_gemini = False
        env_file_exists = cls._workflow_env_file.exists()
        env_file_error = ""
        if env_file_exists:
            try:
                for raw_line in cls._workflow_env_file.read_text(encoding="utf-8").splitlines():
                    line = raw_line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    if key.strip() == "GEMINI_API_KEY":
                        cleaned = value.strip().strip('"').strip("'")
                        file_has_gemini = not cls._is_placeholder_secret(cleaned)
                        break
            except Exception as exc:  # noqa: BLE001
                env_file_error = str(exc)

        node_ok = False
        node_version = ""
        node_error = ""
        try:
            proc = subprocess.run(
                [cls._node_bin, "--version"],
                cwd=str(cls._workflow_root),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=8,
                check=False,
            )
            if proc.returncode == 0:
                node_ok = True
                node_version = str(proc.stdout or "").strip() or str(proc.stderr or "").strip()
            else:
                node_error = str(proc.stderr or proc.stdout or "").strip() or f"node exited with {proc.returncode}"
        except Exception as exc:  # noqa: BLE001
            node_error = str(exc)

        active_jobs = ReelRenderJob.objects.filter(status__in=["queued", "running"]).count()
        return {
            "host": {
                "pythonAnywhere": cls._is_pythonanywhere(),
                "pid": os.getpid(),
            },
            "node": {
                "bin": cls._node_bin,
                "ok": node_ok,
                "version": node_version,
                "error": node_error,
            },
            "workflow": {
                "root": str(cls._workflow_root),
                "bridgeExists": cls._bridge_path.exists(),
                "packageJsonExists": cls._workflow_package_json.exists(),
                "nodeModulesExists": cls._workflow_node_modules.exists(),
            },
            "keys": {
                "geminiInProcessEnv": env_has_gemini,
                "geminiInDotEnv": file_has_gemini,
                "envFileExists": env_file_exists,
                "envFileError": env_file_error,
            },
            "jobs": {
                "activeCount": active_jobs,
                "maxParallelJobs": cls._max_parallel_jobs,
                "workerMode": "external" if cls.uses_external_worker() else "inline-thread",
            },
        }

    @classmethod
    def _stream_process_output(cls, job: ReelRenderJob, proc: subprocess.Popen) -> None:
        stream = proc.stdout
        if stream is None:
            proc.wait()
            return

        for raw_line in stream:
            line = str(raw_line).strip()
            if not line:
                continue
            cls._append_log(job, line)
            cls._infer_progress_from_log(job, line)
        proc.wait()

    @classmethod
    def _infer_progress_from_log(cls, job: ReelRenderJob, line: str) -> None:
        lower = line.lower()
        if "downloading album videos" in lower or "downloaded " in lower:
            cls._update_progress(job, "download", "Downloading clips...")
            return
        if "extracting" in lower and "frame" in lower:
            cls._update_progress(job, "frames", "Extracting frames...")
            return
        if "sending them to gemini" in lower or "classified " in lower:
            cls._update_progress(job, "analyze", "Analyzing and planning...")
            return
        if "main reel encoding heartbeat" in lower:
            cls._update_progress(job, "compose", "Composing main reel...")
            return
        if "rendering end scene" in lower or "end scene frames" in lower or "building branded end scene" in lower:
            cls._update_progress(job, "compose", "Rendering end scene...")
            return
        if "concatenating main reel + end scene" in lower:
            cls._update_progress(job, "compose", "Finalizing reel...")
            return
        if "composing the selected local clips" in lower:
            cls._update_progress(job, "compose", "Composing reel + end scene...")
            return
        if "synthesizing speech with elevenlabs" in lower:
            cls._update_progress(job, "voiceover", "Generating voice-over audio...")
            return
        if "muxing final reel with voice-over audio" in lower:
            cls._update_progress(job, "voiceover", "Stitching voice-over into reel...")
            return
        if "publishing mp4 final reel" in lower or "publishing output" in lower:
            cls._update_progress(job, "publish", "Publishing output...")
            return
        if "building preview mp4 for faster playback" in lower:
            cls._update_progress(job, "publish", "Building preview stream...")
            return
        if "voice-over complete" in lower:
            cls._update_progress(job, "voiceover", "Voice-over complete.")
            return
        if "voice-over scripts" in lower or "script options" in lower:
            cls._update_progress(job, "voiceover", "Voice-over scripts...")
            return
        if "composed reel:" in lower:
            cls._update_progress(job, "compose", "Finalizing reel...")

    @classmethod
    def _build_run_report_from_outputs(cls, run_dir: Path, command: str) -> dict:
        downloads_manifest = cls._read_json(run_dir / "downloads-manifest.json") or {}
        frames_manifest = cls._read_json(run_dir / "frames-manifest.json") or {}
        analysis_manifest = cls._read_json(run_dir / "analysis.json") or {}
        reel_plan = cls._read_json(run_dir / "reel-plan.json") or {}
        voiceover_draft_manifest = cls._read_json(run_dir / "voiceover-script-draft.json") or {}
        voiceover_status_manifest = cls._read_json(run_dir / "voiceover-status.json") or {}
        voiceover_manifest = cls._read_json(run_dir / "voiceover-manifest.json") or {}
        publish_manifest = cls._read_json(run_dir / "final-reel-publish.json") or {}

        downloaded_videos = downloads_manifest.get("videos", []) if isinstance(downloads_manifest, dict) else []
        framed_videos = frames_manifest.get("videos", []) if isinstance(frames_manifest, dict) else []
        analyzed_clips = analysis_manifest.get("clips", []) if isinstance(analysis_manifest, dict) else []
        planned_segments = (
            ((reel_plan.get("composition") or {}).get("segments") or [])
            if isinstance(reel_plan, dict)
            else []
        )

        final_reel_webm = run_dir / "final-reel.webm"
        final_reel_mp4 = run_dir / "final-reel.mp4"
        final_reel_preview_mp4 = run_dir / "final-reel-preview.mp4"
        main_reel_mp4 = run_dir / "main-reel.mp4"
        main_reel_webm = run_dir / "main-reel.webm"
        draft_variants = (
            voiceover_draft_manifest.get("variants", [])
            if isinstance(voiceover_draft_manifest, dict)
            else []
        )
        has_voiceover = isinstance(voiceover_manifest, dict) and bool(voiceover_manifest)
        remote_final_url = (
            str((publish_manifest or {}).get("cdnUrl", "")).strip()
            if isinstance(publish_manifest, dict)
            else ""
        )
        remote_preview_url = (
            str((publish_manifest or {}).get("previewCdnUrl", "")).strip()
            if isinstance(publish_manifest, dict)
            else ""
        )
        if not remote_final_url.lower().startswith(("http://", "https://")):
            remote_final_url = ""
        if not remote_preview_url.lower().startswith(("http://", "https://")):
            remote_preview_url = ""
        remote_publish_ok = (
            bool((publish_manifest or {}).get("ok"))
            if isinstance(publish_manifest, dict)
            else False
        ) and bool(remote_final_url)
        remote_publish_error = (
            str((publish_manifest or {}).get("error", "")).strip()
            if isinstance(publish_manifest, dict)
            else ""
        )
        voiceover_status = (
            "applied"
            if has_voiceover
            else str((voiceover_status_manifest or {}).get("status", "")).strip()
        )

        return {
            "pipeline": {
                "download": {"done": len(downloaded_videos) > 0},
                "frames": {"done": len(framed_videos) > 0},
                "prepare": {"done": len(framed_videos) > 0},
                "analyze": {"done": len(analyzed_clips) > 0},
                "render": {"done": final_reel_webm.exists() or final_reel_mp4.exists()},
            },
            "stats": {
                "downloads": len(downloaded_videos),
                "frames": sum(len(v.get("framePaths", []) or []) for v in framed_videos if isinstance(v, dict)),
                "analyzed": len(analyzed_clips),
                "planned": len(planned_segments),
            },
            "downloadsManifest": downloads_manifest,
            "framesManifest": frames_manifest,
            "analysis": analysis_manifest,
            "plan": reel_plan,
            "runDir": str(run_dir),
            "command": command,
            "voiceoverDraft": (
                {
                    "status": str(voiceover_draft_manifest.get("status", "pending")).strip() or "pending",
                    "createdAt": voiceover_draft_manifest.get("createdAt"),
                    "appliedAt": voiceover_draft_manifest.get("appliedAt"),
                    "variants": draft_variants if isinstance(draft_variants, list) else [],
                }
                if isinstance(voiceover_draft_manifest, dict) and voiceover_draft_manifest
                else {"variants": []}
            ),
            "voiceoverStatus": voiceover_status,
            "voiceoverLastError": str((voiceover_status_manifest or {}).get("lastError", "")).strip(),
            "hasVoiceover": has_voiceover,
            "mainReelUrl": str(main_reel_mp4 if main_reel_mp4.exists() else (main_reel_webm if main_reel_webm.exists() else "")),
            "finalReelUrl": remote_final_url or str(
                final_reel_mp4 if final_reel_mp4.exists() else (final_reel_webm if final_reel_webm.exists() else "")
            ),
            "finalReelRemoteUrl": remote_final_url,
            "finalReelRemoteUploadOk": remote_publish_ok,
            "finalReelRemoteError": remote_publish_error,
            "finalReelPreviewUrl": remote_preview_url or (str(final_reel_preview_mp4) if final_reel_preview_mp4.exists() else ""),
            "finalReelWebmUrl": str(final_reel_webm) if final_reel_webm.exists() else "",
            "videos": analyzed_clips if analyzed_clips else framed_videos,
        }

    @staticmethod
    def _read_json(path: Path) -> dict | list | None:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    @staticmethod
    def jobs_payload() -> dict:
        try:
            ReelRenderService._recover_stale_jobs()
            jobs = list(ReelRenderJob.objects.all()[:50])
        except (OperationalError, ProgrammingError):
            return {"activeJobId": None, "activeJobIds": [], "maxParallelJobs": ReelRenderService._max_parallel_jobs, "jobs": []}
        active_jobs = [j for j in jobs if j.status in {"queued", "running"}]
        active = active_jobs[0] if active_jobs else None
        return {
            "activeJobId": active.job_id if active else None,
            "activeJobIds": [j.job_id for j in active_jobs],
            "maxParallelJobs": ReelRenderService._max_parallel_jobs,
            "workerMode": "external" if ReelRenderService.uses_external_worker() else "inline-thread",
            "jobs": [ReelRenderService._job_to_public(j) for j in jobs],
        }

    @classmethod
    def control_job(cls, job_id: str, action: str) -> dict:
        normalized_job_id = str(job_id or "").strip()
        normalized_action = str(action or "").strip().lower()
        if not normalized_job_id:
            raise RuntimeError("Missing job id.")
        if normalized_action not in {"pause", "resume", "stop", "remove"}:
            raise RuntimeError("Unsupported action.")

        job = ReelRenderJob.objects.filter(job_id=normalized_job_id).first()
        if not job:
            raise RuntimeError("Job not found.")

        run_id = str((job.payload or {}).get("runId", "")).strip()

        if normalized_action == "pause":
            if job.status != "queued":
                raise RuntimeError("Only queued jobs can be paused.")
            job.status = "paused"
            job.save(update_fields=["status"])
            cls._update_progress(job, "queued", "Paused.")
            if run_id:
                ReelRun.objects.filter(run_id=run_id).update(status="paused")
            return cls._job_to_public(job)

        if normalized_action == "resume":
            if job.status != "paused":
                raise RuntimeError("Only paused jobs can be resumed.")
            job.status = "queued"
            job.save(update_fields=["status"])
            cls._update_progress(job, "queued", "Waiting in queue...")
            if run_id:
                ReelRun.objects.filter(run_id=run_id).update(status="queued")
            if cls.uses_external_worker():
                cls._append_log(
                    job,
                    "Resumed and queued for external worker processing.",
                )
            else:
                threading.Thread(target=cls._run_job, args=(job.id,), daemon=True).start()
            job.refresh_from_db()
            return cls._job_to_public(job)

        if normalized_action == "stop":
            if job.status in {"completed", "failed", "cancelled"}:
                return cls._job_to_public(job)
            with cls._process_lock:
                proc = cls._active_processes.get(normalized_job_id)
            if proc and proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
            job.status = "cancelled"
            job.error = "Stopped by user."
            job.finished_at = timezone.now()
            job.save(update_fields=["status", "error", "finished_at"])
            cls._update_progress(job, "error", "Stopped.")
            if run_id:
                ReelRun.objects.filter(run_id=run_id).update(status="failed")
            return cls._job_to_public(job)

        if job.status == "running":
            raise RuntimeError("Stop the running job before removing it.")

        job_public = cls._job_to_public(job)
        job.delete()
        if run_id:
            ReelRun.objects.filter(run_id=run_id, status__in=["queued", "paused", "failed"]).delete()
        return job_public

    @staticmethod
    def _duration_label(total_seconds: float) -> str:
        seconds = max(0, int(total_seconds))
        if seconds < 60:
            return f"{seconds}s"
        minutes, rem = divmod(seconds, 60)
        if minutes < 60:
            return f"{minutes}m {rem}s"
        hours, mins = divmod(minutes, 60)
        return f"{hours}h {mins}m"

    @classmethod
    def _latest_jobs_by_run_id(cls, limit: int = 500) -> dict[str, ReelRenderJob]:
        latest: dict[str, ReelRenderJob] = {}
        for job in ReelRenderJob.objects.all()[:limit]:
            payload = job.payload or {}
            result = job.result or {}
            run_id = str(payload.get("runId") or result.get("runId") or "").strip()
            if not run_id or run_id in latest:
                continue
            latest[run_id] = job
        return latest

    @classmethod
    def _derive_debug_reason(
        cls,
        *,
        run: ReelRun,
        run_error: str,
        latest_job: ReelRenderJob | None,
    ) -> str:
        report = run.report or {}
        pipeline = report.get("pipeline", {}) if isinstance(report, dict) else {}
        has_render = bool((pipeline.get("render") or {}).get("done"))
        has_scripts = bool(((report.get("voiceoverDraft") or {}).get("variants") or []))
        has_any_pipeline_step = any(
            bool((pipeline.get(step) or {}).get("done"))
            for step in ("download", "frames", "analyze", "render")
        )
        run_status = str(run.status or "").strip().lower()
        if run_status in {"failed", "cancelled"}:
            return run_error

        if latest_job is None:
            if run_status in {"queued", "running"}:
                age_seconds = (timezone.now() - run.updated_at).total_seconds()
                if age_seconds > 90:
                    base = f"Run is {run_status} but has no active job record for {cls._duration_label(age_seconds)}."
                    if cls.uses_external_worker():
                        return (
                            f"{base} External worker mode is enabled. Start/verify the queue worker "
                            "process to consume queued jobs."
                        )
                    if cls._is_pythonanywhere():
                        return (
                            f"{base} PythonAnywhere web workers do not support this thread-based background runner; "
                            "move processing to an Always-on task/queue worker."
                        )
                    return f"{base} Check worker process logs."
            return ""

        job_status = str(latest_job.status or "").strip().lower()
        progress = (latest_job.result or {}).get("progress", {}) if isinstance(latest_job.result, dict) else {}
        phase = str(progress.get("phase", "")).strip().lower()
        label = str(progress.get("label", "")).strip()
        command = str(latest_job.command or (latest_job.payload or {}).get("command", "") or "").strip().lower()
        job_error = str(latest_job.error or "").strip()

        if job_status in {"failed", "cancelled"}:
            return job_error or run_error or label or "Job failed."

        if job_status == "completed" and command in {"script-draft", "voiceover-draft"}:
            if not has_scripts:
                if not has_any_pipeline_step and not has_render:
                    return (
                        "Script-draft job completed but produced zero script variants. "
                        "Most common causes: missing/placeholder GEMINI_API_KEY or empty car description."
                    )
                return "Script-draft completed without script variants. Check job logs for Gemini/validation issues."
            return ""

        if job_status == "queued" and latest_job.started_at is None:
            age_seconds = (timezone.now() - latest_job.created_at).total_seconds()
            if age_seconds > 90:
                base = f"Job has been queued for {cls._duration_label(age_seconds)} and has not started."
                if cls.uses_external_worker():
                    return (
                        f"{base} External worker mode is enabled. Start/verify the queue worker process "
                        "(for PythonAnywhere: Always-on task) to process queued jobs."
                    )
                if cls._is_pythonanywhere():
                    return (
                        f"{base} PythonAnywhere WSGI web apps do not support this background thread pattern. "
                        "Use an Always-on task or queue worker."
                    )
                return f"{base} Check worker capacity or background thread startup."

        if job_status == "running":
            started_at = latest_job.started_at or latest_job.created_at
            age_seconds = (timezone.now() - started_at).total_seconds()
            warn_seconds = int(os.environ.get("REAL_FOOTAGE_RUNNING_WARN_SEC", "900") or "900")
            if phase == "compose":
                warn_seconds = max(warn_seconds, int(os.environ.get("REAL_FOOTAGE_COMPOSE_WARN_SEC", "1200") or "1200"))
            if age_seconds > warn_seconds and phase in {"voiceover", "download", "frames", "analyze", "compose", "publish"}:
                return f"Job has been running for {cls._duration_label(age_seconds)} at phase '{phase or 'unknown'}'. Last update: {label or '-'}."

        return ""

    @staticmethod
    def _job_summary(job: ReelRenderJob | None) -> dict | None:
        if job is None:
            return None
        result = job.result or {}
        progress = result.get("progress", {}) if isinstance(result, dict) else {}
        payload = job.payload or {}
        return {
            "id": job.job_id,
            "runId": payload.get("runId") or result.get("runId"),
            "command": job.command,
            "status": job.status,
            "createdAt": job.created_at.isoformat(),
            "startedAt": job.started_at.isoformat() if job.started_at else None,
            "finishedAt": job.finished_at.isoformat() if job.finished_at else None,
            "error": job.error or None,
            "progress": progress if isinstance(progress, dict) else {},
        }

    @classmethod
    def runs_payload(cls) -> dict:
        try:
            cls._recover_stale_jobs()
            runs = ReelRun.objects.all()[:100]
            latest_jobs_by_run_id = cls._latest_jobs_by_run_id(limit=500)
        except (OperationalError, ProgrammingError):
            return {"runs": []}
        return {
            "runs": [
                {
                    "runId": r.run_id,
                    "createdAt": r.created_at.isoformat(),
                    "updatedAt": r.updated_at.isoformat(),
                    "listingTitle": r.listing_title,
                    "stockId": r.stock_id,
                    "listingPrice": r.listing_price,
                    "status": r.status,
                    # ReelRun has no DB-level `error` field in older/live schemas.
                    # Keep API backward-compatible by deriving error text safely.
                    "error": (
                        str(getattr(r, "error", "") or "").strip()
                        or str((r.report or {}).get("error", "")).strip()
                        or str((r.report or {}).get("lastError", "")).strip()
                    ),
                    "pipeline": (r.report or {}).get("pipeline", {}),
                    "stats": {
                        "downloads": int((r.report or {}).get("stats", {}).get("downloads", 0)),
                        "frames": int((r.report or {}).get("stats", {}).get("frames", 0)),
                        "analyzed": int((r.report or {}).get("stats", {}).get("analyzed", 0)),
                        "planned": int((r.report or {}).get("stats", {}).get("planned", 0)),
                    },
                    "voiceoverDraft": (r.report or {}).get("voiceoverDraft", {"variants": []}),
                    "voiceoverStatus": (r.report or {}).get("voiceoverStatus", ""),
                    "hasVoiceover": bool((r.report or {}).get("hasVoiceover", False)),
                    "lastJob": cls._job_summary(latest_jobs_by_run_id.get(r.run_id)),
                    "debugReason": cls._derive_debug_reason(
                        run=r,
                        run_error=(
                            str(getattr(r, "error", "") or "").strip()
                            or str((r.report or {}).get("error", "")).strip()
                            or str((r.report or {}).get("lastError", "")).strip()
                        ),
                        latest_job=latest_jobs_by_run_id.get(r.run_id),
                    ),
                }
                for r in runs
            ]
        }

    @staticmethod
    def delete_run(run_id: str) -> bool:
        try:
            deleted, _ = ReelRun.objects.filter(run_id=run_id).delete()
            return deleted > 0
        except (OperationalError, ProgrammingError):
            return False

    @staticmethod
    def _job_to_public(j: ReelRenderJob) -> dict:
        payload = j.payload or {}
        result = j.result or {}
        return {
            "id": j.job_id,
            "runId": payload.get("runId") or result.get("runId"),
            "command": j.command,
            "urls": [payload["url"]] if payload.get("url") else payload.get("urls", []),
            "listingTitle": payload.get("listingTitle", ""),
            "stockId": payload.get("stockId", ""),
            "carDescription": payload.get("carDescription", ""),
            "listingPrice": payload.get("listingPrice", ""),
            "priceIncludes": payload.get("priceIncludes", None),
            "maxClips": payload.get("maxClips", None),
            "compose": bool(payload.get("compose", False)),
            "headless": not bool(payload.get("headful", False)),
            "status": j.status,
            "createdAt": j.created_at.isoformat(),
            "startedAt": j.started_at.isoformat() if j.started_at else None,
            "finishedAt": j.finished_at.isoformat() if j.finished_at else None,
            "logs": list(result.get("logs", [])),
            "result": result,
            "error": j.error or None,
            "resumeRunId": payload.get("resumeRunId"),
            "sourcePayload": payload,
            "progress": result.get("progress"),
            "voiceoverScriptApproval": bool(payload.get("voiceoverScriptApproval", True)),
        }


class VehicleInventoryService:
    _cache_path = Path(__file__).resolve().parent / "workflow_engine" / ".ui-cache" / "all_stock.json"
    _api_url = "https://www.cbs.s1.carbarn.com.au/carbarnau/api/v1/vehicles"

    @classmethod
    def _load_payload(cls) -> dict:
        if not cls._cache_path.exists():
            return {}
        try:
            payload = json.loads(cls._cache_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
        except Exception:
            return {}
        return {}

    @classmethod
    def _load(cls) -> list[dict]:
        payload = cls._load_payload()
        if isinstance(payload.get("vehicles"), list):
            return payload["vehicles"]
        if isinstance(payload, list):
            return payload
        return []

    @classmethod
    def status(cls) -> dict:
        payload = cls._load_payload()
        vehicles = cls._load()
        return {
            "cachePath": str(cls._cache_path),
            "cachedAt": payload.get("cachedAt"),
            "count": len(vehicles),
            "refreshing": False,
            "lastError": "",
        }

    @classmethod
    def search(cls, query: str, limit: int = 20) -> dict:
        q = (query or "").strip().lower()
        vehicles = cls._load()
        if not q:
            return {"query": query, "count": len(vehicles), "matches": []}
        out = []
        for v in vehicles:
            title = str(v.get("title", "")).lower()
            stock = str(v.get("stockNo", "")).lower()
            if q in title or q in stock:
                out.append(v)
            if len(out) >= limit:
                break
        return {"query": query, "count": len(vehicles), "matches": out}

    @classmethod
    def refresh(cls) -> dict:
        vehicles = []
        page = 0
        total_pages = None

        while True:
            query = urlencode(
                {
                    "page": page,
                    "size": 500,
                    "sort": "id,asc",
                    "soldStatus": "UnSold",
                }
            )
            url = f"{cls._api_url}?{query}"
            req = Request(url, headers={"accept": "application/json"})
            with urlopen(req, timeout=30) as resp:  # noqa: S310
                payload = json.loads(resp.read().decode("utf-8"))

            content = payload.get("content") or []
            if not content:
                break
            vehicles.extend(content)

            if total_pages is None:
                try:
                    total_pages = int(((payload.get("page") or {}).get("totalPages")) or 0)
                except Exception:
                    total_pages = 0

            page += 1
            if total_pages and page >= total_pages:
                break

            if page > 3000:
                break

        cls._cache_path.parent.mkdir(parents=True, exist_ok=True)
        data = {"cachedAt": datetime.utcnow().isoformat(), "vehicles": vehicles}
        cls._cache_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return {"ok": True, "count": len(vehicles), "cachedAt": data["cachedAt"]}
