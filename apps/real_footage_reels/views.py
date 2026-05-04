import mimetypes
import re
import traceback
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlparse

from django.http import FileResponse, Http404, HttpResponse, StreamingHttpResponse
from django.shortcuts import get_object_or_404, render
from django.conf import settings
from django.views import View
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import ReelRun
from .services import ReelRenderService, VehicleInventoryService

_WORKFLOW_ROOT = Path(__file__).resolve().parent / "workflow_engine"
_LEGACY_WORKFLOW_ROOT = Path(__file__).resolve().parent / "Carbarn-Au-real-footage-reels"
_BYTE_RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$", re.IGNORECASE)


class _UnsatisfiableRangeError(Exception):
    """Raised when a valid-by-syntax range cannot be satisfied for a resource size."""


def _iter_file_bytes(open_file, *, start: int, end: int, chunk_size: int = 1024 * 64):
    remaining = end - start + 1
    open_file.seek(start)
    try:
        while remaining > 0:
            chunk = open_file.read(min(chunk_size, remaining))
            if not chunk:
                break
            remaining -= len(chunk)
            yield chunk
    finally:
        open_file.close()


def _parse_single_byte_range(range_header: str, *, file_size: int) -> tuple[int, int] | None:
    header = str(range_header or "").strip()
    if not header:
        return None

    # Keep implementation intentionally simple and robust:
    # only a single "bytes=start-end" range is supported.
    if "," in header:
        return None

    match = _BYTE_RANGE_RE.match(header)
    if not match:
        return None

    start_raw, end_raw = match.groups()
    if not start_raw and not end_raw:
        return None

    if file_size <= 0:
        raise _UnsatisfiableRangeError

    if not start_raw:
        suffix_length = int(end_raw)
        if suffix_length <= 0:
            return None
        start = max(file_size - suffix_length, 0)
        end = file_size - 1
        return start, end

    start = int(start_raw)
    if start >= file_size:
        raise _UnsatisfiableRangeError

    if not end_raw:
        end = file_size - 1
    else:
        end = int(end_raw)
        if end < start:
            return None
        end = min(end, file_size - 1)

    return start, end


def _asset_url(run_id: str, abs_path: str) -> str:
    encoded = quote(str(abs_path), safe="")
    return f"/workflows/real-footage-reels/api/runs/{run_id}/asset?path={encoded}"


def _pick_path(item: dict, *keys: str) -> str:
    for key in keys:
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _resolve_report_asset_path(run_dir_value: str, raw_value: str) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""

    parsed = urlparse(value)
    if parsed.path.endswith("/api/file"):
        encoded = parse_qs(parsed.query).get("path", [""])[0]
        value = unquote(str(encoded or "")).strip()
        if not value:
            return ""

    candidate = Path(value)
    if candidate.is_absolute():
        return str(candidate)

    normalized = value.replace("\\", "/").lstrip("/")
    if normalized.startswith("runs/"):
        # Support both migrated and legacy roots; prefer an existing file.
        candidates = [
            (_WORKFLOW_ROOT / normalized).resolve(),
            (_LEGACY_WORKFLOW_ROOT / normalized).resolve(),
        ]
        for candidate_path in candidates:
            if candidate_path.exists():
                return str(candidate_path)
        return str(candidates[0])

    run_dir_text = str(run_dir_value or "").strip()
    if not run_dir_text:
        return value

    try:
        run_dir = Path(run_dir_text).resolve()
    except Exception:
        return value

    return str((run_dir / normalized).resolve())


def _resolve_run_dir_path(run_dir_value: str) -> Path | None:
    run_dir_text = str(run_dir_value or "").strip()
    if not run_dir_text:
        return None

    raw_path = Path(run_dir_text)
    try:
        candidate = raw_path.resolve()
    except Exception:
        candidate = raw_path

    if candidate.exists():
        return candidate

    # Backward compatibility after moving from legacy folder name to workflow_engine.
    try:
        relative = candidate.relative_to(_LEGACY_WORKFLOW_ROOT)
        migrated = (_WORKFLOW_ROOT / relative).resolve()
        if migrated.exists():
            return migrated
    except Exception:
        pass

    return candidate


def _decorate_media_item(run_id: str, run_dir_value: str, item: dict) -> dict:
    if not isinstance(item, dict):
        return {}
    video_path = _pick_path(item, "videoPath", "filePath", "path")
    if not video_path:
        video_path = _resolve_report_asset_path(run_dir_value, _pick_path(item, "videoUrl"))
    frame_path = _pick_path(item, "framePath")
    if not frame_path:
        frame_paths = item.get("framePaths")
        if isinstance(frame_paths, list) and frame_paths:
            first = frame_paths[0]
            if isinstance(first, str):
                frame_path = first
    if not frame_path:
        frame_path = _resolve_report_asset_path(run_dir_value, _pick_path(item, "frameUrl"))
    return {
        **item,
        "videoUrl": _asset_url(run_id, video_path) if video_path else "",
        "frameUrl": _asset_url(run_id, frame_path) if frame_path else "",
    }


class WorkflowHomeView(View):
    template_name = "real_footage_reels/home.html"

    def get(self, request, *args, **kwargs):
        return render(
            request,
            self.template_name,
            {
                "route_run_id": kwargs.get("run_id", ""),
                "request_path": request.path,
            },
        )


class MetaApiView(APIView):
    def get(self, request):
        return Response(
            {
                "appName": "AU Real Footage Reels",
                "features": {"analysisEnabled": True, "voiceoverEnabled": False},
            }
        )


class JobsApiView(APIView):
    def get(self, request, *args, **kwargs):
        return Response(ReelRenderService.jobs_payload())

    def post(self, request, *args, **kwargs):
        payload = request.data if isinstance(request.data, dict) else {}
        run_id = kwargs.get("run_id")
        if run_id and "resumeRunId" not in payload:
            payload["resumeRunId"] = run_id

        path_text = str(request.path).lower()
        if "command" not in payload:
            if path_text.endswith("/identify"):
                payload["command"] = "script-draft"
                payload["compose"] = False
            elif path_text.endswith("/compose"):
                payload["command"] = "compose"
                payload["compose"] = True
            elif path_text.endswith("/prepare"):
                payload["command"] = "prepare"
            elif path_text.endswith("/end-scene"):
                payload["command"] = "end-scene-rerender"
            elif path_text.endswith("/voiceover/draft"):
                payload["command"] = "voiceover-draft"
            elif path_text.endswith("/voiceover/apply"):
                payload["command"] = "voiceover-apply"

        command = str(payload.get("command", "")).strip().lower()
        # Backward-compatibility guard:
        # if older UI sends "run" for the first step, force script-first behavior.
        if command == "run" and not bool(payload.get("compose", False)):
            command = "script-draft"
            payload["command"] = "script-draft"
            payload["compose"] = False
        if command in {"compose", "voiceover-apply"} and "approvedScript" not in payload:
            script_value = payload.get("script")
            if isinstance(script_value, str):
                payload["approvedScript"] = script_value
        if command == "compose" and not str(payload.get("approvedScript", "")).strip():
            return Response({"error": "Approve/select a script first, then generate full video."}, status=400)
        try:
            job = ReelRenderService.start_job(payload)
        except RuntimeError as exc:
            return Response({"error": str(exc)}, status=409)
        except Exception as exc:  # pragma: no cover - defensive error surface for UI debugging
            message = f"{exc.__class__.__name__}: {exc}"
            body = {"error": message}
            if bool(getattr(settings, "DEBUG", False)):
                body["traceback"] = traceback.format_exc()
            return Response(body, status=500)

        return Response(ReelRenderService._job_to_public(job), status=202)


class JobControlApiView(APIView):
    def post(self, request, job_id):
        payload = request.data if isinstance(request.data, dict) else {}
        action = str(payload.get("action", "")).strip().lower()
        try:
            job = ReelRenderService.control_job(job_id=job_id, action=action)
        except RuntimeError as exc:
            message = str(exc)
            lower = message.lower()
            if "not found" in lower:
                return Response({"error": message}, status=404)
            if "only" in lower or "unsupported" in lower or "stop the running job" in lower:
                return Response({"error": message}, status=409)
            return Response({"error": message}, status=400)
        return Response(job, status=200)


class RunsApiView(APIView):
    def get(self, request):
        return Response(ReelRenderService.runs_payload())


class WorkflowDebugApiView(APIView):
    def get(self, request):
        try:
            return Response(ReelRenderService.workflow_debug_status())
        except Exception as exc:  # pragma: no cover - defensive diagnostics endpoint
            return Response({"error": f"{exc.__class__.__name__}: {exc}"}, status=500)


class RunDeleteApiView(APIView):
    def delete(self, request, run_id):
        deleted = ReelRenderService.delete_run(run_id)
        if not deleted:
            return Response({"error": "Run not found."}, status=404)
        return Response({"ok": True, "runId": run_id})


class RunDetailApiView(APIView):
    def get(self, request, run_id):
        run = get_object_or_404(ReelRun, run_id=run_id)
        report = dict(run.report or {})
        run_dir_path = _resolve_run_dir_path(report.get("runDir")) or (_WORKFLOW_ROOT / "runs" / run_id)
        if str(run.status or "").strip().lower() in {"queued", "running"}:
            try:
                live_report = ReelRenderService._build_run_report_from_outputs(
                    run_dir_path,
                    str(report.get("command", "run")).strip() or "run",
                )
                if isinstance(live_report, dict) and live_report:
                    report = {**report, **live_report}
            except Exception:
                # Keep API resilient while background processing is still writing files.
                pass
        run_dir_value = str(run_dir_path) if run_dir_path else str(report.get("runDir") or "").strip()
        videos = report.get("videos") or []
        decorated_videos = [
            _decorate_media_item(run_id, run_dir_value, item) for item in videos if isinstance(item, dict)
        ]
        plan = report.get("plan") or {}
        sequence = plan.get("sequence") if isinstance(plan, dict) else []
        composition = (plan.get("composition") or {}) if isinstance(plan, dict) else {}
        segments = composition.get("segments") if isinstance(composition, dict) else []
        decorated_sequence = (
            [_decorate_media_item(run_id, run_dir_value, item) for item in sequence] if isinstance(sequence, list) else []
        )
        decorated_segments = (
            [_decorate_media_item(run_id, run_dir_value, item) for item in segments] if isinstance(segments, list) else []
        )

        downloads_manifest = report.get("downloadsManifest") or {}
        price_includes = downloads_manifest.get("priceIncludes")
        listing_title = (
            str(run.listing_title or "").strip()
            or str(report.get("listingTitle") or "").strip()
            or str(downloads_manifest.get("listingTitle") or "").strip()
        )
        listing_price = (
            str(run.listing_price or "").strip()
            or str(report.get("listingPrice") or "").strip()
            or str(downloads_manifest.get("listingPrice") or "").strip()
        )
        final_reel_path = _resolve_report_asset_path(run_dir_value, report.get("finalReelUrl"))
        final_reel_webm_path = _resolve_report_asset_path(run_dir_value, report.get("finalReelWebmUrl"))
        return Response(
            {
                "runId": run.run_id,
                "listingTitle": listing_title,
                "stockId": run.stock_id,
                "carDescription": run.car_description,
                "listingPrice": listing_price,
                "priceIncludes": price_includes or "",
                "pipeline": report.get("pipeline", {}),
                "status": run.status,
                "updatedAt": run.updated_at.isoformat(),
                "createdAt": run.created_at.isoformat(),
                "stats": report.get("stats", {"downloads": 0, "frames": 0, "analyzed": 0, "planned": 0}),
                "voiceoverDraft": report.get("voiceoverDraft", {"variants": []}),
                "voiceoverStatus": report.get("voiceoverStatus", ""),
                "hasVoiceover": bool(report.get("hasVoiceover", False)),
                "finalReelUrl": _asset_url(run_id, final_reel_path) if final_reel_path else "",
                "finalReelWebmUrl": _asset_url(run_id, final_reel_webm_path) if final_reel_webm_path else "",
                "videos": decorated_videos,
                "plan": {
                    **plan,
                    "sequence": decorated_sequence,
                    "composition": {
                        **composition,
                        "segments": decorated_segments,
                    },
                }
                if isinstance(plan, dict)
                else None,
            }
        )

    def delete(self, request, run_id):
        deleted = ReelRenderService.delete_run(run_id)
        if not deleted:
            return Response({"error": "Run not found."}, status=404)
        return Response({"ok": True, "runId": run_id})


class RunThumbnailApiView(APIView):
    def post(self, request, run_id):
        run = get_object_or_404(ReelRun, run_id=run_id)
        report = run.report or {}
        run_dir_value = str(report.get("runDir") or "").strip()
        if not run_dir_value:
            return Response({"error": "Run directory not found."}, status=404)

        title = str((request.data or {}).get("title", "")).strip()
        subtitle = str((request.data or {}).get("subtitle", "")).strip()
        reference_image_data_url = str((request.data or {}).get("referenceImageDataUrl", "")).strip()
        if not title:
            return Response({"error": "title is required."}, status=400)
        if not subtitle:
            return Response({"error": "subtitle is required."}, status=400)
        if not reference_image_data_url:
            return Response({"error": "referenceImageDataUrl is required."}, status=400)

        listing_price = (
            str((request.data or {}).get("price", "")).strip()
            or str(run.listing_price or "").strip()
            or str(report.get("listingPrice") or "").strip()
            or "AU "
        )

        try:
            generated = ReelRenderService.generate_thumbnail(
                run_id=run_id,
                title=title,
                subtitle=subtitle,
                reference_image_data_url=reference_image_data_url,
                price=listing_price,
            )
        except RuntimeError as exc:
            message = str(exc)
            low = message.lower()
            status = 400 if "required" in low or "gemini" in low or "reference image" in low else 500
            return Response({"error": message}, status=status)

        run_dir = Path(run_dir_value).resolve()
        generated_path = Path(str(generated.get("imagePath", "")).strip())
        candidate = generated_path.resolve() if generated_path.is_absolute() else (run_dir / generated_path).resolve()
        try:
            candidate.relative_to(run_dir)
        except ValueError:
            return Response({"error": "Generated thumbnail path is outside run directory."}, status=500)

        return Response(
            {
                "runId": run_id,
                "imageUrl": _asset_url(run_id, str(candidate)),
                "mimeType": str(generated.get("imageMimeType", "")).strip() or "image/png",
            }
        )


class VehicleInventoryStatusApiView(APIView):
    def get(self, request):
        return Response(VehicleInventoryService.status())


class VehicleInventorySearchApiView(APIView):
    def get(self, request):
        q = request.query_params.get("q", "")
        limit = int(request.query_params.get("limit", 20))
        return Response(VehicleInventoryService.search(q, limit=limit))


class VehicleInventoryRefreshApiView(APIView):
    def post(self, request):
        try:
            result = VehicleInventoryService.refresh()
            return Response({"ok": True, "refreshing": False, "kicked": True, **result}, status=202)
        except Exception as exc:  # noqa: BLE001
            return Response({"error": str(exc)}, status=500)


class RunAssetApiView(APIView):
    def get(self, request, run_id):
        run = get_object_or_404(ReelRun, run_id=run_id)
        report = run.report or {}
        run_dir_path = _resolve_run_dir_path(report.get("runDir"))
        if not run_dir_path:
            raise Http404("Run directory not found.")

        requested = str(request.query_params.get("path", "")).strip()
        if not requested:
            raise Http404("Missing file path.")

        run_dir = run_dir_path.resolve()
        candidate = Path(requested).resolve()
        try:
            candidate.relative_to(run_dir)
        except ValueError as exc:
            raise Http404("File is outside run directory.") from exc

        if not candidate.exists() or not candidate.is_file():
            raise Http404("File not found.")

        file_size = candidate.stat().st_size
        content_type, _encoding = mimetypes.guess_type(str(candidate))
        if not content_type:
            content_type = "application/octet-stream"

        range_header = request.headers.get("Range") or request.META.get("HTTP_RANGE", "")
        try:
            byte_range = _parse_single_byte_range(range_header, file_size=file_size)
        except _UnsatisfiableRangeError:
            response = HttpResponse(status=416)
            response["Content-Range"] = f"bytes */{file_size}"
            response["Accept-Ranges"] = "bytes"
            return response

        if byte_range is None:
            response = FileResponse(candidate.open("rb"), as_attachment=False, content_type=content_type)
            response["Accept-Ranges"] = "bytes"
            return response

        start, end = byte_range
        response = StreamingHttpResponse(
            _iter_file_bytes(candidate.open("rb"), start=start, end=end),
            status=206,
            content_type=content_type,
        )
        response["Accept-Ranges"] = "bytes"
        response["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        response["Content-Length"] = str(end - start + 1)
        return response
