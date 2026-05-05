import json
import shutil
import uuid
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlparse
from unittest.mock import Mock, patch

from django.test import TestCase, override_settings

from .models import ReelRenderJob, ReelRun
from .services import ReelRenderService


class BridgePayloadTests(TestCase):
    def _fake_popen(self, captured_payload: dict, *, lines=None, returncode=0):
        stream_lines = lines or ['[RESULT] {"runDir":"C:/tmp/fake-run","report":{}}\n']

        def _build(cmd, **kwargs):
            payload_arg_index = cmd.index("--payload") + 1
            payload_path = Path(cmd[payload_arg_index])
            captured_payload.update(json.loads(payload_path.read_text(encoding="utf-8")))

            proc = Mock()
            proc.stdout = iter(stream_lines)
            proc.returncode = returncode
            proc.wait = Mock(return_value=0)
            return proc

        return _build

    @patch("apps.real_footage_reels.services.subprocess.Popen")
    def test_fresh_run_uses_out_dir_instead_of_resume_id(self, popen_mock):
        captured = {}
        popen_mock.side_effect = self._fake_popen(captured)

        job = ReelRenderJob.objects.create(
            job_id="job-fresh",
            command="run",
            status="queued",
            payload={"url": "https://photos.app.goo.gl/example"},
        )

        run_dir = Path("C:/tmp/test-real-footage-runs/2026-05-02T03-51-32")
        ReelRenderService._run_bridge_workflow(job, command="run", run_dir=run_dir, run_id="2026-05-02T03-51-32")

        self.assertNotIn("resumeRunId", captured)
        self.assertEqual(captured.get("outDir"), str(run_dir))

    @patch("apps.real_footage_reels.services.subprocess.Popen")
    def test_resume_run_keeps_resume_id_and_skips_out_dir(self, popen_mock):
        captured = {}
        popen_mock.side_effect = self._fake_popen(captured)

        job = ReelRenderJob.objects.create(
            job_id="job-resume",
            command="compose",
            status="queued",
            payload={
                "url": "https://photos.app.goo.gl/example",
                "resumeRunId": "2026-05-02T03-51-32",
            },
        )

        run_dir = Path("C:/tmp/test-real-footage-runs/2026-05-02T03-51-32")
        ReelRenderService._run_bridge_workflow(job, command="compose", run_dir=run_dir, run_id="2026-05-02T03-51-32")

        self.assertEqual(captured.get("resumeRunId"), "2026-05-02T03-51-32")
        self.assertNotIn("outDir", captured)

    @patch("apps.real_footage_reels.services.subprocess.Popen")
    def test_bridge_error_message_is_exposed_to_job_error(self, popen_mock):
        captured = {}
        popen_mock.side_effect = self._fake_popen(
            captured,
            lines=[
                "[LOG] starting\n",
                "[ERROR] Error: Could not resume this run because downloads-manifest.json was not found.\n",
            ],
            returncode=1,
        )

        job = ReelRenderJob.objects.create(
            job_id="job-error",
            command="run",
            status="queued",
            payload={"url": "https://photos.app.goo.gl/example"},
        )

        run_dir = Path("C:/tmp/test-real-footage-runs/2026-05-02T03-51-32")
        with self.assertRaisesRegex(RuntimeError, "downloads-manifest.json was not found"):
            ReelRenderService._run_bridge_workflow(job, command="run", run_dir=run_dir, run_id="2026-05-02T03-51-32")


class JobsApiPayloadTests(TestCase):
    def test_compose_endpoint_maps_script_to_approved_script(self):
        job = ReelRenderJob.objects.create(
            job_id="job-compose",
            command="compose",
            status="queued",
            payload={},
        )

        with patch.object(ReelRenderService, "start_job", return_value=job) as start_job_mock:
            response = self.client.post(
                "/workflows/real-footage-reels/api/runs/run-123/compose",
                data=json.dumps({"script": "Approved script body"}),
                content_type="application/json",
            )

        self.assertEqual(response.status_code, 202)
        sent_payload = start_job_mock.call_args.args[0]
        self.assertEqual(sent_payload.get("command"), "compose")
        self.assertEqual(sent_payload.get("resumeRunId"), "run-123")
        self.assertEqual(sent_payload.get("approvedScript"), "Approved script body")

    def test_prepare_analysis_defaults_auto_compose_followup(self):
        job = ReelRenderJob.objects.create(
            job_id="job-prepare-auto-compose",
            command="run",
            status="queued",
            payload={},
        )

        with patch.object(ReelRenderService, "start_job", return_value=job) as start_job_mock:
            response = self.client.post(
                "/workflows/real-footage-reels/api/runs/run-456/prepare-analysis",
                data=json.dumps({"script": "Approved script body"}),
                content_type="application/json",
            )

        self.assertEqual(response.status_code, 202)
        sent_payload = start_job_mock.call_args.args[0]
        self.assertEqual(sent_payload.get("command"), "run")
        self.assertEqual(sent_payload.get("resumeRunId"), "run-456")
        self.assertTrue(sent_payload.get("prepareAnalysis"))
        self.assertTrue(sent_payload.get("autoComposeAfterPrepare"))


class RunDeletionCleanupTests(TestCase):
    def test_delete_run_removes_run_directory(self):
        run_id = f"delete-run-{uuid.uuid4().hex[:8]}"
        run_dir = (
            Path(__file__).resolve().parent
            / "workflow_engine"
            / "runs"
            / run_id
        )
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "final-reel.mp4").write_bytes(b"video")
        self.assertTrue(run_dir.exists())

        ReelRun.objects.create(
            run_id=run_id,
            listing_title="Delete test",
            stock_id="DEL1",
            car_description="cleanup",
            listing_price="AU$1",
            status="completed",
            report={"runDir": str(run_dir)},
        )

        deleted = ReelRenderService.delete_run(run_id)
        self.assertTrue(deleted)
        self.assertFalse(run_dir.exists())


@override_settings(ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1"])
class RunDetailAssetResolutionTests(TestCase):
    def test_legacy_api_file_urls_are_resolved_to_django_asset_endpoint(self):
        run_id = "test-run-legacy"
        run_dir = (
            Path(__file__).resolve().parent
            / "workflow_engine"
            / "runs"
            / run_id
        )
        report = {
            "runDir": str(run_dir),
            "pipeline": {"download": {"done": True}, "frames": {"done": True}, "analyze": {"done": True}, "render": {"done": True}},
            "stats": {"downloads": 1, "frames": 1, "analyzed": 1, "planned": 1},
            "finalReelUrl": f"/api/file?path=runs%2F{run_id}%2Ffinal-reel.mp4",
            "finalReelWebmUrl": f"/api/file?path=runs%2F{run_id}%2Ffinal-reel.webm",
            "videos": [],
            "plan": {"sequence": [], "composition": {"segments": []}},
        }
        ReelRun.objects.create(
            run_id=run_id,
            listing_title="Test listing",
            stock_id="1001",
            car_description="Test",
            listing_price="AU$1",
            status="completed",
            report=report,
        )

        response = self.client.get(f"/workflows/real-footage-reels/api/runs/{run_id}")
        self.assertEqual(response.status_code, 200)

        data = response.json()
        final_reel_url = str(data.get("finalReelUrl") or "")
        self.assertIn(f"/workflows/real-footage-reels/api/runs/{run_id}/asset?path=", final_reel_url)

        parsed = urlparse(final_reel_url)
        encoded_path = parse_qs(parsed.query).get("path", [""])[0]
        decoded_path = unquote(encoded_path).replace("\\", "/")
        self.assertTrue(decoded_path.endswith(f"/runs/{run_id}/final-reel.mp4"))

    def test_run_detail_falls_back_to_report_listing_title(self):
        run_id = "test-run-title-fallback"
        run_dir = (
            Path(__file__).resolve().parent
            / "workflow_engine"
            / "runs"
            / run_id
        )
        report = {
            "runDir": str(run_dir),
            "listingTitle": "Title From Report",
            "downloadsManifest": {"listingTitle": "Title From Manifest"},
            "pipeline": {"download": {"done": True}, "frames": {"done": True}, "analyze": {"done": True}, "render": {"done": True}},
            "stats": {"downloads": 1, "frames": 1, "analyzed": 1, "planned": 1},
            "videos": [],
            "plan": {"sequence": [], "composition": {"segments": []}},
        }
        ReelRun.objects.create(
            run_id=run_id,
            listing_title="",
            stock_id="1003",
            car_description="Test",
            listing_price="AU$1",
            status="completed",
            report=report,
        )

        response = self.client.get(f"/workflows/real-footage-reels/api/runs/{run_id}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json().get("listingTitle"), "Title From Report")

    def test_run_detail_keeps_remote_preview_and_final_urls(self):
        run_id = "test-run-remote-preview"
        run_dir = Path(__file__).resolve().parent / "workflow_engine" / "runs" / run_id
        report = {
            "runDir": str(run_dir),
            "pipeline": {"download": {"done": True}, "frames": {"done": True}, "analyze": {"done": True}, "render": {"done": True}},
            "stats": {"downloads": 1, "frames": 1, "analyzed": 1, "planned": 1},
            "finalReelUrl": "https://cdn.example.com/reels/test-run-remote-preview-final-reel.mp4",
            "finalReelPreviewUrl": "https://cdn.example.com/reels/test-run-remote-preview-final-reel-preview.mp4",
            "finalReelRemoteUploadOk": True,
            "videos": [],
            "plan": {"sequence": [], "composition": {"segments": []}},
        }
        ReelRun.objects.create(
            run_id=run_id,
            listing_title="Remote listing",
            stock_id="1004",
            car_description="Test",
            listing_price="AU$1",
            status="completed",
            report=report,
        )

        response = self.client.get(f"/workflows/real-footage-reels/api/runs/{run_id}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(
            data.get("finalReelUrl"),
            "https://cdn.example.com/reels/test-run-remote-preview-final-reel.mp4",
        )
        self.assertEqual(
            data.get("finalReelPreviewUrl"),
            "https://cdn.example.com/reels/test-run-remote-preview-final-reel-preview.mp4",
        )

    def test_run_detail_resolves_legacy_run_dir_to_workflow_engine(self):
        run_id = "test-run-legacy-path"
        app_dir = Path(__file__).resolve().parent
        legacy_root = app_dir / "Carbarn-Au-real-footage-reels"
        new_root = app_dir / "workflow_engine"
        relative_run = Path("runs") / run_id
        new_run_dir = new_root / relative_run
        new_run_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(new_run_dir, ignore_errors=True))
        (new_run_dir / "final-reel.mp4").write_bytes(b"mp4")
        legacy_run_dir = legacy_root / relative_run

        report = {
            "runDir": str(legacy_run_dir),
            "finalReelUrl": f"/api/file?path=runs%2F{run_id}%2Ffinal-reel.mp4",
            "pipeline": {"download": {"done": True}, "frames": {"done": True}, "analyze": {"done": True}, "render": {"done": True}},
            "stats": {"downloads": 1, "frames": 1, "analyzed": 1, "planned": 1},
            "videos": [],
            "plan": {"sequence": [], "composition": {"segments": []}},
        }
        ReelRun.objects.create(
            run_id=run_id,
            listing_title="Legacy path run",
            stock_id="1010",
            car_description="Test",
            listing_price="AU$1",
            status="completed",
            report=report,
        )

        response = self.client.get(f"/workflows/real-footage-reels/api/runs/{run_id}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn(f"/workflows/real-footage-reels/api/runs/{run_id}/asset?path=", data.get("finalReelUrl", ""))

    def test_run_detail_prefers_existing_legacy_file_for_runs_relative_asset_path(self):
        run_id = "test-run-legacy-final"
        app_dir = Path(__file__).resolve().parent
        legacy_root = app_dir / "Carbarn-Au-real-footage-reels"
        relative_run = Path("runs") / run_id
        legacy_run_dir = legacy_root / relative_run
        legacy_run_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(legacy_run_dir, ignore_errors=True))
        (legacy_run_dir / "final-reel.mp4").write_bytes(b"legacy-mp4")

        report = {
            "runDir": str(legacy_run_dir),
            "finalReelUrl": f"/api/file?path=runs%2F{run_id}%2Ffinal-reel.mp4",
            "pipeline": {"download": {"done": True}, "frames": {"done": True}, "analyze": {"done": True}, "render": {"done": True}},
            "stats": {"downloads": 1, "frames": 1, "analyzed": 1, "planned": 1},
            "videos": [],
            "plan": {"sequence": [], "composition": {"segments": []}},
        }
        ReelRun.objects.create(
            run_id=run_id,
            listing_title="Legacy final reel",
            stock_id="1011",
            car_description="Test",
            listing_price="AU$1",
            status="completed",
            report=report,
        )

        response = self.client.get(f"/workflows/real-footage-reels/api/runs/{run_id}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        parsed = urlparse(str(data.get("finalReelUrl") or ""))
        encoded_path = parse_qs(parsed.query).get("path", [""])[0]
        decoded_path = unquote(encoded_path).replace("\\", "/")
        self.assertIn("Carbarn-Au-real-footage-reels/runs", decoded_path)


@override_settings(ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1"])
class RunAssetRangeTests(TestCase):
    def _make_run_with_file(self, run_id: str, file_name: str, data: bytes) -> tuple[ReelRun, Path]:
        base_runs_dir = Path(__file__).resolve().parent / "workflow_engine" / "runs"
        base_runs_dir.mkdir(parents=True, exist_ok=True)
        run_dir = base_runs_dir / f"test-range-{uuid.uuid4().hex[:10]}"
        run_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(run_dir, ignore_errors=True))
        file_path = run_dir / file_name
        file_path.write_bytes(data)
        run = ReelRun.objects.create(
            run_id=run_id,
            listing_title="Range test",
            stock_id="2001",
            car_description="Range",
            listing_price="AU$2",
            status="completed",
            report={"runDir": str(run_dir)},
        )
        return run, file_path

    def test_asset_returns_partial_content_for_range_requests(self):
        run_id = "range-run-206"
        payload = bytes(range(100))
        run, file_path = self._make_run_with_file(run_id, "video.mp4", payload)
        encoded = quote(str(file_path), safe="")

        response = self.client.get(
            f"/workflows/real-footage-reels/api/runs/{run.run_id}/asset?path={encoded}",
            HTTP_RANGE="bytes=10-19",
        )

        self.assertEqual(response.status_code, 206)
        self.assertEqual(response["Accept-Ranges"], "bytes")
        self.assertEqual(response["Content-Range"], "bytes 10-19/100")
        self.assertEqual(response["Content-Length"], "10")
        self.assertEqual(b"".join(response.streaming_content), payload[10:20])

    def test_asset_returns_416_for_unsatisfiable_range(self):
        run_id = "range-run-416"
        payload = bytes(range(30))
        run, file_path = self._make_run_with_file(run_id, "video.mp4", payload)
        encoded = quote(str(file_path), safe="")

        response = self.client.get(
            f"/workflows/real-footage-reels/api/runs/{run.run_id}/asset?path={encoded}",
            HTTP_RANGE="bytes=200-300",
        )

        self.assertEqual(response.status_code, 416)
        self.assertEqual(response["Accept-Ranges"], "bytes")
        self.assertEqual(response["Content-Range"], "bytes */30")


@override_settings(ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1"])
class RunThumbnailApiTests(TestCase):
    def test_thumbnail_endpoint_returns_asset_url(self):
        run_id = "thumb-run-200"
        base_runs_dir = Path(__file__).resolve().parent / "workflow_engine" / "runs"
        run_dir = base_runs_dir / f"test-thumb-{uuid.uuid4().hex[:10]}"
        run_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(run_dir, ignore_errors=True))
        output_image = run_dir / "thumbnails" / "thumbnail-1.png"
        output_image.parent.mkdir(parents=True, exist_ok=True)
        output_image.write_bytes(b"png")

        ReelRun.objects.create(
            run_id=run_id,
            listing_title="Thumb listing",
            stock_id="9001",
            car_description="Thumb",
            listing_price="AU$100",
            status="completed",
            report={"runDir": str(run_dir)},
        )

        with patch.object(
            ReelRenderService,
            "generate_thumbnail",
            return_value={
                "runDir": str(run_dir),
                "imagePath": str(output_image),
                "imageMimeType": "image/png",
            },
        ) as gen_mock:
            response = self.client.post(
                f"/workflows/real-footage-reels/api/runs/{run_id}/thumbnail",
                data=json.dumps(
                    {
                        "title": "A title",
                        "subtitle": "A subtitle",
                        "price": "AU$100",
                        "referenceImageDataUrl": "data:image/png;base64,AAAA",
                    }
                ),
                content_type="application/json",
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload.get("runId"), run_id)
        self.assertEqual(payload.get("mimeType"), "image/png")
        self.assertIn(f"/workflows/real-footage-reels/api/runs/{run_id}/asset?path=", payload.get("imageUrl", ""))

        kwargs = gen_mock.call_args.kwargs
        self.assertEqual(kwargs.get("run_id"), run_id)
        self.assertEqual(kwargs.get("title"), "A title")
        self.assertEqual(kwargs.get("subtitle"), "A subtitle")

    def test_thumbnail_endpoint_validates_required_fields(self):
        run_id = "thumb-run-400"
        base_runs_dir = Path(__file__).resolve().parent / "workflow_engine" / "runs"
        run_dir = base_runs_dir / f"test-thumb-{uuid.uuid4().hex[:10]}"
        run_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(run_dir, ignore_errors=True))

        ReelRun.objects.create(
            run_id=run_id,
            listing_title="Thumb listing",
            stock_id="9002",
            car_description="Thumb",
            listing_price="AU$100",
            status="completed",
            report={"runDir": str(run_dir)},
        )

        response = self.client.post(
            f"/workflows/real-footage-reels/api/runs/{run_id}/thumbnail",
            data=json.dumps({"title": "A title", "subtitle": ""}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("subtitle is required", response.json().get("error", ""))

