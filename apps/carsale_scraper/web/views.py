from __future__ import annotations

import json
from pathlib import Path

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods


DATA_FILE = Path(__file__).resolve().parents[1] / "full_comparisons.json"
MY_CAR_LIST_FILE = Path(__file__).resolve().parents[1] / "my_car_list.json"


def _read_comparisons() -> dict:
    if not DATA_FILE.exists():
        return {"updated_at": None, "rows_count": 0, "rows": []}

    with DATA_FILE.open("r", encoding="utf-8") as fp:
        payload = json.load(fp)

    rows = payload.get("rows") if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        rows = []

    return {
        "updated_at": payload.get("updated_at") if isinstance(payload, dict) else None,
        "rows_count": len(rows),
        "rows": rows,
    }


def dashboard(request):
    return render(request, "find_my_cars_dashboard.html")


def api_comparisons(request):
    try:
        data = _read_comparisons()
    except (OSError, json.JSONDecodeError) as exc:
        return JsonResponse({"ok": False, "error": str(exc), "rows": [], "rows_count": 0}, status=500)

    return JsonResponse({"ok": True, **data})


def _read_my_car_list_urls() -> list[str]:
    if not MY_CAR_LIST_FILE.exists():
        return []

    with MY_CAR_LIST_FILE.open("r", encoding="utf-8") as fp:
        raw = fp.read().strip()

    if not raw:
        return []

    payload = json.loads(raw)
    if isinstance(payload, list):
        return [str(x).strip() for x in payload if str(x).strip()]
    if isinstance(payload, dict) and isinstance(payload.get("urls"), list):
        return [str(x).strip() for x in payload["urls"] if str(x).strip()]
    return []


def _write_my_car_list_urls(urls: list[str]) -> None:
    MY_CAR_LIST_FILE.write_text(json.dumps(urls, indent=2), encoding="utf-8")


@csrf_exempt
@require_http_methods(["POST"])
def api_my_car_list_add(request):
    try:
        body = json.loads(request.body.decode("utf-8")) if request.body else {}
    except (UnicodeDecodeError, json.JSONDecodeError):
        return JsonResponse({"ok": False, "error": "Invalid JSON body."}, status=400)

    url = str(body.get("url", "")).strip()
    if not url:
        return JsonResponse({"ok": False, "error": "URL is required."}, status=400)
    if not (url.startswith("http://") or url.startswith("https://")):
        return JsonResponse({"ok": False, "error": "Only http/https URLs are allowed."}, status=400)

    try:
        urls = _read_my_car_list_urls()
        if url not in urls:
            urls.append(url)
            _write_my_car_list_urls(urls)
    except (OSError, json.JSONDecodeError) as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=500)

    return JsonResponse({"ok": True, "saved_url": url})


def favicon(request):
    return HttpResponse(status=204)


def control_panel_sw(request):
    return HttpResponse("self.addEventListener('fetch', () => {});", content_type="application/javascript")
