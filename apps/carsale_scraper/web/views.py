from __future__ import annotations

import json
from pathlib import Path

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render


DATA_FILE = Path(__file__).resolve().parents[1] / "full_comparisons.json"


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


def favicon(request):
    return HttpResponse(status=204)


def control_panel_sw(request):
    return HttpResponse("self.addEventListener('fetch', () => {});", content_type="application/javascript")
