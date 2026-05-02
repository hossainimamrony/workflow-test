from __future__ import annotations

import json
from importlib import import_module
from urllib.parse import parse_qs

from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
from django.template.loader import render_to_string
from django.views.decorators.csrf import csrf_exempt


def _legacy_module():
    return import_module("apps.carsale_scraper.web.dashboard.runtime")


def _extract_json_body(request) -> dict:
    if not request.body:
        return {}
    try:
        return json.loads(request.body.decode("utf-8"))
    except Exception:
        return {}


def _args_dict(request) -> dict:
    parsed = parse_qs(request.META.get("QUERY_STRING", ""), keep_blank_values=True)
    return {k: (v[-1] if isinstance(v, list) and v else "") for k, v in parsed.items()}


def _resolve_response(result):
    if isinstance(result, tuple) and len(result) == 2:
        payload, status = result
    else:
        payload, status = result, 200

    if hasattr(payload, "template_name"):
        html = render_to_string(payload.template_name, payload.context)
        return HttpResponse(html, status=int(status), content_type="text/html; charset=utf-8")

    if hasattr(payload, "content_type") and hasattr(payload, "data"):
        data = payload.data
        if hasattr(data, "__iter__") and not isinstance(data, (bytes, str, dict, list)):
            return StreamingHttpResponse(
                data,
                status=int(getattr(payload, "status", status) or status),
                content_type=getattr(payload, "content_type", "text/plain"),
            )
        return HttpResponse(
            data,
            status=int(getattr(payload, "status", status) or status),
            content_type=getattr(payload, "content_type", "text/plain"),
            headers=getattr(payload, "headers", None) or {},
        )

    if isinstance(payload, dict):
        return JsonResponse(payload, status=int(status))

    return HttpResponse(payload, status=int(status))


def _call_legacy(fn_name: str, request):
    legacy = _legacy_module()
    compat = import_module("apps.carsale_scraper.web.compat")
    compat.request.set(args=_args_dict(request), json_body=_extract_json_body(request))
    fn = getattr(legacy, fn_name)
    return _resolve_response(fn())


def dashboard(request):
    return _call_legacy("index", request)


def api_config(request):
    return _call_legacy("api_config", request)


def api_status(request):
    return _call_legacy("api_status", request)


def api_version(request):
    return _call_legacy("api_version", request)


def api_progress_stream(request):
    return _call_legacy("api_progress_stream", request)


def api_files(request):
    return _call_legacy("api_files", request)


def api_identified(request):
    return _call_legacy("api_identified", request)


def api_comparisons(request):
    return _call_legacy("api_comparisons", request)


@csrf_exempt
def api_carbarn_inventory_refresh(request):
    return _call_legacy("api_carbarn_inventory_refresh", request)


def api_not_found(request):
    return _call_legacy("api_not_found", request)


def api_image(request):
    return _call_legacy("api_image", request)


@csrf_exempt
def api_manual_urls(request):
    if request.method.upper() == "POST":
        return _call_legacy("api_manual_urls_save", request)
    return _call_legacy("api_manual_urls_get", request)


@csrf_exempt
def api_run(request):
    return _call_legacy("api_run", request)


@csrf_exempt
def api_stop(request):
    return _call_legacy("api_stop", request)


@csrf_exempt
def api_open_output(request):
    return _call_legacy("api_open_output", request)


@csrf_exempt
def api_session_mode(request):
    return _call_legacy("api_session_mode", request)


@csrf_exempt
def api_open_antibot_url(request):
    return _call_legacy("api_open_antibot_url", request)


@csrf_exempt
def api_manual_refresh_one(request):
    return _call_legacy("api_manual_refresh_one", request)


def favicon(request):
    return _call_legacy("favicon", request)


def control_panel_sw(request):
    return _call_legacy("control_panel_sw", request)
