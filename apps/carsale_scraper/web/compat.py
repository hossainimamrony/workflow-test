from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class CompatResponse:
    def __init__(
        self,
        data: Any,
        status: int = 200,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        mimetype: str | None = None,
        **kwargs,
    ):
        self.data = data
        self.status = status
        self.headers = headers
        # Legacy handlers may pass `mimetype`; normalize to content_type.
        self.content_type = content_type or mimetype or "text/plain"


@dataclass
class TemplateRender:
    template_name: str
    context: dict[str, Any]


class JsonPayload(dict):
    pass


class _RequestCompat:
    def __init__(self):
        self.args: dict[str, Any] = {}
        self._json: dict[str, Any] = {}

    def set(self, *, args: dict[str, Any], json_body: dict[str, Any]) -> None:
        self.args = args
        self._json = json_body

    def get_json(self, silent: bool = True):
        return self._json


request = _RequestCompat()


def jsonify(payload: Any):
    if isinstance(payload, dict):
        return JsonPayload(payload)
    return payload


def render_template(template_name: str, **context):
    return TemplateRender(template_name=template_name, context=context)


Response = CompatResponse
