from __future__ import annotations

from unittest.mock import MagicMock

from app.infrastructure.http.middleware import _extract_matched_route


def _make_request(path: str, route_path: str | None = None) -> MagicMock:
    """Build a minimal mock of a Starlette Request with optional matched route."""
    request = MagicMock()
    request.url.path = path
    if route_path is not None:
        route = MagicMock()
        route.path = route_path
        request.scope = {"route": route}
    else:
        request.scope = {}
    return request


def test_extract_matched_route_returns_template_when_route_matched() -> None:
    request = _make_request(
        path="/api/v1/pipelines/abc-123/run",
        route_path="/api/v1/pipelines/{pipeline_id}/run",
    )
    result = _extract_matched_route(request)
    assert result == "/api/v1/pipelines/{pipeline_id}/run"


def test_extract_matched_route_falls_back_to_raw_path_when_no_route() -> None:
    request = _make_request(path="/unknown/path")
    result = _extract_matched_route(request)
    assert result == "/unknown/path"


def test_extract_matched_route_falls_back_when_route_has_no_path_attr() -> None:
    request = MagicMock()
    request.url.path = "/some/path"
    request.scope = {"route": object()}  # route without .path attribute
    result = _extract_matched_route(request)
    assert result == "/some/path"
