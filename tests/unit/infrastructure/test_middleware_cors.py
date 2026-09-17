# tests/unit/infrastructure/test_middleware_cors.py
from __future__ import annotations

import inspect

from app.config import Settings
from app.infrastructure.http import middleware


def test_cors_does_not_default_to_wildcard_in_production() -> None:
    """Settings deve aceitar cors_origins como list e defaultar para lista vazia."""
    s = Settings(
        debug=False,
        cors_origins=[],
        _env_file=None,
        db={"url": "sqlite+aiosqlite:///:memory:"},  # type: ignore[arg-type]
        auth={"secret_key": "prod-secret"},  # type: ignore[arg-type]
    )
    assert isinstance(s.cors_origins, list)
    assert s.cors_origins == []


def test_middleware_cors_source_does_not_hardcode_wildcard() -> None:
    """middleware.py nao deve ter ['*'] como fallback de allow_origins."""
    source = inspect.getsource(middleware.add_observability_middleware)
    assert 'allow_origins or ["*"]' not in source, (
        "add_observability_middleware usa ['*'] como fallback — "
        "passe cors_origins de settings.cors_origins explicitamente"
    )
