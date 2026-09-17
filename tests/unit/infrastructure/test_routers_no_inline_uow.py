# tests/unit/infrastructure/test_routers_no_inline_uow.py
from __future__ import annotations

import pathlib


def test_no_router_instantiates_sqluow_inline() -> None:
    """Routers nao devem instanciar SqlUnitOfWork diretamente — usar Depends(get_uow)."""
    routers_dir = pathlib.Path("app/infrastructure/http/routers")
    violations = []
    for p in routers_dir.glob("*.py"):
        content = p.read_text(encoding="utf-8")
        if "SqlUnitOfWork(get_session_factory())" in content:
            violations.append(str(p))
    assert violations == [], (
        f"Inline SqlUnitOfWork found in: {violations}. Inject via Depends(get_uow) instead."
    )
