# tests/unit/infrastructure/test_audit_helper.py
from __future__ import annotations


def test_audit_helper_exports_system_actor_constants() -> None:
    """audit_helper deve exportar SYSTEM_ACTOR_ID e SYSTEM_ACTOR_EMAIL como constantes."""
    from app.infrastructure.http.audit_helper import SYSTEM_ACTOR_EMAIL, SYSTEM_ACTOR_ID

    assert isinstance(SYSTEM_ACTOR_ID, str) and SYSTEM_ACTOR_ID != ""
    assert isinstance(SYSTEM_ACTOR_EMAIL, str) and "@" in SYSTEM_ACTOR_EMAIL


def test_system_actor_email_is_not_hardcoded_in_routers() -> None:
    """Nenhum router deve hardcodar a string literal 'worker@airflow.apache.org'."""
    import pathlib

    routers_dir = pathlib.Path("app/infrastructure/http/routers")
    violations = []
    for p in routers_dir.glob("*.py"):
        content = p.read_text(encoding="utf-8")
        if "worker@airflow.apache.org" in content:
            violations.append(str(p))
    assert violations == [], (
        f"Hardcoded actor email found in: {violations}. Use SYSTEM_ACTOR_EMAIL from audit_helper."
    )
