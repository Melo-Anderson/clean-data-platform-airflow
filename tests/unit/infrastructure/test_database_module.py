# tests/unit/infrastructure/test_database_module.py
from __future__ import annotations

import app.infrastructure.persistence.database as db_module


def test_database_module_has_no_magic_getattr() -> None:
    """database.py não deve usar __getattr__ como workaround de compatibilidade."""
    assert not hasattr(db_module, "__getattr__"), (
        "database.py define __getattr__ — remova-o e atualize os callers para get_engine()"
    )


def test_get_engine_is_callable() -> None:
    assert callable(db_module.get_engine)


def test_get_session_factory_is_callable() -> None:
    assert callable(db_module.get_session_factory)
