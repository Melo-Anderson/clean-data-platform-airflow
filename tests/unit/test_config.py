from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from app.config import AuthSettings, DatabaseSettings, RateLimitSettings, Settings, get_settings


def test_database_settings_has_no_default_url() -> None:
    """DatabaseSettings.url deve ser string vazia por default, nunca uma URL com credenciais."""
    db = DatabaseSettings()
    assert db.url == ""


def test_auth_settings_has_no_hardcoded_secret_key() -> None:
    """AuthSettings.secret_key deve ser string vazia por default."""
    auth = AuthSettings()
    assert auth.secret_key == ""


def test_settings_fails_fast_when_db_url_empty(monkeypatch) -> None:
    """Settings deve falhar imediatamente se db.url estiver vazio."""
    monkeypatch.delenv("PLATFORM_DB__URL", raising=False)
    monkeypatch.delenv("PLATFORM_DATABASE_URL", raising=False)
    monkeypatch.setenv("PLATFORM_AUTH__SECRET_KEY", "test-secret")
    with pytest.raises((ValidationError, ValueError)):
        Settings(_env_file=None)


def test_settings_fails_fast_when_secret_key_empty(monkeypatch) -> None:
    """Settings deve falhar imediatamente se auth.secret_key estiver vazio."""
    monkeypatch.setenv("PLATFORM_DB__URL", "sqlite+aiosqlite:///:memory:")
    monkeypatch.delenv("PLATFORM_AUTH__SECRET_KEY", raising=False)
    monkeypatch.delenv("PLATFORM_SECRET_KEY", raising=False)
    with pytest.raises((ValidationError, ValueError)):
        Settings(_env_file=None)


def test_rate_limit_settings_has_reasonable_defaults() -> None:
    """RateLimitSettings deve ter defaults sem credenciais."""
    rl = RateLimitSettings()
    assert rl.global_ == "100/minute"
    assert rl.write == "10/minute"


def test_settings_db_url_accessible_via_sub_model(monkeypatch) -> None:
    monkeypatch.setenv("PLATFORM_DB__URL", "sqlite+aiosqlite:///:memory:")
    monkeypatch.setenv("PLATFORM_AUTH__SECRET_KEY", "test-secret")
    s = Settings(_env_file=None, debug=True)
    assert s.db.url == "sqlite+aiosqlite:///:memory:"


def test_settings_auth_accessible_via_sub_model(monkeypatch) -> None:
    monkeypatch.setenv("PLATFORM_AUTH__SECRET_KEY", "my-real-secret")
    monkeypatch.setenv("PLATFORM_DB__URL", "sqlite+aiosqlite:///:memory:")
    s = Settings(_env_file=None, debug=True)
    assert s.auth.secret_key == "my-real-secret"


def test_settings_has_no_legacy_database_url_property(monkeypatch) -> None:
    """Settings não deve expor a propriedade legacy 'database_url'."""
    monkeypatch.setenv("PLATFORM_DB__URL", "sqlite+aiosqlite:///:memory:")
    monkeypatch.setenv("PLATFORM_AUTH__SECRET_KEY", "test-secret")
    s = Settings(_env_file=None, debug=True)
    with pytest.raises(AttributeError):
        _ = s.database_url  # type: ignore[attr-defined]


def test_settings_has_gcp_defaults():
    get_settings.cache_clear()
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("PLATFORM_DWH__GCP_PROJECT", None)
        os.environ.pop("PLATFORM_DWH__PROVISIONER_ADAPTER", None)
        settings = get_settings()
        assert hasattr(settings.dwh, "gcp_project")
        assert hasattr(settings.dwh, "provisioner_adapter")
    get_settings.cache_clear()


def test_settings_reads_gcp_env_vars():
    get_settings.cache_clear()
    with patch.dict(
        os.environ,
        {
            "PLATFORM_DWH__GCP_PROJECT": "test-gcp-project",
            "PLATFORM_DWH__PROVISIONER_ADAPTER": "bigquery",
        },
    ):
        settings = get_settings()
        assert settings.dwh.gcp_project == "test-gcp-project"
        assert settings.dwh.provisioner_adapter == "bigquery"
    get_settings.cache_clear()


def test_compute_settings_has_generic_transformation_staging_bucket() -> None:
    settings = Settings()
    assert (
        settings.compute.transformation_staging_bucket == "/opt/airflow/logs/transformation_outputs"
    )
    assert settings.compute.dbt_staging_bucket == "/opt/airflow/logs/dbt_outputs"


def test_dataform_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """DataformSettings deve ter defaults corretos sem credenciais embutidas."""
    monkeypatch.setenv("PLATFORM_DB__URL", "sqlite+aiosqlite:///:memory:")
    monkeypatch.setenv("PLATFORM_AUTH__SECRET_KEY", "test-secret")
    settings = Settings(_env_file=None, debug=True)
    assert settings.dataform.project_dir == "/opt/airflow/dataform_project"
    assert settings.dataform.output_base_dir == "/opt/airflow/logs/dataform_outputs"
    assert (
        settings.dataform.compilation_result_path
        == "/opt/airflow/dataform_project/compilation_result.json"
    )
    assert settings.dataform.default_schema == "dataform"
