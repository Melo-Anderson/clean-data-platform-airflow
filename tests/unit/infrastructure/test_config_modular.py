from app.config import (
    AirflowSettings,
    AuthSettings,
    ComputeSettings,
    DatabaseSettings,
    DwhSettings,
    ObservabilitySettings,
    Settings,
    get_settings,
)


def test_modular_settings_composition() -> None:
    settings = Settings()
    assert isinstance(settings.db, DatabaseSettings)
    assert isinstance(settings.auth, AuthSettings)
    assert isinstance(settings.compute, ComputeSettings)
    assert isinstance(settings.dwh, DwhSettings)
    assert isinstance(settings.airflow, AirflowSettings)
    assert isinstance(settings.observability, ObservabilitySettings)


def test_settings_legacy_attributes_removed() -> None:
    settings = Settings()
    for attr in [
        "database_url",
        "secret_key",
        "gcp_project",
        "duckdb_output_dir",
        "dags_path",
        "airflow_url",
    ]:
        assert not hasattr(settings, attr), f"Legacy attribute {attr} should not exist on Settings"


def test_get_settings_cached_singleton() -> None:
    s1 = get_settings()
    s2 = get_settings()
    assert s1 is s2
