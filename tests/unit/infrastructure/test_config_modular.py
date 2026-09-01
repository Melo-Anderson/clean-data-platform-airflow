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


def test_settings_backward_compatibility_attributes() -> None:
    settings = Settings()
    # Backward compatibility aliases
    assert settings.database_url == settings.db.url
    assert settings.secret_key == settings.auth.secret_key
    assert settings.gcp_project == settings.dwh.gcp_project
    assert settings.duckdb_output_dir == settings.compute.duckdb_output_dir  # both str
    assert settings.dags_path == settings.airflow.dags_path  # both str
    assert settings.airflow_url == settings.airflow.url


def test_get_settings_cached_singleton() -> None:
    s1 = get_settings()
    s2 = get_settings()
    assert s1 is s2
