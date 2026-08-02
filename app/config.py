from __future__ import annotations

from functools import cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Platform configuration loaded from environment variables.

    All fields are injected via env vars prefixed with PLATFORM_.
    Suitable for Kubernetes ConfigMap / Secret injection or environment variables.
    """

    model_config = SettingsConfigDict(env_file=".env", env_prefix="PLATFORM_", extra="ignore")

    database_url: str = "postgresql+asyncpg://airflow:airflow@postgres:5432/platform_db"
    secret_key: str = "test_secret_key"
    algorithm: str = "HS256"
    debug: bool = False

    catalog_adapter: str = "noop"  # "noop" | "database" | "datahub" | "openmetadata"
    notification_adapter: str = "noop"  # "noop" | "slack"
    secret_manager_adapter: str = "noop"  # "noop" | "openbao"

    # GCP & BigQuery DWH Provisioner settings
    gcp_project: str = ""
    dwh_provisioner_adapter: str = "noop"  # "noop" | "bigquery"
    google_application_credentials: str = ""

    # Compute engine & DAG paths
    duckdb_output_dir: str = "/tmp/duckdb_outputs"
    rest_api_output_dir: str = "/tmp/airflow_data"
    dags_path: str = "/opt/airflow/dags"

    # Pipeline & extraction defaults
    default_load_strategy: str = "full_load"
    default_page_size: int = 1000
    default_compression: str = "snappy"
    default_encoding: str = "utf-8"
    default_postgres_credential_ref: str = "secret/postgres"

    # Compute defaults
    default_compute_engine: str = "duckdb"
    default_compute_staging_bucket: str = ""
    default_compute_num_workers: int = 1
    default_compute_machine_type: str = "n1-standard-2"

    # Airflow defaults
    default_airflow_retries: int = 3
    default_airflow_retry_delay_minutes: int = 5
    default_airflow_execution_timeout_minutes: int = 120
    default_airflow_sla_minutes: int = 90
    default_airflow_pool: str = "default_pool"

    # DataHub settings (used only when catalog_adapter = "datahub")
    datahub_url: str = ""
    datahub_token: str = ""

    # OpenMetadata settings (used only when catalog_adapter = "openmetadata")
    openmetadata_url: str = ""
    openmetadata_api_key: str = ""

    vault_url: str = ""
    vault_token: str = ""

    airflow_url: str = "http://airflow-webserver:8080"
    airflow_username: str = "admin"
    airflow_password: str = "admin"

    auth_jwt_public_key_pem: str = ""
    auth_jwt_issuer: str = ""
    auth_jwt_audience: str = ""
    jwt_roles_claim: str = "roles"
    permission_cache_ttl_seconds: int = 300
    rate_limit_global: str = "100/minute"
    rate_limit_write: str = "10/minute"


@cache
def get_settings() -> Settings:
    """
    Return the cached Settings singleton.

    Using @cache (not @lru_cache) avoids recreating Settings on every call.
    Safe for use as a FastAPI dependency.

    Example:
        settings = get_settings()
        print(settings.catalog_adapter)  # "noop"
    """
    return Settings()
