from __future__ import annotations

import logging
from functools import cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


_DEFAULT_DEV_JWT_PUBLIC_KEY_PEM = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqdez7Ek393tN5sEe+hgo
Jt86QEmGm9V5BiYq7LfzWb0GfPANbtkE4kHloTu0wy14p5KNp9GRcTWUl9v8EHAT
Cgp8Fsav8RROm+98x0JKHbo/mEI9n/vMb2PiKtKBMiIugyihZtu47HfrmAmGrmZ6
/XSim7+67r/i9CoMKLAsaqwrMTYQ2Zf9PP5Um9i13yMmyboelTEAUS6pE9eaQyMm
8Ehgo8uAYCMlBKsIznPgHKGAzL9NdO01jLuCGgr4IlD2Yoc2WbKgFdPdJcorshed
/q7/OKCy/sx8vzRuzKYou7yZ02lD3/WwRVuzC8I8HZZIeKL+PzEiarCE8mPqCRt2
nQIDAQAB
-----END PUBLIC KEY-----"""


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

    @property
    def resolved_dags_path(self) -> Path:
        """Retorna o caminho configurado para gravacao das DAGs (PLATFORM_DAGS_PATH).

        Cria o diretorio se nao existir. Nao possui fallback silencioso — qualquer
        falha de permissao ira propagar o erro para que seja corrigido na configuracao.
        """
        p = Path(self.dags_path)
        p.mkdir(parents=True, exist_ok=True)
        logger.info("DAGs path resolved: %s", p.resolve())
        return p

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
    auth_jwt_public_key_pem_file: str = ""
    auth_jwt_issuer: str = ""
    auth_jwt_audience: str = ""
    jwt_roles_claim: str = "roles"
    permission_cache_ttl_seconds: int = 300
    rate_limit_global: str = "100/minute"
    rate_limit_write: str = "10/minute"

    otel_exporter_otlp_endpoint: str | None = None

    @property
    def resolved_auth_jwt_public_key_pem(self) -> str:
        """Return the JWT RSA public key PEM string.

        Supports direct PEM string, file path in auth_jwt_public_key_pem,
        file path in auth_jwt_public_key_pem_file, standard mounts, or fallback dev key.
        """
        if self.auth_jwt_public_key_pem and "BEGIN PUBLIC KEY" in self.auth_jwt_public_key_pem:
            return self.auth_jwt_public_key_pem

        if self.auth_jwt_public_key_pem:
            p = Path(self.auth_jwt_public_key_pem)
            if p.is_file():
                return p.read_text(encoding="utf-8")

        if self.auth_jwt_public_key_pem_file:
            p = Path(self.auth_jwt_public_key_pem_file)
            if p.is_file():
                return p.read_text(encoding="utf-8")

        for candidate in [Path("/run/secrets/jwt_public.pem"), Path("keys/jwt_public.pem")]:
            if candidate.is_file():
                return candidate.read_text(encoding="utf-8")

        return _DEFAULT_DEV_JWT_PUBLIC_KEY_PEM


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
