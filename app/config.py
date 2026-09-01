from __future__ import annotations

import logging
import os
from functools import cache
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class DatabaseSettings(BaseModel):
    url: str = "postgresql+asyncpg://airflow:airflow@postgres:5432/platform_db"
    pool_size: int = 20
    max_overflow: int = 10


class AuthSettings(BaseModel):
    secret_key: str = "test_secret_key"
    algorithm: str = "HS256"
    jwt_public_key_pem: str = ""
    jwt_public_key_pem_file: str = ""
    jwt_issuer: str = ""
    jwt_audience: str = ""
    jwt_roles_claim: str = "roles"
    permission_cache_ttl_seconds: int = 300
    rate_limit_global: str = "100/minute"
    rate_limit_write: str = "10/minute"


class ComputeSettings(BaseModel):
    duckdb_output_dir: str = "/tmp/duckdb_outputs"
    rest_api_output_dir: str = "/tmp/airflow_data"
    omnibeam_output_dir: str = "/tmp/omnibeam_outputs"
    omnibeam_docker_image: str = "omnibeam-pipeline:latest"
    omnibeam_binary_path: str = "pipeline"
    dbt_staging_bucket: str = "/opt/airflow/logs/dbt_outputs"
    default_engine: str = "duckdb"
    default_staging_bucket: str = ""
    default_num_workers: int = 1
    default_machine_type: str = "n1-standard-2"


class DbtSettings(BaseModel):
    project_dir: str = "/opt/airflow/dbt_project"
    profiles_dir: str = "/opt/airflow/dbt_project"
    output_base_dir: str = "/opt/airflow/logs/dbt_outputs"
    manifest_path: str = "/opt/airflow/dbt_project/target/manifest.json"


class DwhSettings(BaseModel):
    gcp_project: str = ""
    provisioner_adapter: str = "noop"
    google_application_credentials: str = ""
    google_application_credentials_host: str = ""
    cache_ttl_seconds: int = 300

    @property
    def resolved_credentials_path(self) -> str | None:
        """Resolve o caminho absoluto válido para credenciais GCP.

        Encapsulado aqui (Settings) como Single Source of Truth — §3.3 clean-code.md.
        """
        for candidate in [
            self.google_application_credentials,
            self.google_application_credentials_host,
            os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
            os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_HOST"),
        ]:
            if candidate and Path(candidate).is_file() and Path(candidate).stat().st_size > 0:
                return str(Path(candidate).resolve())
        return None


class AirflowSettings(BaseModel):
    url: str = "http://airflow-webserver:8080"
    username: str = "admin"
    password: str = "admin"
    dags_path: str = "/opt/airflow/dags"
    default_retries: int = 3
    default_retry_delay_minutes: int = 5
    default_execution_timeout_minutes: int = 120
    default_sla_minutes: int = 90
    default_pool: str = "default_pool"


class ObservabilitySettings(BaseModel):
    otlp_endpoint: str | None = None
    notification_adapter: str = "noop"
    catalog_adapter: str = "noop"
    datahub_url: str = ""
    datahub_token: str = ""
    openmetadata_url: str = ""
    openmetadata_api_key: str = ""


class Settings(BaseSettings):
    """Platform configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="PLATFORM_",
        env_nested_delimiter="__",
        extra="allow",
    )

    debug: bool = False
    is_container_env: bool = False
    secret_manager_adapter: str = "noop"
    vault_url: str = ""
    vault_token: str = ""
    platform_api_url: str = "http://platform-api:8000"

    default_load_strategy: str = "full_load"
    default_page_size: int = 1000
    default_compression: str = "snappy"
    default_encoding: str = "utf-8"
    default_postgres_credential_ref: str = "secret/postgres"

    # Composed Sub-Settings
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    auth: AuthSettings = Field(default_factory=AuthSettings)
    compute: ComputeSettings = Field(default_factory=ComputeSettings)
    dbt: DbtSettings = Field(default_factory=DbtSettings)
    dwh: DwhSettings = Field(default_factory=DwhSettings)
    airflow: AirflowSettings = Field(default_factory=AirflowSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)

    def __init__(self, **values: Any) -> None:
        super().__init__(**values)
        for key, val in values.items():
            prop = getattr(type(self), key, None)
            if isinstance(prop, property) and prop.fset is not None:
                setattr(self, key, val)

    # Backward Compatibility Properties (Getters and Setters)
    @property
    def database_url(self) -> str:
        return self.db.url

    @database_url.setter
    def database_url(self, val: str) -> None:
        self.db.url = val

    @property
    def secret_key(self) -> str:
        return self.auth.secret_key

    @secret_key.setter
    def secret_key(self, val: str) -> None:
        self.auth.secret_key = val

    @property
    def algorithm(self) -> str:
        return self.auth.algorithm

    @algorithm.setter
    def algorithm(self, val: str) -> None:
        self.auth.algorithm = val

    @property
    def gcp_project(self) -> str:
        return self.dwh.gcp_project

    @gcp_project.setter
    def gcp_project(self, val: str) -> None:
        self.dwh.gcp_project = val

    @property
    def dwh_provisioner_adapter(self) -> str:
        return self.dwh.provisioner_adapter

    @dwh_provisioner_adapter.setter
    def dwh_provisioner_adapter(self, val: str) -> None:
        self.dwh.provisioner_adapter = val

    @property
    def google_application_credentials(self) -> str:
        return self.dwh.google_application_credentials

    @google_application_credentials.setter
    def google_application_credentials(self, val: str) -> None:
        self.dwh.google_application_credentials = val

    @property
    def duckdb_output_dir(self) -> str:
        return self.compute.duckdb_output_dir

    @duckdb_output_dir.setter
    def duckdb_output_dir(self, val: str) -> None:
        self.compute.duckdb_output_dir = val

    @property
    def rest_api_output_dir(self) -> str:
        return self.compute.rest_api_output_dir

    @rest_api_output_dir.setter
    def rest_api_output_dir(self, val: str) -> None:
        self.compute.rest_api_output_dir = val

    @property
    def omnibeam_output_dir(self) -> str:
        return self.compute.omnibeam_output_dir

    @omnibeam_output_dir.setter
    def omnibeam_output_dir(self, val: str) -> None:
        self.compute.omnibeam_output_dir = val

    @property
    def omnibeam_binary_path(self) -> str:
        return self.compute.omnibeam_binary_path

    @omnibeam_binary_path.setter
    def omnibeam_binary_path(self, val: str) -> None:
        self.compute.omnibeam_binary_path = val

    @property
    def omnibeam_docker_image(self) -> str:
        return self.compute.omnibeam_docker_image

    @omnibeam_docker_image.setter
    def omnibeam_docker_image(self, val: str) -> None:
        self.compute.omnibeam_docker_image = val

    @property
    def dags_path(self) -> str:
        return self.airflow.dags_path

    @dags_path.setter
    def dags_path(self, val: str) -> None:
        self.airflow.dags_path = val

    @property
    def resolved_dags_path(self) -> Path:
        """Return the configured path for storing generated DAG files (PLATFORM_DAGS_PATH).

        Creates the directory if it does not exist. No silent fallback — permission
        failures will propagate immediately so they can be fixed in configuration.
        """
        p = Path(self.airflow.dags_path)
        p.mkdir(parents=True, exist_ok=True)
        logger.info("DAGs path resolved: %s", p.resolve())
        return p

    @property
    def airflow_url(self) -> str:
        return self.airflow.url

    @airflow_url.setter
    def airflow_url(self, val: str) -> None:
        self.airflow.url = val

    @property
    def airflow_username(self) -> str:
        return self.airflow.username

    @airflow_username.setter
    def airflow_username(self, val: str) -> None:
        self.airflow.username = val

    @property
    def airflow_password(self) -> str:
        return self.airflow.password

    @airflow_password.setter
    def airflow_password(self, val: str) -> None:
        self.airflow.password = val

    @property
    def catalog_adapter(self) -> str:
        return self.observability.catalog_adapter

    @catalog_adapter.setter
    def catalog_adapter(self, val: str) -> None:
        self.observability.catalog_adapter = val

    @property
    def notification_adapter(self) -> str:
        return self.observability.notification_adapter

    @notification_adapter.setter
    def notification_adapter(self, val: str) -> None:
        self.observability.notification_adapter = val

    @property
    def otel_exporter_otlp_endpoint(self) -> str | None:
        return self.observability.otlp_endpoint

    @otel_exporter_otlp_endpoint.setter
    def otel_exporter_otlp_endpoint(self, val: str | None) -> None:
        self.observability.otlp_endpoint = val

    @property
    def default_airflow_retries(self) -> int:
        return self.airflow.default_retries

    @default_airflow_retries.setter
    def default_airflow_retries(self, val: int) -> None:
        self.airflow.default_retries = val

    @property
    def default_airflow_retry_delay_minutes(self) -> int:
        return self.airflow.default_retry_delay_minutes

    @default_airflow_retry_delay_minutes.setter
    def default_airflow_retry_delay_minutes(self, val: int) -> None:
        self.airflow.default_retry_delay_minutes = val

    @property
    def default_airflow_execution_timeout_minutes(self) -> int:
        return self.airflow.default_execution_timeout_minutes

    @default_airflow_execution_timeout_minutes.setter
    def default_airflow_execution_timeout_minutes(self, val: int) -> None:
        self.airflow.default_execution_timeout_minutes = val

    @property
    def default_airflow_sla_minutes(self) -> int:
        return self.airflow.default_sla_minutes

    @default_airflow_sla_minutes.setter
    def default_airflow_sla_minutes(self, val: int) -> None:
        self.airflow.default_sla_minutes = val

    @property
    def default_airflow_pool(self) -> str:
        return self.airflow.default_pool

    @default_airflow_pool.setter
    def default_airflow_pool(self, val: str) -> None:
        self.airflow.default_pool = val

    @property
    def default_compute_engine(self) -> str:
        return self.compute.default_engine

    @default_compute_engine.setter
    def default_compute_engine(self, val: str) -> None:
        self.compute.default_engine = val

    @property
    def default_compute_staging_bucket(self) -> str:
        return self.compute.default_staging_bucket

    @default_compute_staging_bucket.setter
    def default_compute_staging_bucket(self, val: str) -> None:
        self.compute.default_staging_bucket = val

    @property
    def default_compute_num_workers(self) -> int:
        return self.compute.default_num_workers

    @default_compute_num_workers.setter
    def default_compute_num_workers(self, val: int) -> None:
        self.compute.default_num_workers = val

    @property
    def default_compute_machine_type(self) -> str:
        return self.compute.default_machine_type

    @default_compute_machine_type.setter
    def default_compute_machine_type(self, val: str) -> None:
        self.compute.default_machine_type = val

    @property
    def datahub_url(self) -> str:
        return self.observability.datahub_url

    @datahub_url.setter
    def datahub_url(self, val: str) -> None:
        self.observability.datahub_url = val

    @property
    def datahub_token(self) -> str:
        return self.observability.datahub_token

    @datahub_token.setter
    def datahub_token(self, val: str) -> None:
        self.observability.datahub_token = val

    @property
    def openmetadata_url(self) -> str:
        return self.observability.openmetadata_url

    @openmetadata_url.setter
    def openmetadata_url(self, val: str) -> None:
        self.observability.openmetadata_url = val

    @property
    def openmetadata_api_key(self) -> str:
        return self.observability.openmetadata_api_key

    @openmetadata_api_key.setter
    def openmetadata_api_key(self, val: str) -> None:
        self.observability.openmetadata_api_key = val

    @property
    def auth_jwt_public_key_pem(self) -> str:
        return self.auth.jwt_public_key_pem

    @auth_jwt_public_key_pem.setter
    def auth_jwt_public_key_pem(self, val: str) -> None:
        self.auth.jwt_public_key_pem = val

    @property
    def auth_jwt_public_key_pem_file(self) -> str:
        return self.auth.jwt_public_key_pem_file

    @auth_jwt_public_key_pem_file.setter
    def auth_jwt_public_key_pem_file(self, val: str) -> None:
        self.auth.jwt_public_key_pem_file = val

    @property
    def auth_jwt_issuer(self) -> str:
        return self.auth.jwt_issuer

    @auth_jwt_issuer.setter
    def auth_jwt_issuer(self, val: str) -> None:
        self.auth.jwt_issuer = val

    @property
    def auth_jwt_audience(self) -> str:
        return self.auth.jwt_audience

    @auth_jwt_audience.setter
    def auth_jwt_audience(self, val: str) -> None:
        self.auth.jwt_audience = val

    @property
    def jwt_roles_claim(self) -> str:
        return self.auth.jwt_roles_claim

    @jwt_roles_claim.setter
    def jwt_roles_claim(self, val: str) -> None:
        self.auth.jwt_roles_claim = val

    @property
    def permission_cache_ttl_seconds(self) -> int:
        return self.auth.permission_cache_ttl_seconds

    @permission_cache_ttl_seconds.setter
    def permission_cache_ttl_seconds(self, val: int) -> None:
        self.auth.permission_cache_ttl_seconds = val

    @property
    def rate_limit_global(self) -> str:
        return self.auth.rate_limit_global

    @rate_limit_global.setter
    def rate_limit_global(self, val: str) -> None:
        self.auth.rate_limit_global = val

    @property
    def rate_limit_write(self) -> str:
        return self.auth.rate_limit_write

    @rate_limit_write.setter
    def rate_limit_write(self, val: str) -> None:
        self.auth.rate_limit_write = val

    @property
    def resolved_auth_jwt_public_key_pem(self) -> str:
        """Return the JWT RSA public key PEM string."""
        if self.auth.jwt_public_key_pem and "BEGIN PUBLIC KEY" in self.auth.jwt_public_key_pem:
            return self.auth.jwt_public_key_pem

        if self.auth.jwt_public_key_pem:
            p = Path(self.auth.jwt_public_key_pem)
            if p.is_file():
                return p.read_text(encoding="utf-8")

        if self.auth.jwt_public_key_pem_file:
            p = Path(self.auth.jwt_public_key_pem_file)
            if p.is_file():
                return p.read_text(encoding="utf-8")

        for candidate in [Path("/run/secrets/jwt_public.pem"), Path("keys/jwt_public.pem")]:
            if candidate.is_file():
                return candidate.read_text(encoding="utf-8")

        return ""  # Fail-fast: sem chave hardcoded de desenvolvimento.

    @property
    def resolved_google_credentials_path(self) -> str | None:
        return self.dwh.resolved_credentials_path

    @property
    def dbt_project_dir(self) -> str:
        return self.dbt.project_dir

    @dbt_project_dir.setter
    def dbt_project_dir(self, val: str) -> None:
        self.dbt.project_dir = val

    @property
    def dbt_profiles_dir(self) -> str:
        return self.dbt.profiles_dir

    @dbt_profiles_dir.setter
    def dbt_profiles_dir(self, val: str) -> None:
        self.dbt.profiles_dir = val

    @property
    def dbt_output_base_dir(self) -> str:
        return self.dbt.output_base_dir

    @dbt_output_base_dir.setter
    def dbt_output_base_dir(self, val: str) -> None:
        self.dbt.output_base_dir = val

    @property
    def dbt_manifest_path(self) -> str:
        return self.dbt.manifest_path

    @dbt_manifest_path.setter
    def dbt_manifest_path(self, val: str) -> None:
        self.dbt.manifest_path = val


@cache
def get_settings() -> Settings:
    """Return the cached Settings singleton."""
    return Settings()
