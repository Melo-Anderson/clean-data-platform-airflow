from __future__ import annotations

import logging
import os
from functools import cache
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class DatabaseSettings(BaseModel):
    url: str = ""
    pool_size: int = 20
    max_overflow: int = 10


class AuthSettings(BaseModel):
    secret_key: str = ""
    algorithm: Literal["HS256", "RS256"] = "HS256"
    jwt_public_key_pem: str = ""
    jwt_public_key_pem_file: str = ""
    jwt_issuer: str = ""
    jwt_audience: str = ""
    jwt_roles_claim: str = "roles"
    permission_cache_ttl_seconds: int = 300


class RateLimitSettings(BaseModel):
    global_: str = Field(default="100/minute", alias="global")
    write: str = "10/minute"

    model_config = ConfigDict(populate_by_name=True)


class ComputeSettings(BaseModel):
    duckdb_output_dir: str = "/tmp/duckdb_outputs"
    rest_api_output_dir: str = "/tmp/airflow_data"
    omnibeam_output_dir: str = "/tmp/omnibeam_outputs"
    omnibeam_docker_image: str = "omnibeam-pipeline:latest"
    omnibeam_binary_path: str = "pipeline"
    transformation_staging_bucket: str = "/opt/airflow/logs/transformation_outputs"
    dbt_staging_bucket: str = "/opt/airflow/logs/dbt_outputs"
    default_engine: Literal["duckdb", "omnibeam", "rest_api", "dbt", "dataform"] = "duckdb"
    default_staging_bucket: str = ""
    default_num_workers: int = 1
    default_machine_type: str = "n1-standard-2"


class DbtSettings(BaseModel):
    project_dir: str = "/opt/airflow/dbt_project"
    profiles_dir: str = "/opt/airflow/dbt_project"
    output_base_dir: str = "/opt/airflow/logs/dbt_outputs"
    manifest_path: str = "/opt/airflow/dbt_project/target/manifest.json"


class DataformSettings(BaseModel):
    project_dir: str = "/opt/airflow/dataform_project"
    output_base_dir: str = "/opt/airflow/logs/dataform_outputs"
    compilation_result_path: str = "/opt/airflow/dataform_project/compilation_result.json"
    default_schema: str = "dataform"


class DwhSettings(BaseModel):
    gcp_project: str = ""
    provisioner_adapter: Literal["noop", "bigquery", "snowflake", "databricks"] = "noop"
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
    notification_adapter: Literal["noop", "slack", "email", "webhook"] = "noop"
    catalog_adapter: Literal["noop", "datahub", "openmetadata"] = "noop"
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
    secret_manager_adapter: Literal["noop", "openbao", "vault"] = "noop"
    vault_url: str = ""
    vault_token: str = ""
    platform_api_url: str = "http://platform-api:8000"
    build_commit_hash: str = "unknown"

    default_load_strategy: Literal["full_load", "incremental", "cdc"] = "full_load"
    default_page_size: int = 1000
    default_compression: Literal["snappy", "gzip", "zstd", "none"] = "snappy"
    default_encoding: str = "utf-8"
    default_postgres_credential_ref: str = "secret/postgres"

    # Composed Sub-Settings
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    auth: AuthSettings = Field(default_factory=AuthSettings)
    compute: ComputeSettings = Field(default_factory=ComputeSettings)
    dbt: DbtSettings = Field(default_factory=DbtSettings)
    dataform: DataformSettings = Field(default_factory=DataformSettings)
    dwh: DwhSettings = Field(default_factory=DwhSettings)
    airflow: AirflowSettings = Field(default_factory=AirflowSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    rate_limit: RateLimitSettings = Field(default_factory=RateLimitSettings)
    cors_origins: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_required_settings(self) -> Self:
        if not self.db.url:
            raise ValueError("PLATFORM_DB__URL is required. Set it via environment variable.")
        if not self.auth.secret_key:
            raise ValueError(
                "PLATFORM_AUTH__SECRET_KEY is required. Set it via environment variable."
            )
        return self

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


@cache
def get_settings() -> Settings:
    """Return the cached Settings singleton."""
    return Settings()
