from __future__ import annotations

from functools import cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Platform configuration loaded from environment variables.

    All fields are injected via env vars prefixed with PLATFORM_.
    Suitable for Kubernetes ConfigMap / Secret injection or environment variables.
    """

    model_config = SettingsConfigDict(env_file=".env", env_prefix="PLATFORM_")

    database_url: str
    secret_key: str
    algorithm: str = "HS256"
    debug: bool = False

    catalog_adapter: str = "noop"  # "noop" | "database" | "datahub" | "openmetadata"
    notification_adapter: str = "noop"  # "noop" | "slack"
    secret_manager_adapter: str = "noop"  # "noop" | "openbao"

    # DataHub settings (used only when catalog_adapter = "datahub")
    datahub_url: str = ""
    datahub_token: str = ""

    # OpenMetadata settings (used only when catalog_adapter = "openmetadata")
    openmetadata_url: str = ""
    openmetadata_api_key: str = ""

    vault_url: str = ""
    vault_token: str = ""

    auth_jwt_public_key_pem: str = ""
    auth_jwt_issuer: str = ""
    auth_jwt_audience: str = ""
    jwt_roles_claim: str = "roles"
    permission_cache_ttl_seconds: int = 300


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
    return Settings()  # type: ignore[call-arg]
