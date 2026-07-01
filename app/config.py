from __future__ import annotations

from functools import cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Platform configuration loaded from environment variables.

    All fields are injected via env vars prefixed with PLATFORM_.
    Suitable for GKE ConfigMap / Secret injection.
    """

    model_config = SettingsConfigDict(env_file=".env", env_prefix="PLATFORM_")

    database_url: str
    secret_key: str
    algorithm: str = "HS256"
    debug: bool = False

    catalog_adapter: str = "noop"  # "noop" | "datahub" | "openmetadata"
    notification_adapter: str = "noop"  # "noop" | "slack"


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
