from pydantic_settings import BaseSettings, SettingsConfigDict


class HarnessSettings(BaseSettings):
    """All configuration via env vars or .env file. No hardcoded values elsewhere."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    openai_api_key: str = ""
    openai_model: str = "gpt-4o"
    openai_temperature: float = 0.0
    platform_db_url: str = ""
    metrics_storage_path: str = "./data/metrics"
    max_iterations: int = 3
    langsmith_api_key: str = ""
    langsmith_project: str = "harness-engine"


settings = HarnessSettings()  # type: ignore[call-arg]
