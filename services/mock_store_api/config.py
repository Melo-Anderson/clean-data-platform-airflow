from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://airflow:airflow@localhost:5432/platform_db"
    port: int = 8081
    testing: bool = False


def get_settings() -> Settings:
    # No @lru_cache: allows monkeypatch.setenv to work correctly in tests
    return Settings()
