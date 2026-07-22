from services.mock_store_api.config import get_settings


def test_settings_default_values():
    settings = get_settings()
    assert settings.database_url.startswith("postgresql+asyncpg://")
    assert settings.port == 8081


def test_settings_load_from_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/testdb")
    monkeypatch.setenv("PORT", "9999")

    settings = get_settings()
    assert settings.database_url == "postgresql+asyncpg://test:test@localhost:5432/testdb"
    assert settings.port == 9999
