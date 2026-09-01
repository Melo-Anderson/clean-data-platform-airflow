from pathlib import Path

from app.config import AuthSettings, DwhSettings, Settings


def test_dwh_settings_resolved_credentials_path_with_valid_file(tmp_path: Path) -> None:
    key_file = tmp_path / "gcp-key.json"
    key_file.write_text('{"project_id": "test-prj"}', encoding="utf-8")
    dwh = DwhSettings(google_application_credentials=str(key_file))
    assert dwh.resolved_credentials_path == str(key_file.resolve())


def test_dwh_settings_resolved_credentials_path_returns_none_when_empty() -> None:
    dwh = DwhSettings(google_application_credentials="", google_application_credentials_host="")
    assert dwh.resolved_credentials_path is None


def test_auth_settings_no_hardcoded_dev_key_by_default(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    settings = Settings(auth=AuthSettings(jwt_public_key_pem="", jwt_public_key_pem_file=""))
    # Deve retornar string vazia — sem chave RSA de desenvolvimento hardcoded como fallback.
    assert settings.resolved_auth_jwt_public_key_pem == ""
