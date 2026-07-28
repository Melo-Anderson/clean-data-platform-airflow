"""Tests para o LLM factory agnóstico."""

from unittest.mock import MagicMock, patch


@patch("src.infrastructure.llm_factory.init_chat_model")
def test_get_llm_calls_init_chat_model_with_defaults(mock_init: MagicMock) -> None:
    """Deve chamar init_chat_model com os valores de config padrão."""
    # Importar aqui para pegar o mock
    from src.infrastructure.llm_factory import get_llm

    get_llm()
    mock_init.assert_called_once_with(
        model="gpt-4o",
        model_provider="openai",
        temperature=0.0,
    )


@patch("src.infrastructure.llm_factory.init_chat_model")
def test_get_llm_passes_base_url_when_configured(mock_init: MagicMock) -> None:
    """Deve passar base_url quando llm_base_url estiver configurado."""
    from src.config import settings

    original = settings.llm_base_url
    settings.llm_base_url = "http://localhost:11434"
    try:
        from src.infrastructure import llm_factory

        llm_factory.get_llm()
        call_kwargs = mock_init.call_args[1]
        assert call_kwargs.get("base_url") == "http://localhost:11434"
    finally:
        settings.llm_base_url = original
