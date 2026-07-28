"""
LLM Factory — instanciação agnóstica via init_chat_model.

Isola todos os imports provider-específicos. Para trocar de provider,
basta ajustar LLM_PROVIDER + LLM_MODEL nas env vars. Nenhum código Python
precisa ser modificado.

Providers suportados (via LangChain init_chat_model):
  - openai     (padrão) → LLM_MODEL=gpt-4o
  - anthropic            → LLM_MODEL=claude-3-5-sonnet-20241022
  - google-genai         → LLM_MODEL=gemini-2.0-flash
  - ollama               → LLM_MODEL=llama3.1, LLM_BASE_URL=http://localhost:11434
"""

from __future__ import annotations

from typing import Any

from langchain.chat_models import init_chat_model
from langchain_core.language_models.chat_models import BaseChatModel

from src.config import settings


def get_llm() -> BaseChatModel:
    """Retorna um BaseChatModel configurado via env vars (sem hardcoding de provider)."""
    kwargs: dict[str, Any] = {}
    if settings.llm_base_url:
        kwargs["base_url"] = settings.llm_base_url

    return init_chat_model(
        model=settings.llm_model,
        model_provider=settings.llm_provider,
        temperature=settings.llm_temperature,
        **kwargs,
    )
