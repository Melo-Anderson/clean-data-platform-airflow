from __future__ import annotations

from collections.abc import Callable

from app.infrastructure.airflow_callbacks.dwh_loader_adapter import DwhLoaderAdapter


class DwhLoaderRegistry:
    """Registry for DWH loader adapters."""

    _registry: dict[str, Callable[[], DwhLoaderAdapter]] = {}

    @classmethod
    def register(cls, engine: str, factory: Callable[[], DwhLoaderAdapter]) -> None:
        cls._registry[engine.lower()] = factory

    @classmethod
    def get(cls, engine: str) -> DwhLoaderAdapter:
        factory = cls._registry.get(engine.lower())
        if factory is None:
            raise ValueError(f"Unsupported DWH Loader engine: {engine}")
        return factory()

    @classmethod
    def list_loaders(cls) -> list[str]:
        return list(cls._registry.keys())
