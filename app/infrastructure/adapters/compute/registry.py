from __future__ import annotations

from collections.abc import Callable

from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobAdapter


class ComputeAdapterRegistry:
    """Registry for compute engine adapters."""

    _registry: dict[str, Callable[[], ComputeJobAdapter]] = {}

    @classmethod
    def register(cls, engine: str, factory: Callable[[], ComputeJobAdapter]) -> None:
        cls._registry[engine.lower()] = factory

    @classmethod
    def get(cls, engine: str) -> ComputeJobAdapter:
        factory = cls._registry.get(engine.lower())
        if factory is None:
            raise ValueError(
                f"Unsupported compute engine: '{engine}'. Registered engines: {cls.list_engines()}"
            )
        return factory()

    @classmethod
    def list_engines(cls) -> list[str]:
        return list(cls._registry.keys())
