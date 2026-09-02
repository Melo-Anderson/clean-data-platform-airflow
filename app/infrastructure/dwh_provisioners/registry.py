from __future__ import annotations

from collections.abc import Callable

from app.application.shared.ports.dwh_provisioner_port import DwhProvisionerPort
from app.config import Settings


class DwhProvisionerRegistry:
    """Registry for DWH provisioner adapters."""

    _registry: dict[str, Callable[[Settings], DwhProvisionerPort]] = {}

    @classmethod
    def register(cls, adapter_name: str, factory: Callable[[Settings], DwhProvisionerPort]) -> None:
        cls._registry[adapter_name.lower()] = factory

    @classmethod
    def get(cls, settings: Settings) -> DwhProvisionerPort:
        adapter_name = settings.dwh_provisioner_adapter.lower()
        factory = cls._registry.get(adapter_name)
        if factory is None:
            raise ValueError(
                f"Unsupported DWH provisioner adapter: '{adapter_name}'. Registered: {cls.list_provisioners()}"
            )
        return factory(settings)

    @classmethod
    def list_provisioners(cls) -> list[str]:
        return list(cls._registry.keys())
