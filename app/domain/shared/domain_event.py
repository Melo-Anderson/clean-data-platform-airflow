from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass(frozen=True)
class DomainEvent:
    """
    Base class for all domain events.

    Events are immutable records of something that happened in the domain.
    Collected during a unit of work and dispatched after commit.
    """

    occurred_at: datetime = field(default_factory=_utcnow)
    actor_id: str = ""
    actor_email: str = ""


@dataclass(frozen=True)
class AssetRegistered(DomainEvent):
    """Raised when a new DataAsset is successfully created in DRAFT state."""

    asset_id: str = ""
    asset_name: str = ""


@dataclass(frozen=True)
class AssetActivated(DomainEvent):
    """Raised when a DataAsset transitions from DRAFT to ACTIVE."""

    asset_id: str = ""
    endpoint_id: str = ""


@dataclass(frozen=True)
class AssetStateChanged(DomainEvent):
    """Raised on any DataAsset lifecycle state transition."""

    asset_id: str = ""
    previous_state: str = ""
    new_state: str = ""


@dataclass(frozen=True)
class EndpointProvisioned(DomainEvent):
    """Raised when an SRE provisions a new Endpoint."""

    endpoint_id: str = ""
    asset_id: str = ""
    endpoint_type: str = ""
