from __future__ import annotations

from dataclasses import dataclass, field

from app.domain.assets.asset_state import AssetState
from app.domain.shared.auditable import Auditable
from app.domain.shared.policy_tag import PolicyTag
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress


@dataclass(kw_only=True)
class DataAsset(Auditable):
    """
    DataAsset: aggregate root representing a business data domain.

    Stable after registration. discovery_scope and discovery_schedule
    are the only fields modified after activation (no SRE required).
    endpoint_id is set during the SRE handoff (DRAFT → ACTIVE transition).

    No SQLAlchemy. No Pydantic. No FastAPI.
    """

    id: str
    name: str
    description: str
    owner: EmailAddress
    tags: list[str] = field(default_factory=list)
    policy_tags: list[PolicyTag] = field(default_factory=list)
    state: AssetState = AssetState.DRAFT
    discovery_schedule: CronSchedule = field(default_factory=lambda: CronSchedule("0 6 * * *"))
    discovery_scope: DiscoveryScope = field(default_factory=DiscoveryScope)
    endpoint_id: str | None = None
