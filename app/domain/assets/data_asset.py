from __future__ import annotations

from dataclasses import dataclass, field

from app.domain.assets.asset_state import VALID_TRANSITIONS, AssetState
from app.domain.shared.auditable import Auditable
from app.domain.shared.exceptions import PlatformValidationError
from app.domain.shared.policy_tag import PolicyTag
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress


class InvalidStateTransitionError(PlatformValidationError):
    def __init__(self, current: AssetState, target: AssetState) -> None:
        allowed = sorted(VALID_TRANSITIONS[current])
        super().__init__(
            f"Cannot transition from '{current}' to '{target}'. "
            f"Allowed targets from '{current}': {allowed}"
        )


@dataclass(kw_only=True)
class DataAsset(Auditable):
    """
    DataAsset: aggregate root representing a business data domain.

    Encapsulates its own lifecycle transitions and metadata invariants.
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

    def activate(self, endpoint_id: str) -> None:
        """Transition DRAFT -> ACTIVE when SRE provisions the Endpoint."""
        if not endpoint_id or not endpoint_id.strip():
            raise ValueError(f"endpoint_id cannot be empty for activation on asset '{self.name}'")
        self._assert_valid_transition(AssetState.ACTIVE)
        self.endpoint_id = endpoint_id.strip()
        self.state = AssetState.ACTIVE
        self.touch()

    def deprecate(self) -> None:
        """Transition ACTIVE -> DEPRECATED."""
        self._assert_valid_transition(AssetState.DEPRECATED)
        self.state = AssetState.DEPRECATED
        self.touch()

    def archive(self) -> None:
        """Transition DEPRECATED -> ARCHIVED."""
        self._assert_valid_transition(AssetState.ARCHIVED)
        self.state = AssetState.ARCHIVED
        self.touch()

    def update_scope(self, scope: DiscoveryScope) -> None:
        """Update discovery_scope."""
        self.discovery_scope = scope
        self.touch()

    def update_schedule(self, schedule: CronSchedule) -> None:
        """Update discovery_schedule."""
        self.discovery_schedule = schedule
        self.touch()

    def update_metadata(
        self,
        description: str | None = None,
        owner: EmailAddress | None = None,
        tags: list[str] | None = None,
        policy_tags: list[PolicyTag] | None = None,
    ) -> None:
        """Update general metadata properties."""
        if description is not None:
            self.description = description
        if owner is not None:
            self.owner = owner
        if tags is not None:
            self.tags = tags
        if policy_tags is not None:
            self.policy_tags = policy_tags
        self.touch()

    def _assert_valid_transition(self, target: AssetState) -> None:
        if target not in VALID_TRANSITIONS[self.state]:
            raise InvalidStateTransitionError(self.state, target)
