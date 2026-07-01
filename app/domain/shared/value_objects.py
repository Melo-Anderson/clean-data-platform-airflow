from __future__ import annotations

from dataclasses import dataclass, field

from croniter import CroniterBadCronError, croniter


@dataclass(frozen=True)
class EmailAddress:
    """
    Value Object for a validated email address.

    Immutable and comparable by value. Raises ValueError on invalid input.

    Example:
        owner = EmailAddress("eng@company.com")
    """

    value: str

    def __post_init__(self) -> None:
        if "@" not in self.value or not self.value.strip():
            raise ValueError(
                f"Invalid email: {self.value!r}. Expected format: user@domain.com"
            )


@dataclass(frozen=True)
class CredentialReference:
    """
    Value Object for a Vault / Secret Manager lookup path.

    Stores only the reference path — never the actual credential.

    Example:
        ref = CredentialReference("vault/secret/oracle-prod")
    """

    path: str

    def __post_init__(self) -> None:
        if not self.path.strip():
            raise ValueError(
                f"CredentialReference path cannot be empty: {self.path!r}"
            )


@dataclass(frozen=True)
class CronSchedule:
    """
    Value Object for a validated 5-field cron expression.

    Raises ValueError if the expression is not a valid standard cron.

    Example:
        sched = CronSchedule("0 6 * * *")  # daily at 06:00
    """

    expression: str

    def __post_init__(self) -> None:
        parts = self.expression.strip().split()
        if len(parts) != 5:
            raise ValueError(
                f"Invalid cron expression: {self.expression!r}. "
                "Expected exactly 5 fields: minute hour day month weekday."
            )
        try:
            croniter(self.expression)
        except CroniterBadCronError as exc:
            raise ValueError(
                f"Invalid cron expression: {self.expression!r}. Detail: {exc}"
            ) from exc


@dataclass(frozen=True)
class DiscoveryScope:
    """
    Value Object representing which DataObjects should be included/excluded during Discovery.

    Empty include list means 'scan everything'. Exclude patterns support glob syntax.

    Example:
        scope = DiscoveryScope(include=["customers", "orders"], exclude=["temp_*"])
    """

    include: tuple[str, ...] = field(default_factory=tuple)
    exclude: tuple[str, ...] = field(default_factory=tuple)

    def __init__(
        self,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> None:
        object.__setattr__(self, "include", tuple(include or []))
        object.__setattr__(self, "exclude", tuple(exclude or []))

    def to_dict(self) -> dict[str, list[str]]:
        """Serialize to a plain dict for storage."""
        return {"include": list(self.include), "exclude": list(self.exclude)}

    @classmethod
    def from_dict(cls, data: dict[str, list[str]]) -> DiscoveryScope:
        """Deserialize from a plain dict (e.g., from JSON storage)."""
        return cls(include=data.get("include", []), exclude=data.get("exclude", []))
