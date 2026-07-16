from typing import Any, Protocol


class AuditLogRepository(Protocol):
    """
    Protocol for recording critical system events for audit and compliance.

    Audit logs are append-only and immutable.
    """

    def save(
        self,
        event_type: str,
        entity_type: str,
        entity_id: str,
        actor_id: str,
        actor_email: str,
        payload: dict[str, Any],
        description: str,
    ) -> None:
        """
        Record a new audit log entry.
        """
        ...
