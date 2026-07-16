from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.shared.audit_log_repository import AuditLogRepository
from app.infrastructure.persistence.models.audit_log_model import AuditLogModel


class SqlAuditLogRepository(AuditLogRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

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
        model = AuditLogModel(
            event_type=event_type,
            entity_type=entity_type,
            entity_id=entity_id,
            actor_id=actor_id,
            actor_email=actor_email,
            payload=payload,
            description=description,
        )
        self._session.add(model)
