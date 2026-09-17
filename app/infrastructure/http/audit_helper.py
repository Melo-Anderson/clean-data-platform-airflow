from typing import Any

from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.repositories.sql_audit_log_repository import (
    SqlAuditLogRepository,
)

SYSTEM_ACTOR_ID: str = "airflow_worker"
"""System actor ID used in audit logs for callbacks originating from Airflow workers."""

SYSTEM_ACTOR_EMAIL: str = "worker@airflow.apache.org"
"""System actor e-mail used in audit logs for callbacks originating from Airflow workers."""


async def write_audit_log_task(
    actor_id: str,
    actor_email: str,
    event_type: str,
    entity_type: str,
    entity_id: str,
    payload: dict[str, Any],
    description: str,
) -> None:
    session_factory = get_session_factory()
    async with session_factory() as session:
        repo = SqlAuditLogRepository(session)
        repo.save(
            event_type=event_type,
            entity_type=entity_type,
            entity_id=entity_id,
            actor_id=actor_id,
            actor_email=actor_email,
            payload=payload,
            description=description,
        )
        await session.commit()
