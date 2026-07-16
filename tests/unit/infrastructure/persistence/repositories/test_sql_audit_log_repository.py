import pytest

from app.infrastructure.persistence.repositories.sql_audit_log_repository import (
    SqlAuditLogRepository,
)


class MockSession:
    def __init__(self):
        self.added = []

    def add(self, model):
        self.added.append(model)


@pytest.mark.asyncio
async def test_sql_audit_log_repository_save():
    session = MockSession()
    repo = SqlAuditLogRepository(session)
    repo.save(
        event_type="test.event",
        entity_type="TestEntity",
        entity_id="123",
        actor_id="user1",
        actor_email="user1@local",
        payload={"k": "v"},
        description="test log",
    )
    assert len(session.added) == 1
    assert session.added[0].event_type == "test.event"
    assert session.added[0].entity_id == "123"
    assert session.added[0].payload == {"k": "v"}
