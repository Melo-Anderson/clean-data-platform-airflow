from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.persistence.base_model import Base, TimestampMixin


class DiscoveryRunModel(Base, TimestampMixin):
    """
    ORM model for DiscoveryRun aggregates.

    JSON fields are denormalized for simplicity at this phase.
    """

    __tablename__ = "discovery_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    asset_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    triggered_by: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(2000), nullable=True)
    objects_discovered: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    fields_discovered: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    snapshots_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    drift_events_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    policy_suggestions_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    auto_descriptions_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    soft_failures: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
