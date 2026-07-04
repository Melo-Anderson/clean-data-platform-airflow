from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.persistence.base_model import Base, TimestampMixin


class DriftApprovalModel(Base, TimestampMixin):
    """
    ORM model for DriftApproval entities.

    One row per critical DriftEvent per DiscoveryRun.
    decision defaults to "pending".
    """

    __tablename__ = "drift_approvals"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    discovery_run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("discovery_runs.id"), nullable=False, index=True
    )
    asset_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    object_id: Mapped[str] = mapped_column(String(36), nullable=False)
    field_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    change_type: Mapped[str] = mapped_column(String(50), nullable=False)
    severity_description: Mapped[str] = mapped_column(String(2000), nullable=False)
    decision: Mapped[str] = mapped_column(String(30), nullable=False, default="pending")
    decided_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    owner_notes: Mapped[str | None] = mapped_column(String(2000), nullable=True)
