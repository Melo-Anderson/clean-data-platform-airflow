from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.persistence.base_model import Base, TimestampMixin


class PipelineRunModel(Base, TimestampMixin):
    """
    ORM model for PipelineRun operational records.

    last_run_at and last_success_at are denormalized columns (not computed)
    to allow fast dashboard queries without GROUP BY aggregations.

    These columns are maintained by the UpsertPipelineRunSummary operation
    inside SqlPipelineRunRepository.upsert_summary(), called after each save().

    Index strategy:
    - pipeline_id: dashboard queries filter by pipeline
    - started_at DESC: latest run lookup
    - status: filtering by healthy/degraded pipelines
    """

    __tablename__ = "pipeline_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    pipeline_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("pipelines.id"), nullable=False, index=True
    )
    pipeline_name: Mapped[str] = mapped_column(String(255), nullable=False)
    pipeline_type: Mapped[str] = mapped_column(String(50), nullable=False)
    dag_run_id: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Denormalized for dashboard efficiency
    last_run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    failed_task: Mapped[str | None] = mapped_column(String(255), nullable=True)
    optional_failures: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    quality_violations: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    metrics: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    sla_breached: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    sla_minutes: Mapped[int] = mapped_column(Integer, nullable=False, default=90)
