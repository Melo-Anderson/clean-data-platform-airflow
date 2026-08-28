from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.persistence.base_model import Base, TimestampMixin


class PipelineRunFileModel(Base, TimestampMixin):
    """
    ORM model for physical files associated with a PipelineRun.
    """

    __tablename__ = "pipeline_run_files"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    pipeline_run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False
    )
    file_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    file_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    mtime: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    hash_md5: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="PENDING")
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    pipeline_run = relationship("PipelineRunModel", back_populates="files")

    __table_args__ = (
        Index("ix_pipeline_run_files_run_id", "pipeline_run_id"),
        Index("ix_pipeline_run_files_hash_md5", "hash_md5"),
    )
