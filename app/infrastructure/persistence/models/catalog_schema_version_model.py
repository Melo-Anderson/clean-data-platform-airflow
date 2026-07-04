from __future__ import annotations

import uuid

from sqlalchemy import JSON, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.persistence.base_model import Base, TimestampMixin


class CatalogSchemaVersionModel(Base, TimestampMixin):
    """
    Versioned snapshot of a DataObject's schema structure.

    A new row is inserted only when the schema has structurally changed
    compared to the latest stored version. Rows are immutable after insert.
    """

    __tablename__ = "catalog_schema_versions"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    object_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("data_objects.id"), nullable=False, index=True
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    # Stores the full field list as [{name, source_type, normalized_type, nullable, is_primary_key, description}]
    snapshot_json: Mapped[list] = mapped_column(JSON, nullable=False)
