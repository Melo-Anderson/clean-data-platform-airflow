from __future__ import annotations

import logging

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.assets.data_asset import DataAsset
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.lineage.lineage_mapping import LineageMapping
from app.infrastructure.persistence.models.catalog_schema_version_model import (
    CatalogSchemaVersionModel,
)
from app.infrastructure.persistence.models.lineage_mapping_model import LineageMappingModel

logger = logging.getLogger(__name__)


def _snapshot_to_json(snapshot: SchemaSnapshot) -> list[dict]:
    """Converts a SchemaSnapshot's fields into a stable, serializable list of dicts."""
    return [
        {
            "name": f.name,
            "source_type": f.source_type,
            "normalized_type": f.normalized_type,
            "nullable": f.nullable,
            "is_primary_key": f.is_primary_key,
            "description": f.description or "",
        }
        for f in snapshot.fields
    ]


class DatabaseCatalogAdapter:
    """
    Local database implementation of the CatalogAdapter protocol.

    Stores versioned schema snapshots and lineage edges in the platform's own
    Postgres database. No external catalog dependency required.

    Versioning contract:
      - A new CatalogSchemaVersionModel row is only inserted when snapshot_json
        differs from the latest stored version.
      - Historical version rows are immutable — update_policy_tags creates a new version.
      - Lineage edges are upserted by (pipeline_id, source_object_id, destination_object_id).
    """

    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._session_factory = session_factory

    async def publish_asset(self, asset_id: str, name: str, state: str, metadata: dict) -> None:
        pass  # Database catalog doesn't track high-level assets natively yet

    async def publish_schema(self, asset: DataAsset, snapshot: SchemaSnapshot) -> None:
        """
        Inserts a new schema version only if the structure has changed.
        Idempotent: calling with the same snapshot twice produces one version.
        """
        incoming = _snapshot_to_json(snapshot)

        async with self._session_factory() as session:
            latest = await self._latest_version(session, snapshot.object_id)

            if latest is not None and latest.snapshot_json == incoming:
                logger.debug(
                    "publish_schema: no structural change for object_id=%s (v%d). Skipped.",
                    snapshot.object_id,
                    latest.version,
                )
                return

            next_version = (latest.version + 1) if latest else 1
            session.add(
                CatalogSchemaVersionModel(
                    object_id=snapshot.object_id,
                    version=next_version,
                    snapshot_json=incoming,
                )
            )
            await session.commit()
            logger.info(
                "publish_schema: saved v%d for object_id=%s (%d fields).",
                next_version,
                snapshot.object_id,
                len(incoming),
            )

    async def publish_lineage(self, mapping: LineageMapping) -> None:
        """
        Upserts a lineage edge. If the (pipeline, source, destination) triple already
        exists, column_mappings is updated. Otherwise a new edge is inserted.
        """
        serialized = [
            {
                "source_column": col.source_column,
                "destination_column": col.destination_column,
                "expression": col.transformation_expression or "",
            }
            for col in mapping.column_mappings
        ]

        async with self._session_factory() as session:
            query = select(LineageMappingModel).filter_by(
                pipeline_id=mapping.pipeline_id,
                source_object_id=mapping.source_object_id,
                destination_object_id=mapping.destination_object_id,
            )
            existing = (await session.execute(query)).scalar_one_or_none()

            if existing:
                existing.column_mappings = serialized
            else:
                session.add(
                    LineageMappingModel(
                        id=mapping.id,
                        pipeline_id=mapping.pipeline_id,
                        source_object_id=mapping.source_object_id,
                        destination_object_id=mapping.destination_object_id,
                        column_mappings=serialized,
                    )
                )
            await session.commit()

    async def update_policy_tags(self, object_id: str, policy_tags: dict[str, str]) -> None:
        """
        Applies governance policy tags by creating a new immutable schema version.

        The latest version is read and a copy is created with the tags applied.
        Historical versions are never mutated, preserving the audit trail.
        """
        async with self._session_factory() as session:
            latest = await self._latest_version(session, object_id)

            if not latest:
                logger.warning(
                    "update_policy_tags: no schema version found for object_id=%s. Skipped.",
                    object_id,
                )
                return

            updated_fields = [
                {**col, "policy_tag": policy_tags[col["name"]]}
                if col["name"] in policy_tags
                else col
                for col in latest.snapshot_json
            ]

            session.add(
                CatalogSchemaVersionModel(
                    object_id=object_id,
                    version=latest.version + 1,
                    snapshot_json=updated_fields,
                )
            )
            await session.commit()

    @staticmethod
    async def _latest_version(
        session: AsyncSession, object_id: str
    ) -> CatalogSchemaVersionModel | None:
        """Retrieves the highest-version schema record for a given object."""
        query = (
            select(CatalogSchemaVersionModel)
            .filter_by(object_id=object_id)
            .order_by(desc(CatalogSchemaVersionModel.version))
            .limit(1)
        )
        return (await session.execute(query)).scalar_one_or_none()
