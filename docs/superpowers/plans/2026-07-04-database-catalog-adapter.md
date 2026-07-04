# Database Catalog Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a local `DatabaseCatalogAdapter` that satisfies the `CatalogAdapter` protocol, storing versioned schema snapshots (only on change) and lineage edges in dedicated Postgres tables.

**Architecture:** Two new SQLAlchemy models inherit from `Base` + `TimestampMixin` (platform standard). The adapter receives a `async_sessionmaker`, compares incoming snapshots structurally before writing, and upserts lineage edges. A `catalog_factory.py` reads all adapter configuration exclusively from `Settings` — no hardcoded URLs.

**Tech Stack:** Python 3.12, SQLAlchemy 2.0, pytest-asyncio, aiosqlite (for tests)

## Global Constraints

- All ORM models **must** inherit from both `Base` and `TimestampMixin` (platform convention in `base_model.py`).
- No hardcoded URLs or credentials anywhere in factory or adapter code; all configuration comes from `Settings`.
- Use async SQLAlchemy exclusively — no sync DB calls.
- Schema versions are immutable records — never mutate a historical version's JSON. A tag update creates a new version.
- No unused imports.

---

### Task 1: Database Models for Local Catalog

**Files:**
- Create: `app/infrastructure/persistence/models/catalog_schema_version_model.py`
- Create: `app/infrastructure/persistence/models/catalog_lineage_model.py`
- Modify: `app/infrastructure/persistence/models/__init__.py`

**Interfaces:**
- Produces: `CatalogSchemaVersionModel`, `CatalogLineageModel` (both importable from `app.infrastructure.persistence.models`).

- [ ] **Step 1: Create the schema version model**

  Create `app/infrastructure/persistence/models/catalog_schema_version_model.py`:
  ```python
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
  ```

  > **Note:** `created_at` / `updated_at` are provided by `TimestampMixin`. Do NOT redefine them. The field is named `snapshot_json` (not `columns_json`) for semantic clarity.

- [ ] **Step 2: Create the lineage edge model**

  Create `app/infrastructure/persistence/models/catalog_lineage_model.py`:
  ```python
  from __future__ import annotations

  import uuid

  from sqlalchemy import JSON, ForeignKey, String
  from sqlalchemy.orm import Mapped, mapped_column

  from app.infrastructure.persistence.base_model import Base, TimestampMixin


  class CatalogLineageModel(Base, TimestampMixin):
      """
      Represents a lineage edge between source and destination DataObjects.

      Rows are upserted: if an edge for (pipeline_id, source, destination) already
      exists, only column_mappings is updated; no duplicate edges are created.
      """

      __tablename__ = "catalog_lineages"

      id: Mapped[str] = mapped_column(
          String(36), primary_key=True, default=lambda: str(uuid.uuid4())
      )
      pipeline_id: Mapped[str] = mapped_column(
          String(36), ForeignKey("pipelines.id"), nullable=False
      )
      source_object_id: Mapped[str] = mapped_column(
          String(36), ForeignKey("data_objects.id"), nullable=False
      )
      destination_object_id: Mapped[str] = mapped_column(
          String(36), ForeignKey("data_objects.id"), nullable=False
      )
      # Stores [{source_column, destination_column, expression}]
      column_mappings: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
  ```

- [ ] **Step 3: Register the new models in the package `__init__`**

  Add to the **top of** `app/infrastructure/persistence/models/__init__.py` (alongside the existing imports):
  ```python
  from app.infrastructure.persistence.models.catalog_schema_version_model import CatalogSchemaVersionModel
  from app.infrastructure.persistence.models.catalog_lineage_model import CatalogLineageModel
  ```
  And add `"CatalogSchemaVersionModel"` and `"CatalogLineageModel"` to `__all__`.

  Full expected `__all__` after modification:
  ```python
  __all__ = [
      "AuditLogModel",
      "CatalogLineageModel",
      "CatalogSchemaVersionModel",
      "DataAssetModel",
      "DataObjectModel",
      "DiscoveryRunModel",
      "DriftApprovalModel",
      "EndpointModel",
      "LineageMappingModel",
      "PipelineModel",
      "PipelineObjectModel",
      "PipelineRunModel",
  ]
  ```

- [ ] **Step 4: Commit**
  ```bash
  git add app/infrastructure/persistence/models/catalog_schema_version_model.py \
          app/infrastructure/persistence/models/catalog_lineage_model.py \
          app/infrastructure/persistence/models/__init__.py
  git commit -m "feat(catalog): add ORM models for local catalog versioning and lineage"
  ```

---

### Task 2: Implement `DatabaseCatalogAdapter` (TDD)

**Files:**
- Create: `app/infrastructure/adapters/catalog/database_catalog_adapter.py`
- Test: `tests/unit/infrastructure/adapters/catalog/test_database_catalog_adapter.py`

**Interfaces:**
- Consumes: `CatalogAdapter` protocol (`app/application/shared/adapters/catalog_adapter.py`), `CatalogSchemaVersionModel`, `CatalogLineageModel`.
- Produces: `DatabaseCatalogAdapter(session_factory: async_sessionmaker)` with methods `publish_schema`, `publish_lineage`, `update_policy_tags`.

- [ ] **Step 1: Write failing tests**

  Create `tests/unit/infrastructure/adapters/catalog/test_database_catalog_adapter.py`:
  ```python
  from __future__ import annotations

  import pytest
  from sqlalchemy import select
  from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

  from app.domain.assets.data_asset import DataAsset
  from app.domain.discovery.schema_field import SchemaField
  from app.domain.discovery.schema_snapshot import SchemaSnapshot
  from app.domain.lineage.lineage_mapping import ElementLineage, LineageMapping
  from app.infrastructure.adapters.catalog.database_catalog_adapter import DatabaseCatalogAdapter
  from app.infrastructure.persistence.base_model import Base
  from app.infrastructure.persistence.models.catalog_lineage_model import CatalogLineageModel
  from app.infrastructure.persistence.models.catalog_schema_version_model import CatalogSchemaVersionModel
  from app.infrastructure.persistence.models.data_asset_model import DataAssetModel
  from app.infrastructure.persistence.models.data_object_model import DataObjectModel
  from app.infrastructure.persistence.models.pipeline_model import PipelineModel


  @pytest.fixture
  async def session_factory():
      engine = create_async_engine("sqlite+aiosqlite:///:memory:")
      factory = async_sessionmaker(engine, expire_on_commit=False)
      async with engine.begin() as conn:
          await conn.run_sync(Base.metadata.create_all)
      async with factory() as session:
          session.add_all([
              DataAssetModel(
                  id="asset-1", name="sales", description="d", owner_email="x@y.com",
                  discovery_schedule="* * * * *",
              ),
              DataObjectModel(id="obj-1", asset_id="asset-1", name="orders", type="table"),
              DataObjectModel(id="obj-2", asset_id="asset-1", name="customers", type="table"),
              PipelineModel(id="pipe-1", name="etl", description="d", schedule="* * * * *", definition_yaml="x"),
          ])
          await session.commit()
      yield factory
      await engine.dispose()


  def _make_asset() -> DataAsset:
      return DataAsset(
          id="asset-1", name="sales", description="d", owner="x@y.com",
          tags=[], policy_tags=[], discovery_schedule="* * * * *",
      )


  def _make_snapshot(fields: list[SchemaField]) -> SchemaSnapshot:
      return SchemaSnapshot(object_id="obj-1", object_name="orders", runner_type="sqlite", fields=fields)


  # ---------- publish_schema ----------

  @pytest.mark.asyncio
  async def test_publish_schema_creates_first_version(session_factory: async_sessionmaker) -> None:
      adapter = DatabaseCatalogAdapter(session_factory)
      snap = _make_snapshot([SchemaField(name="id", source_type="INT", normalized_type="integer", nullable=False)])

      await adapter.publish_schema(_make_asset(), snap)

      async with session_factory() as s:
          rows = (await s.execute(select(CatalogSchemaVersionModel).filter_by(object_id="obj-1"))).scalars().all()
      assert len(rows) == 1
      assert rows[0].version == 1
      assert rows[0].snapshot_json[0]["name"] == "id"


  @pytest.mark.asyncio
  async def test_publish_schema_is_idempotent_when_unchanged(session_factory: async_sessionmaker) -> None:
      adapter = DatabaseCatalogAdapter(session_factory)
      field = SchemaField(name="id", source_type="INT", normalized_type="integer", nullable=False)
      snap = _make_snapshot([field])

      await adapter.publish_schema(_make_asset(), snap)
      await adapter.publish_schema(_make_asset(), snap)  # identical — must NOT create v2

      async with session_factory() as s:
          rows = (await s.execute(select(CatalogSchemaVersionModel).filter_by(object_id="obj-1"))).scalars().all()
      assert len(rows) == 1


  @pytest.mark.asyncio
  async def test_publish_schema_increments_version_on_structural_change(session_factory: async_sessionmaker) -> None:
      adapter = DatabaseCatalogAdapter(session_factory)
      snap_v1 = _make_snapshot([SchemaField(name="id", source_type="INT", normalized_type="integer", nullable=False)])
      snap_v2 = _make_snapshot([
          SchemaField(name="id", source_type="INT", normalized_type="integer", nullable=False),
          SchemaField(name="name", source_type="VARCHAR", normalized_type="string", nullable=True),
      ])

      await adapter.publish_schema(_make_asset(), snap_v1)
      await adapter.publish_schema(_make_asset(), snap_v2)

      async with session_factory() as s:
          rows = (await s.execute(
              select(CatalogSchemaVersionModel)
              .filter_by(object_id="obj-1")
              .order_by(CatalogSchemaVersionModel.version)
          )).scalars().all()
      assert len(rows) == 2
      assert rows[0].version == 1 and len(rows[0].snapshot_json) == 1
      assert rows[1].version == 2 and len(rows[1].snapshot_json) == 2


  # ---------- publish_lineage ----------

  @pytest.mark.asyncio
  async def test_publish_lineage_creates_edge(session_factory: async_sessionmaker) -> None:
      adapter = DatabaseCatalogAdapter(session_factory)
      mapping = LineageMapping(
          id="lin-1", pipeline_id="pipe-1",
          source_object_id="obj-1", destination_object_id="obj-2",
          column_mappings=[ElementLineage(source_column="id", destination_column="order_id")],
      )

      await adapter.publish_lineage(mapping)

      async with session_factory() as s:
          rows = (await s.execute(select(CatalogLineageModel).filter_by(pipeline_id="pipe-1"))).scalars().all()
      assert len(rows) == 1
      assert rows[0].column_mappings[0]["source_column"] == "id"


  @pytest.mark.asyncio
  async def test_publish_lineage_upserts_existing_edge(session_factory: async_sessionmaker) -> None:
      adapter = DatabaseCatalogAdapter(session_factory)
      mapping_v1 = LineageMapping(
          id="lin-1", pipeline_id="pipe-1",
          source_object_id="obj-1", destination_object_id="obj-2",
          column_mappings=[ElementLineage(source_column="id", destination_column="order_id")],
      )
      mapping_v2 = LineageMapping(
          id="lin-2", pipeline_id="pipe-1",  # same edge
          source_object_id="obj-1", destination_object_id="obj-2",
          column_mappings=[
              ElementLineage(source_column="id", destination_column="order_id"),
              ElementLineage(source_column="total", destination_column="amount"),
          ],
      )

      await adapter.publish_lineage(mapping_v1)
      await adapter.publish_lineage(mapping_v2)  # must update, not duplicate

      async with session_factory() as s:
          rows = (await s.execute(select(CatalogLineageModel))).scalars().all()
      assert len(rows) == 1
      assert len(rows[0].column_mappings) == 2


  # ---------- update_policy_tags ----------

  @pytest.mark.asyncio
  async def test_update_policy_tags_creates_new_version(session_factory: async_sessionmaker) -> None:
      """Policy tag updates must create a new immutable version, not mutate a historical row."""
      adapter = DatabaseCatalogAdapter(session_factory)
      snap = _make_snapshot([SchemaField(name="cpf", source_type="VARCHAR", normalized_type="string", nullable=True)])
      await adapter.publish_schema(_make_asset(), snap)

      await adapter.update_policy_tags("obj-1", {"cpf": "PII"})

      async with session_factory() as s:
          rows = (await s.execute(
              select(CatalogSchemaVersionModel).filter_by(object_id="obj-1").order_by(CatalogSchemaVersionModel.version)
          )).scalars().all()
      assert len(rows) == 2  # v1 (no tag) + v2 (with tag applied)
      assert rows[0].snapshot_json[0].get("policy_tag") is None  # v1 unchanged
      assert rows[1].snapshot_json[0]["policy_tag"] == "PII"      # v2 with tag
  ```

- [ ] **Step 2: Verify tests fail**
  ```bash
  uv run pytest tests/unit/infrastructure/adapters/catalog/test_database_catalog_adapter.py -v
  ```
  Expected: `ModuleNotFoundError` for `database_catalog_adapter`.

- [ ] **Step 3: Implement `DatabaseCatalogAdapter`**

  Create `app/infrastructure/adapters/catalog/database_catalog_adapter.py`:
  ```python
  from __future__ import annotations

  import logging

  from sqlalchemy import desc, select
  from sqlalchemy.ext.asyncio import async_sessionmaker

  from app.application.shared.adapters.catalog_adapter import CatalogPublishError
  from app.domain.assets.data_asset import DataAsset
  from app.domain.discovery.schema_snapshot import SchemaSnapshot
  from app.domain.lineage.lineage_mapping import LineageMapping
  from app.infrastructure.persistence.models.catalog_lineage_model import CatalogLineageModel
  from app.infrastructure.persistence.models.catalog_schema_version_model import CatalogSchemaVersionModel

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
                      snapshot.object_id, latest.version,
                  )
                  return

              next_version = (latest.version + 1) if latest else 1
              session.add(CatalogSchemaVersionModel(
                  object_id=snapshot.object_id,
                  version=next_version,
                  snapshot_json=incoming,
              ))
              await session.commit()
              logger.info(
                  "publish_schema: saved v%d for object_id=%s (%d fields).",
                  next_version, snapshot.object_id, len(incoming),
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
              query = select(CatalogLineageModel).filter_by(
                  pipeline_id=mapping.pipeline_id,
                  source_object_id=mapping.source_object_id,
                  destination_object_id=mapping.destination_object_id,
              )
              existing = (await session.execute(query)).scalar_one_or_none()

              if existing:
                  existing.column_mappings = serialized
              else:
                  session.add(CatalogLineageModel(
                      pipeline_id=mapping.pipeline_id,
                      source_object_id=mapping.source_object_id,
                      destination_object_id=mapping.destination_object_id,
                      column_mappings=serialized,
                  ))
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
                  logger.warning("update_policy_tags: no schema version found for object_id=%s. Skipped.", object_id)
                  return

              updated_fields = [
                  {**col, "policy_tag": policy_tags[col["name"]]}
                  if col["name"] in policy_tags
                  else col
                  for col in latest.snapshot_json
              ]

              session.add(CatalogSchemaVersionModel(
                  object_id=object_id,
                  version=latest.version + 1,
                  snapshot_json=updated_fields,
              ))
              await session.commit()

      @staticmethod
      async def _latest_version(session, object_id: str) -> CatalogSchemaVersionModel | None:
          """Retrieves the highest-version schema record for a given object."""
          query = (
              select(CatalogSchemaVersionModel)
              .filter_by(object_id=object_id)
              .order_by(desc(CatalogSchemaVersionModel.version))
              .limit(1)
          )
          return (await session.execute(query)).scalar_one_or_none()
  ```

- [ ] **Step 4: Verify all tests pass**
  ```bash
  uv run pytest tests/unit/infrastructure/adapters/catalog/test_database_catalog_adapter.py -v
  ```
  Expected: 6 tests PASS.

- [ ] **Step 5: Commit**
  ```bash
  git add app/infrastructure/adapters/catalog/database_catalog_adapter.py \
          tests/unit/infrastructure/adapters/catalog/test_database_catalog_adapter.py
  git commit -m "feat(catalog): implement DatabaseCatalogAdapter with immutable versioning"
  ```

---

### Task 3: Wiring Settings and Factory

**Files:**
- Modify: `app/config.py`
- Create: `app/infrastructure/adapters/catalog/catalog_factory.py`
- Modify: `app/infrastructure/http/routers/asset_router.py`

**Interfaces:**
- Consumes: `Settings` (reads `catalog_adapter`, `datahub_url`, `datahub_token`, `openmetadata_url`, `openmetadata_api_key`).
- Produces: `get_catalog_adapter(settings: Settings) -> CatalogAdapter`.

- [ ] **Step 1: Add catalog config fields to `Settings`**

  Modify `app/config.py` — add fields after `catalog_adapter`:
  ```python
      catalog_adapter: str = "noop"  # "noop" | "database" | "datahub" | "openmetadata"

      # DataHub settings (used only when catalog_adapter = "datahub")
      datahub_url: str = ""
      datahub_token: str = ""

      # OpenMetadata settings (used only when catalog_adapter = "openmetadata")
      openmetadata_url: str = ""
      openmetadata_api_key: str = ""
  ```

- [ ] **Step 2: Create the catalog factory**

  Create `app/infrastructure/adapters/catalog/catalog_factory.py`:
  ```python
  from __future__ import annotations

  from app.application.shared.adapters.catalog_adapter import CatalogAdapter
  from app.config import Settings
  from app.infrastructure.adapters.catalog.database_catalog_adapter import DatabaseCatalogAdapter
  from app.infrastructure.adapters.catalog.datahub_adapter import DataHubCatalogAdapter
  from app.infrastructure.adapters.catalog.noop_adapter import NoopCatalogAdapter
  from app.infrastructure.adapters.catalog.openmetadata_adapter import OpenMetadataCatalogAdapter
  from app.infrastructure.persistence.database import get_session_factory


  def get_catalog_adapter(settings: Settings) -> CatalogAdapter:
      """
      Factory that resolves the active CatalogAdapter from environment configuration.

      Follows the same pattern as get_secret_manager: zero hardcoded values,
      all configuration delegated to Settings (read from .env or environment variables).
      """
      adapter_name = settings.catalog_adapter.lower()

      if adapter_name == "database":
          return DatabaseCatalogAdapter(get_session_factory())

      if adapter_name == "datahub":
          if not settings.datahub_url:
              raise ValueError("PLATFORM_DATAHUB_URL must be set when using datahub adapter")
          return DataHubCatalogAdapter(gms_url=settings.datahub_url, token=settings.datahub_token or None)

      if adapter_name == "openmetadata":
          if not settings.openmetadata_url:
              raise ValueError("PLATFORM_OPENMETADATA_URL must be set when using openmetadata adapter")
          return OpenMetadataCatalogAdapter(
              server_url=settings.openmetadata_url,
              api_key=settings.openmetadata_api_key or None,
          )

      # Default: noop (safe for local dev and tests)
      return NoopCatalogAdapter()
  ```

- [ ] **Step 3: Inject via factory into `asset_router.py`**

  Modify `app/infrastructure/http/routers/asset_router.py`:
  ```python
  # Add these imports (remove the existing NoopCatalogAdapter import)
  from app.config import get_settings
  from app.infrastructure.adapters.catalog.catalog_factory import get_catalog_adapter

  # Replace inside register_asset:
      use_case = RegisterAssetUseCase(
          uow=uow,
          catalog=get_catalog_adapter(get_settings()),
          notifications=NoopNotificationAdapter(),
      )

  # Replace inside activate_asset:
      use_case = ActivateAssetUseCase(
          uow=uow,
          catalog=get_catalog_adapter(get_settings()),
          notifications=NoopNotificationAdapter(),
      )
  ```

- [ ] **Step 4: Run the full test suite to verify no regressions**
  ```bash
  uv run pytest tests/ -v
  ```
  Expected: All tests PASS.

- [ ] **Step 5: Commit**
  ```bash
  git add app/config.py \
          app/infrastructure/adapters/catalog/catalog_factory.py \
          app/infrastructure/http/routers/asset_router.py
  git commit -m "feat(catalog): wire DatabaseCatalogAdapter via generic catalog factory"
  ```
