from __future__ import annotations

import fnmatch
import inspect
import logging
from datetime import UTC, datetime

from motor.motor_asyncio import AsyncIOMotorClient

from app.application.discovery.discovery_runner import DiscoveryRunner
from app.application.shared.secret_manager_port import SecretManagerPort
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.endpoints.endpoint import Endpoint, NoSqlEndpoint

logger = logging.getLogger(__name__)

# BSON type → platform normalized type
_BSON_TYPE_MAP: dict[str, str] = {
    "string": "string",
    "int": "integer",
    "long": "bigint",
    "double": "float",
    "decimal": "decimal",
    "bool": "boolean",
    "date": "timestamp",
    "binData": "bytes",
    "objectId": "string",
    "object": "json",
    "array": "json",
}

_PYTHON_TYPE_MAP: dict[type, str] = {
    str: "string",
    int: "integer",
    float: "float",
    bool: "boolean",
    dict: "json",
    list: "json",
    bytes: "bytes",
}

_SAMPLE_SIZE = 100
_SERVER_SELECTION_TIMEOUT_MS = 5_000


class MongoDbRunner(DiscoveryRunner):
    """
    DiscoveryRunner for MongoDB using motor.
    """

    def __init__(self, secret_manager: SecretManagerPort) -> None:
        self._secret_manager = secret_manager

    async def run(
        self,
        asset_id: str,
        scope_include: list[str],
        scope_exclude: list[str],
        endpoint: Endpoint,
    ) -> list[SchemaSnapshot]:
        if not isinstance(endpoint, NoSqlEndpoint):
            raise TypeError(
                f"MongoDbRunner only supports NoSqlEndpoint, got {type(endpoint).__name__}"
            )

        payload = await self._secret_manager.resolve(endpoint.credential_ref.path)
        uri: str = payload["uri"]

        client: AsyncIOMotorClient = AsyncIOMotorClient(
            uri, serverSelectionTimeoutMS=_SERVER_SELECTION_TIMEOUT_MS
        )
        try:
            db_name = _extract_db_name(uri)
            db = client[db_name]
            return await self._reflect_all_collections(
                db, db_name, asset_id, scope_include, scope_exclude or []
            )
        finally:
            client.close()

    async def _reflect_all_collections(
        self,
        db: object,
        db_name: str,
        asset_id: str,
        scope_include: list[str],
        scope_exclude: list[str],
    ) -> list[SchemaSnapshot]:
        snapshots: list[SchemaSnapshot] = []
        captured_at = datetime.now(UTC)

        cols_raw = db.list_collections()  # type: ignore
        cursor = await cols_raw if inspect.isawaitable(cols_raw) else cols_raw
        async for col_info in cursor:
            name: str = col_info["name"]
            full_name = f"{db_name}.{name}"

            if not (
                _matches_scope(full_name, scope_include) or _matches_scope(name, scope_include)
            ):
                continue
            if _matches_any(full_name, scope_exclude) or _matches_any(name, scope_exclude):
                logger.debug("Skipping collection %r (excluded by scope_exclude)", name)
                continue

            snapshot = await self._reflect_collection(
                db, db_name, name, asset_id, captured_at, col_info
            )
            snapshots.append(snapshot)

        return snapshots

    async def _reflect_collection(
        self,
        db: object,
        db_name: str,
        name: str,
        asset_id: str,
        captured_at: datetime,
        col_info: dict,
    ) -> SchemaSnapshot:
        fields: list[SchemaField] = []
        validator = col_info.get("options", {}).get("validator", {}).get("$jsonSchema")

        if validator:
            logger.debug("Using $jsonSchema validator for collection %r", name)
            fields = _fields_from_json_schema(validator)
        else:
            logger.debug("No validator found for %r — falling back to $sample", name)
            # mypy thinks db[name] is Any, so we use it dynamically
            collection = getattr(db, name) if hasattr(db, name) else db[name]  # type: ignore
            fields = await _fields_from_sample(collection)

        try:
            collection = getattr(db, name) if hasattr(db, name) else db[name]  # type: ignore
            row_count: int | None = await collection.estimated_document_count()
        except Exception:
            logger.debug("Could not estimate document count for %r", name, exc_info=True)
            row_count = None

        indexes_metadata: list[dict] = []
        try:
            collection = getattr(db, name) if hasattr(db, name) else db[name]  # type: ignore
            raw_indexes = collection.list_indexes()
            cursor = await raw_indexes if inspect.isawaitable(raw_indexes) else raw_indexes
            async for idx in cursor:
                idx_name = idx.get("name", "")
                key_dict = idx.get("key", {})
                cols = (
                    list(key_dict.keys())
                    if isinstance(key_dict, dict)
                    else [k[0] for k in key_dict]
                )
                unique = bool(idx.get("unique", False))
                indexes_metadata.append({"name": idx_name, "columns": cols, "unique": unique})
        except Exception:
            logger.debug("Could not extract indexes for collection %r", name, exc_info=True)

        snapshot_extra = {
            "indexes": indexes_metadata,
            "foreign_keys": [],
            "partition_key": None,
        }

        return SchemaSnapshot(
            object_id="",
            object_name=f"{db_name}.{name}",
            runner_type="mongodb",
            captured_at=captured_at,
            row_count_estimate=row_count,
            fields=fields,
            extra=snapshot_extra,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_db_name(uri: str) -> str:
    """Extract the database name from a MongoDB URI (last path segment)."""
    from urllib.parse import urlparse

    parsed = urlparse(uri)
    db_name = parsed.path.lstrip("/")
    if not db_name:
        raise ValueError(
            f"MongoDB URI must include a database name (e.g. mongodb://host/dbname). Got: {uri!r}"
        )
    return db_name


def _matches_scope(name: str, patterns: list[str]) -> bool:
    """Return True if `name` matches any of the include glob patterns."""
    return any(fnmatch.fnmatch(name, p) for p in patterns)


def _matches_any(name: str, patterns: list[str]) -> bool:
    """Return True if `name` matches any of the exclusion glob patterns."""
    return any(fnmatch.fnmatch(name, p) for p in patterns)


def _fields_from_json_schema(json_schema: dict) -> list[SchemaField]:
    """Parse a MongoDB $jsonSchema validator into a list of SchemaFields."""
    properties: dict = json_schema.get("properties", {})
    required_set: set[str] = set(json_schema.get("required", []))
    fields: list[SchemaField] = []

    for field_name, field_def in properties.items():
        bson_type = field_def.get("bsonType", "string")
        if isinstance(bson_type, list):
            # Pick the first non-null type
            bson_type = next((t for t in bson_type if t != "null"), "string")
        normalized = _BSON_TYPE_MAP.get(bson_type, "string")
        fields.append(
            SchemaField(
                name=field_name,
                source_type=f"bson:{bson_type}",
                normalized_type=normalized,
                nullable=field_name not in required_set,
            )
        )

    return fields


async def _fields_from_sample(collection: object) -> list[SchemaField]:
    """Infer schema from a random $sample of 100 documents."""
    union: dict[str, type] = {}

    async for doc in collection.aggregate([{"$sample": {"size": _SAMPLE_SIZE}}]):  # type: ignore
        for key, value in doc.items():
            if key == "_id":
                continue
            existing = union.get(key)
            py_type = type(value)
            if existing is None:
                union[key] = py_type
            elif existing != py_type:
                # Conflicting types across documents → widen to string
                union[key] = str

    return [
        SchemaField(
            name=field_name,
            source_type=f"python:{py_type.__name__}",
            normalized_type=_PYTHON_TYPE_MAP.get(py_type, "string"),
            nullable=True,
        )
        for field_name, py_type in union.items()
    ]
