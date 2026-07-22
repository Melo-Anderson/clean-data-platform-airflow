# app/infrastructure/discovery/rest_api_runner.py
from __future__ import annotations

import fnmatch
import logging
from datetime import UTC, datetime

import httpx

from app.application.discovery.discovery_runner import DiscoveryRunner
from app.application.shared.secret_manager_port import SecretManagerPort
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.endpoints.endpoint import Endpoint, RestApiEndpoint

logger = logging.getLogger(__name__)

# OpenAPI primitive type + format → platform normalized_type string
_OPENAPI_TYPE_MAP: dict[tuple[str, str], str] = {
    ("integer", ""): "integer",
    ("integer", "int32"): "integer",
    ("integer", "int64"): "bigint",
    ("number", ""): "float",
    ("number", "float"): "float",
    ("number", "double"): "decimal",
    ("boolean", ""): "boolean",
    ("string", ""): "string",
    ("string", "uuid"): "string",
    ("string", "date-time"): "timestamp",
    ("string", "date"): "date",
    ("string", "binary"): "bytes",
    ("array", ""): "json",
    ("object", ""): "json",
}

# Python runtime type → platform normalized_type string (payload sampling fallback)
_PYTHON_TYPE_MAP: dict[type, str] = {
    int: "integer",
    float: "float",
    bool: "boolean",
    str: "string",
    dict: "json",
    list: "json",
    bytes: "bytes",
}

_WRAPPER_KEYS = ("data", "items", "results", "records", "content")


class RestApiRunner(DiscoveryRunner):
    """
    DiscoveryRunner for REST API endpoints using httpx.

    Supports hybrid discovery:
    1. Inspects OpenAPI/Swagger specification (GET /openapi.json) if available.
    2. Falls back to HTTP GET payload sampling if OpenAPI spec is absent or
       if the target resource name is not found in the spec's components/schemas.

    Auth flow: resolves credentials from SecretManagerPort, then builds headers
    based on endpoint.auth_type ("bearer" | "api_key" | "basic").

    scope_include patterns select which resource paths (e.g. "customers", "orders")
    to discover. Each pattern is matched literally against schema names (OpenAPI)
    or used as the path segment for payload sampling.
    """

    def __init__(
        self,
        secret_manager: SecretManagerPort,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._secret_manager = secret_manager
        self._http_client = http_client or httpx.AsyncClient(timeout=10.0)

    async def _build_client(self, endpoint: RestApiEndpoint) -> httpx.AsyncClient:
        """Resolve credentials and return a pre-authenticated httpx.AsyncClient."""
        payload = await self._secret_manager.resolve(endpoint.credential_ref.path)
        token = payload.get("token", "")
        headers: dict[str, str] = {}

        if endpoint.auth_type == "bearer":
            headers["Authorization"] = f"Bearer {token}"
        elif endpoint.auth_type == "api_key":
            headers["x-api-key"] = token
        elif endpoint.auth_type == "basic":
            import base64

            user = payload.get("username", "")
            pwd = payload.get("password", "")
            encoded = base64.b64encode(f"{user}:{pwd}".encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"
        elif endpoint.auth_type == "":
            pass  # unauthenticated endpoint
        else:
            raise ValueError(
                f"Unsupported auth_type {endpoint.auth_type!r}. "
                "Expected: 'bearer', 'api_key', 'basic', or ''."
            )

        return httpx.AsyncClient(
            base_url=endpoint.base_url,
            headers=headers,
            timeout=self._http_client.timeout,
        )

    def _infer_fields_from_sample(self, resource_name: str, sample: dict) -> list[SchemaField]:
        """Infer SchemaFields from a single JSON object (one API response item)."""
        fields: list[SchemaField] = []
        for field_name, value in sample.items():
            py_type = type(value)
            normalized = _PYTHON_TYPE_MAP.get(py_type, "string")
            is_pk = field_name.lower() == "id"
            fields.append(
                SchemaField(
                    name=field_name,
                    source_type=f"python:{py_type.__name__}",
                    normalized_type=normalized,
                    nullable=True,
                    is_primary_key=is_pk,
                    extra={"resource": resource_name, "discovery_method": "payload_sampling"},
                )
            )
        return fields

    async def _snapshot_from_sample(
        self,
        endpoint: RestApiEndpoint,
        asset_id: str,
        resource_name: str,
        client: httpx.AsyncClient,
    ) -> SchemaSnapshot:
        """Fetch one page of /api/v1/{resource_name}, detect envelope, infer schema."""
        response = await client.get(f"/api/v1/{resource_name}")
        response.raise_for_status()
        data = response.json()

        # Unwrap standard envelope keys (JSON:API, envelope pattern)
        items = data
        matched_wrapper_key: str | None = None
        if isinstance(data, dict):
            for key in _WRAPPER_KEYS:
                if key in data and isinstance(data[key], list):
                    items = data[key]
                    matched_wrapper_key = key
                    break

        sample: dict = {}
        if isinstance(items, list) and items:
            sample = items[0]
        elif isinstance(items, dict):
            sample = items

        fields = self._infer_fields_from_sample(resource_name, sample)

        return SchemaSnapshot(
            object_id=asset_id,
            object_name=f"{endpoint.base_url}/{resource_name}",
            runner_type="rest_api",
            captured_at=datetime.now(UTC),
            row_count_estimate=None,
            fields=fields,
            extra={"discovery_method": "payload_sampling", "wrapper_key": matched_wrapper_key},
        )

    def _infer_fields_from_openapi(
        self,
        resource_name: str,
        properties: dict,
        required_fields: set[str],
    ) -> list[SchemaField]:
        """Map OpenAPI schema properties to platform SchemaFields."""
        fields: list[SchemaField] = []
        for field_name, prop in properties.items():
            openapi_type = prop.get("type", "string")
            openapi_format = prop.get("format", "")
            normalized = _OPENAPI_TYPE_MAP.get(
                (openapi_type, openapi_format),
                _OPENAPI_TYPE_MAP.get((openapi_type, ""), "string"),
            )
            is_pk = field_name.lower() == "id" or bool(prop.get("x-primary-key", False))
            is_nullable = field_name not in required_fields
            description = prop.get("description")
            fields.append(
                SchemaField(
                    name=field_name,
                    source_type=f"openapi:{openapi_type}({openapi_format})"
                    if openapi_format
                    else f"openapi:{openapi_type}",
                    normalized_type=normalized,
                    nullable=is_nullable,
                    is_primary_key=is_pk,
                    description=description,
                    extra={"resource": resource_name, "discovery_method": "openapi"},
                )
            )
        return fields

    async def _try_openapi_discovery(
        self,
        endpoint: RestApiEndpoint,
        asset_id: str,
        scope_include: list[str],
        client: httpx.AsyncClient,
    ) -> list[SchemaSnapshot] | None:
        """
        Attempt OpenAPI spec discovery.

        Returns a list of SchemaSnapshots if the spec is available and at least one
        schema name matches scope_include. Returns None if spec is missing (non-200).
        """
        response = await client.get("/openapi.json")
        if response.status_code != 200:
            logger.debug(
                "No OpenAPI spec at /openapi.json for %s (HTTP %d) — will fall back to sampling",
                endpoint.base_url,
                response.status_code,
            )
            return None

        spec = response.json()
        schemas: dict = spec.get("components", {}).get("schemas", {})
        snapshots: list[SchemaSnapshot] = []
        captured_at = datetime.now(UTC)

        for schema_name, schema_def in schemas.items():
            if not any(fnmatch.fnmatch(schema_name, pattern) for pattern in scope_include):
                continue
            properties: dict = schema_def.get("properties", {})
            required_set: set[str] = set(schema_def.get("required", []))
            fields = self._infer_fields_from_openapi(schema_name, properties, required_set)
            snapshots.append(
                SchemaSnapshot(
                    object_id=asset_id,
                    object_name=f"{endpoint.base_url}/components/schemas/{schema_name}",
                    runner_type="rest_api",
                    captured_at=captured_at,
                    row_count_estimate=None,
                    fields=fields,
                    extra={"discovery_method": "openapi", "schema_name": schema_name},
                )
            )

        return snapshots

    async def run(
        self,
        asset_id: str,
        scope_include: list[str],
        scope_exclude: list[str],
        endpoint: Endpoint,
    ) -> list[SchemaSnapshot]:
        """
        Discover schemas for all resources matching scope_include.

        Strategy:
        1. Build authenticated client.
        2. Attempt OpenAPI spec discovery at /openapi.json.
           - If spec is available: match scope_include patterns against schema names.
           - If spec is missing: fall back to payload sampling per scope_include resource.
        3. Apply scope_exclude patterns to filter out unwanted snapshots.
        """
        if not isinstance(endpoint, RestApiEndpoint):
            raise TypeError(
                f"RestApiRunner only supports RestApiEndpoint, got {type(endpoint).__name__}"
            )

        async with await self._build_client(endpoint) as client:
            # Strategy 1: OpenAPI spec
            snapshots = await self._try_openapi_discovery(endpoint, asset_id, scope_include, client)

            # Strategy 2: Payload sampling fallback (one request per scope_include pattern)
            if snapshots is None:
                snapshots = []
                for resource_name in scope_include:
                    try:
                        snapshot = await self._snapshot_from_sample(
                            endpoint, asset_id, resource_name, client
                        )
                        snapshots.append(snapshot)
                    except httpx.HTTPStatusError as exc:
                        logger.warning(
                            "Payload sampling failed for resource %r: HTTP %d",
                            resource_name,
                            exc.response.status_code,
                        )

        # Apply exclusions by object_name glob matching
        return [
            s
            for s in snapshots
            if not any(fnmatch.fnmatch(s.object_name, pat) for pat in scope_exclude)
        ]
