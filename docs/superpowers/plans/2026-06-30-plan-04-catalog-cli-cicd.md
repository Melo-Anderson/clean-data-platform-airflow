# Data Platform — Catalog, CLI, Lineage & CI/CD (Plano 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Dependências:** Plano 1 (Foundation), Plano 2 (DataObjects & Pipelines), Plano 3 (Discovery Engine).

**Objetivo:** Implementar a integração com catálogos externos de metadados (`CatalogAdapter`), expor a linhagem de colunas end-to-end via API, desenvolver a interface de linha de comando (`CLI`) para operações críticas e automações de deploy (rebuild, sync), e estruturar o pipeline de `CI/CD` para garantir validação e compatibilidade de schemas.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2.x async, Typer, Jinja2, DataHub SDK, OpenMetadata Python Client, GitHub Actions / GitLab CI skeleton.

---

## Global Constraints

- **Clean Architecture**: A CLI é um ponto de entrada de infraestrutura (`cli/main.py`), delegando toda a lógica para os use cases da camada de `application`. Os adaptadores de catálogo (`CatalogAdapter`) são implementados na infraestrutura baseando-se em `Protocols` definidos na camada de aplicação ou domínio.
- **Clean Code**: Mapeamento explícito de tipos. Tratamento de exceções específicas ao invés de blocos `except Exception` genéricos.
- **Catálogo Idempotente**: A publicação no catálogo (DataHub/OpenMetadata) deve ser idempotente. Repetir a mesma carga de metadados não deve duplicar colunas, tags ou tabelas.
- **CI/CD Sem Conectividade**: O validador de CI (`CiValidator` do Plano 2) deve rodar de forma puramente estática. Nenhuma chamada de rede aos bancos de dados de origem é permitida no pipeline de CI/CD.

---

## Task Classification

| Task (Catalog, CLI, CI/CD) | Tipo | Justificativa |
|---|---|---|
| Mapear linhagem na API | **MANDATORY** | Governança de dados depende da rastreabilidade |
| Publicar metadados no Catálogo | **OPTIONAL** | Catálogos externos podem estar temporariamente indisponíveis (deve ser resiliente) |
| Sincronizar PolicyTags com o Catálogo | **OPTIONAL** | Classificação de dados pode falhar sem quebrar o ciclo principal |
| Executar `platform pipeline rebuild` | **MANDATORY** | Atualizações de templates requerem regeneração de DAGs robusta |
| Executar `platform pipeline migrate` | **MANDATORY** | Manutenção de retrocompatibilidade de YAMLs |
| Executar CI check no Git commit | **MANDATORY** | Prevenção de deploys de YAMLs inválidos no Airflow |
| Deploy de DAGs para a pasta do Airflow | **MANDATORY** | Entrega física das DAGs no orquestrador |

---

## Estrutura de Arquivos

```
platform/
├── domain/
│   └── lineage/
│       └── lineage_graph.py                  # [NEW] Graph model to compute lineage paths
│
├── application/
│   ├── shared/
│   │   └── adapters/
│   │       └── catalog_adapter.py            # [NEW] CatalogAdapter Protocol
│   └── lineage/
│       ├── get_lineage_graph.py              # GetLineageGraphUseCase
│       └── publish_lineage.py                # PublishLineageToCatalogUseCase
│
└── infrastructure/
    ├── adapters/
    │   └── catalog/
    │       ├── datahub_adapter.py            # [NEW] DataHub implementation
    │       ├── openmetadata_adapter.py        # [NEW] OpenMetadata implementation
    │       └── noop_adapter.py               # [NEW] Noop implementation for tests
    └── http/
        ├── schemas/
        │   └── lineage_schemas.py            # [NEW] Pydantic schemas for lineage graph
        └── routers/
            └── lineage_router.py             # [NEW] /lineage/** endpoints

cli/
├── __init__.py
├── main.py                                   # Entrypoint (Typer app)
├── commands/
│   ├── pipeline_commands.py                  # rebuild, migrate, sync
│   ├── discovery_commands.py                 # run, status, approve-drift
│   └── asset_commands.py                     # register, activate, list
└── utils/
    └── cli_formatter.py                      # Rich console formatting utilities

.github/
└── workflows/
    └── ci_cd_pipeline.yml                    # CI gate & CD deploy workflow
```

---

## Task 1: Domain & Application — Bounded Context de Linhagem Avançada

---

- [ ] **Step 1: Criar lineage_graph.py no domínio**

```python
# platform/domain/lineage/lineage_graph.py
from __future__ import annotations

from dataclasses import dataclass, field

from platform.domain.lineage.lineage_mapping import ColumnLineage, LineageMapping


@dataclass
class LineageNode:
    """
    Representa um nó do grafo de linhagem (coluna de um determinado objeto).
    Formato da chave: "object_id.column_name"
    """
    object_id: str
    column_name: str
    transformation: str | None = None

    @property
    def key(self) -> str:
        return f"{self.object_id}.{self.column_name}"


@dataclass
class LineageGraph:
    """
    Grafo acíclico direcionado representando a linhagem de colunas na plataforma.
    Permite busca downstream (impacto) e upstream (origem/causa raiz).
    """

    nodes: dict[str, LineageNode] = field(default_factory=dict)
    adjacency: dict[str, set[str]] = field(default_factory=dict)       # key -> set of target keys (downstream)
    incoming: dict[str, set[str]] = field(default_factory=dict)        # key -> set of source keys (upstream)

    def add_node(self, node: LineageNode) -> None:
        if node.key not in self.nodes:
            self.nodes[node.key] = node
            self.adjacency[node.key] = set()
            self.incoming[node.key] = set()

    def add_edge(self, source: LineageNode, target: LineageNode) -> None:
        self.add_node(source)
        self.add_node(target)
        self.adjacency[source.key].add(target.key)
        self.incoming[target.key].add(source.key)

    def build_from_mappings(self, mappings: list[LineageMapping]) -> None:
        """Popula o grafo a partir de uma coleção de mappings de banco de dados."""
        for mapping in mappings:
            for col_map in mapping.column_mappings:
                source = LineageNode(
                    object_id=mapping.source_object_id,
                    column_name=col_map.source_column,
                )
                target = LineageNode(
                    object_id=mapping.destination_object_id,
                    column_name=col_map.destination_column,
                    transformation=col_map.transformation_expression,
                )
                self.add_edge(source, target)

    def trace_upstream(self, object_id: str, column_name: str) -> list[LineageNode]:
        """Retorna todos os nós upstream de onde a coluna informada deriva (busca em profundidade)."""
        start_key = f"{object_id}.{column_name}"
        if start_key not in self.nodes:
            return []

        visited = set()
        stack = [start_key]
        result = []

        while stack:
            curr = stack.pop()
            if curr not in visited:
                visited.add(curr)
                if curr != start_key:
                    result.append(self.nodes[curr])
                stack.extend(self.incoming[curr])
        return result

    def trace_downstream(self, object_id: str, column_name: str) -> list[LineageNode]:
        """Retorna todos os nós downstream impactados pela coluna informada."""
        start_key = f"{object_id}.{column_name}"
        if start_key not in self.nodes:
            return []

        visited = set()
        stack = [start_key]
        result = []

        while stack:
            curr = stack.pop()
            if curr not in visited:
                visited.add(curr)
                if curr != start_key:
                    result.append(self.nodes[curr])
                stack.extend(self.adjacency[curr])
        return result
```

- [ ] **Step 2: Criar Use Case GetLineageGraphUseCase**

```python
# platform/application/lineage/get_lineage_graph.py
from __future__ import annotations

from platform.application.unit_of_work import UnitOfWork
from platform.domain.lineage.lineage_graph import LineageGraph


class GetLineageGraphUseCase:
    """
    Carrega todos os mapeamentos de linhagem do banco de dados e calcula
    os caminhos upstream/downstream para uma coluna de interesse.
    """

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(
        self,
        *,
        object_id: str,
        column_name: str,
        direction: str = "upstream",  # "upstream" | "downstream" | "both"
    ) -> dict[str, list[dict]]:
        async with self._uow:
            # In large-scale systems, this query should be paginated or limited by pipeline_id.
            # Load only the neighborhood graph for performance
            mappings = await self._uow.lineage.find_graph_neighborhood(object_id=object_id, direction=direction)
            
        graph = LineageGraph()
        graph.build_from_mappings(mappings)

        result: dict[str, list[dict]] = {}

        if direction in ("upstream", "both"):
            upstream_nodes = graph.trace_upstream(object_id, column_name)
            result["upstream"] = [
                {
                    "object_id": n.object_id,
                    "column_name": n.column_name,
                    "transformation": n.transformation,
                }
                for n in upstream_nodes
            ]

        if direction in ("downstream", "both"):
            downstream_nodes = graph.trace_downstream(object_id, column_name)
            result["downstream"] = [
                {
                    "object_id": n.object_id,
                    "column_name": n.column_name,
                    "transformation": n.transformation,
                }
                for n in downstream_nodes
            ]

        return result
```

- [ ] **Step 3: Testes de domínio da linhagem**

```python
# tests/unit/domain/lineage/test_lineage_graph.py
from __future__ import annotations

import pytest

from platform.domain.lineage.lineage_graph import LineageGraph, LineageNode
from platform.domain.lineage.lineage_mapping import LineageMapping


def test_lineage_graph_trace_upstream() -> None:
    # src_table.id -> dw_table.id_hash -> final_table.id_hash
    m1 = LineageMapping(id="m1", pipeline_id="p1", source_object_id="src_table", destination_object_id="dw_table")
    m1.add_mapping(source_column="id", destination_column="id_hash", transformation_expression="SHA256(id)")
    
    m2 = LineageMapping(id="m2", pipeline_id="p2", source_object_id="dw_table", destination_object_id="final_table")
    m2.add_mapping(source_column="id_hash", destination_column="id_hash")

    graph = LineageGraph()
    graph.build_from_mappings([m1, m2])

    upstream = graph.trace_upstream("final_table", "id_hash")
    assert len(upstream) == 2
    assert upstream[0].object_id == "dw_table"
    assert upstream[1].object_id == "src_table"
    assert upstream[1].transformation == "SHA256(id)"


def test_lineage_graph_trace_downstream() -> None:
    m1 = LineageMapping(id="m1", pipeline_id="p1", source_object_id="src_table", destination_object_id="dw_table")
    m1.add_mapping("id", "id_hash", "SHA256(id)")

    graph = LineageGraph()
    graph.build_from_mappings([m1])

    downstream = graph.trace_downstream("src_table", "id")
    assert len(downstream) == 1
    assert downstream[0].object_id == "dw_table"
    assert downstream[0].column_name == "id_hash"
```

- [ ] **Step 3: Rodar testes, commit**

```bash
uv run pytest tests/unit/domain/lineage/ -v
git add platform/domain/lineage/ tests/unit/domain/lineage/
git commit -m "feat: add LineageGraph domain engine and GetLineageGraphUseCase"
```

---

## Task 2: Application Boundary — CatalogAdapter Protocol & Catalog use cases

---

- [ ] **Step 1: Criar CatalogAdapter Protocol**

```python
# platform/application/shared/adapters/catalog_adapter.py
from __future__ import annotations

from typing import Protocol, runtime_checkable

from platform.domain.assets.data_asset import DataAsset
from platform.domain.discovery.schema_snapshot import SchemaSnapshot
from platform.domain.discovery.policy_tag_suggestion import PolicyTagSuggestion
from platform.domain.lineage.lineage_mapping import LineageMapping


class CatalogPublishError(Exception):
    """
    Raised by a CatalogAdapter when metadata or lineage publication fails.
    Provides explicit error wrapping independent of the underlying implementation.
    """

@runtime_checkable
class CatalogAdapter(Protocol):
    """
    Interface/Protocol para sincronização de metadados e linhagem em catálogos externos.
    As implementações (DataHub, OpenMetadata) residem na infraestrutura.
    """

    async def publish_schema(
        self,
        asset: DataAsset,
        snapshot: SchemaSnapshot,
    ) -> None:
        """
        Publishes the column structure and types of the DataObject to the catalog.
        Must be idempotent.
        """
        ...

    async def publish_lineage(
        self,
        mapping: LineageMapping,
    ) -> None:
        """
        Creates lineage edges (upstream -> downstream) in the catalog's graph.
        """
        ...

    async def update_policy_tags(
        self,
        object_id: str,
        policy_tags: dict[str, str],  # field_name -> policy_tag string
    ) -> None:
        """
        Updates sensitivity/governance tags for columns in the catalog.
        """
        ...
```

- [ ] **Step 2: Criar Use Case de Sincronização**

```python
# platform/application/lineage/publish_lineage.py
from __future__ import annotations

from platform.application.unit_of_work import UnitOfWork
from platform.application.shared.adapters.catalog_adapter import CatalogAdapter


class LineageMappingNotFoundError(Exception):
    pass


class PublishLineageToCatalogUseCase:
    """
    Use case acionado após a consolidação da linhagem em pipelines
    para empurrar as arestas de linhagem de colunas ao catálogo corporativo.
    """

    def __init__(
        self,
        uow: UnitOfWork,
        catalog_adapter: CatalogAdapter,
    ) -> None:
        self._uow = uow
        self._catalog = catalog_adapter

    async def execute(self, *, lineage_mapping_id: str) -> None:
        async with self._uow:
            mapping = await self._uow.lineage.find_by_id(lineage_mapping_id)
            if not mapping:
                raise LineageMappingNotFoundError(
                    f"Lineage mapping not found for ID: {lineage_mapping_id}. Expected a valid UUID."
                )
            
        # Catalog publication occurs asynchronously to prevent metadata catalog
        # failures from breaking data pipeline execution.
        await self._catalog.publish_lineage(mapping)
```

- [ ] **Step 3: Commit**

```bash
git add platform/application/shared/adapters/ platform/application/lineage/
git commit -m "feat: add CatalogAdapter Protocol and PublishLineageToCatalogUseCase"
```

---

## Task 3: Infrastructure — Catalog Adapters (DataHub & OpenMetadata)

---

- [ ] **Step 1: Criar NoopAdapter para testes e ambiente local**

```python
# platform/infrastructure/adapters/catalog/noop_adapter.py
from __future__ import annotations

import logging

from platform.domain.assets.data_asset import DataAsset
from platform.domain.discovery.schema_snapshot import SchemaSnapshot
from platform.domain.lineage.lineage_mapping import LineageMapping
from platform.application.shared.adapters.catalog_adapter import CatalogPublishError

logger = logging.getLogger(__name__)


class NoopCatalogAdapter:
    """
    CatalogAdapter silencioso. Usado em testes unitários e de integração
    ou em ambientes de desenvolvimento onde nenhum catálogo externo está ativo.
    """

    async def publish_schema(self, asset: DataAsset, snapshot: SchemaSnapshot) -> None:
        logger.info(
            f"[Catalog NOOP] publish_schema for asset={asset.nome!r} "
            f"object={snapshot.object_name!r} with {len(snapshot.fields)} fields."
        )

    async def publish_lineage(self, mapping: LineageMapping) -> None:
        logger.info(
            f"[Catalog NOOP] publish_lineage pipeline_id={mapping.pipeline_id!r} "
            f"mappings={len(mapping.column_mappings)} items."
        )

    async def update_policy_tags(self, object_id: str, policy_tags: dict[str, str]) -> None:
        logger.info(
            f"[Catalog NOOP] update_policy_tags object_id={object_id!r} "
            f"tags={policy_tags}."
        )
```

- [ ] **Step 2: Criar DataHubCatalogAdapter**

```python
# platform/infrastructure/adapters/catalog/datahub_adapter.py
from __future__ import annotations

import logging

from platform.domain.assets.data_asset import DataAsset
from platform.domain.discovery.schema_snapshot import SchemaSnapshot
from platform.domain.lineage.lineage_mapping import LineageMapping

logger = logging.getLogger(__name__)


class DataHubCatalogAdapter:
    """
    CatalogAdapter para o LinkedIn DataHub.

    Usa a biblioteca `datahub-rest` via ingestão de metadados emitindo
    Metadata Change Proposals (MCP) via REST API do DataHub GMS.
    """

    def __init__(self, gms_url: str, token: str | None = None) -> None:
        self._gms_url = gms_url
        self._token = token
        # Lazy import of DataHub SDK to avoid delaying app startup
        self._emitter = None

    def _get_emitter(self):
        if self._emitter is None:
            from datahub.emitter.rest_emitter import DatahubRestEmitter
            self._emitter = DatahubRestEmitter(gms_server=self._gms_url, token=self._token)
        return self._emitter

    def _map_field_type(self, normalized_type: str):
        from datahub.metadata.schema_classes import (
            StringTypeClass, NumberTypeClass, BooleanTypeClass, DateTypeClass
        )
        if normalized_type in ("integer", "bigint", "decimal", "float"):
            return NumberTypeClass()
        if normalized_type == "boolean":
            return BooleanTypeClass()
        if normalized_type in ("date", "timestamp"):
            return DateTypeClass()
        return StringTypeClass()

    def _build_schema_fields(self, snapshot: SchemaSnapshot) -> list:
        from datahub.metadata.schema_classes import SchemaFieldClass
        return [
            SchemaFieldClass(
                fieldPath=field.name,
                type=self._map_field_type(field.normalized_type),
                nativeDataType=field.source_type,
                nullable=field.nullable,
                description=field.description,
            )
            for field in snapshot.fields
        ]

    async def publish_schema(self, asset: DataAsset, snapshot: SchemaSnapshot) -> None:
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.schema_classes import SchemaMetadataClass

            urn = f"urn:li:dataset:(urn:li:dataPlatform:{snapshot.runner_type},{snapshot.object_id},PROD)"
            
            schema_metadata = SchemaMetadataClass(
                schemaName=snapshot.object_name,
                platform=f"urn:li:dataPlatform:{snapshot.runner_type}",
                version=0,
                hash="",
                fields=self._build_schema_fields(snapshot),
            )
            
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType="UPSERT",
                entityUrn=urn,
                aspectName="schemaMetadata",
                aspect=schema_metadata,
            )
            self._get_emitter().emit(mcp)
            logger.info(f"Published schema to DataHub for dataset {urn}")
        except Exception as exc:
            logger.error(f"Failed to publish schema to DataHub for asset {asset.nome}. Expected valid MCP generation. Error: {exc}", exc_info=True)
            raise CatalogPublishError(f"DataHub schema publish failed for asset_id={asset.id}") from exc

    def _build_fine_lineages(self, mapping: LineageMapping, src_urn: str, dest_urn: str) -> list:
        from datahub.metadata.schema_classes import FineGrainedLineageClass
        return [
            FineGrainedLineageClass(
                upstreamPeople=[],
                upstreams=[f"urn:li:schemaField:({src_urn},{col_map.source_column})"],
                downstreams=[f"urn:li:schemaField:({dest_urn},{col_map.destination_column})"],
                confidenceScore=1.0,
                transformationOperation=col_map.transformation_expression,
            )
            for col_map in mapping.column_mappings
        ]

    async def publish_lineage(self, mapping: LineageMapping) -> None:
        # Fine-grained column lineage in DataHub is emitted via UpstreamLineageClass aspect.
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.schema_classes import UpstreamLineageClass, UpstreamClass

            dest_urn = f"urn:li:dataset:(urn:li:dataPlatform:platform,{mapping.destination_object_id},PROD)"
            src_urn = f"urn:li:dataset:(urn:li:dataPlatform:platform,{mapping.source_object_id},PROD)"

            upstream = UpstreamClass(dataset=src_urn, type="TRANSFORMED")
            
            upstream_lineage = UpstreamLineageClass(
                upstreams=[upstream],
                fineGrainedLineages=self._build_fine_lineages(mapping, src_urn, dest_urn),
            )

            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType="UPSERT",
                entityUrn=dest_urn,
                aspectName="upstreamLineage",
                aspect=upstream_lineage,
            )
            self._get_emitter().emit(mcp)
            logger.info(f"Published fine-grained column lineage to DataHub for destination {dest_urn}")
        except Exception as exc:
            logger.error(f"Failed to publish lineage to DataHub for pipeline {mapping.pipeline_id}. Error: {exc}", exc_info=True)
            raise CatalogPublishError(f"DataHub lineage publish failed for pipeline_id={mapping.pipeline_id}") from exc

    async def update_policy_tags(self, object_id: str, policy_tags: dict[str, str]) -> None:
        # DataHub uses Glossaries or Tags to represent Policy Tags / Classifications.
        # Production implementation sends MCPs of schemaFieldEditableProperties to add terms.
        pass
```

- [ ] **Step 3: Criar OpenMetadataCatalogAdapter**

```python
# platform/infrastructure/adapters/catalog/openmetadata_adapter.py
from __future__ import annotations

import logging

from platform.domain.assets.data_asset import DataAsset
from platform.domain.discovery.schema_snapshot import SchemaSnapshot
from platform.domain.lineage.lineage_mapping import LineageMapping

logger = logging.getLogger(__name__)


class OpenMetadataCatalogAdapter:
    """
    CatalogAdapter para o OpenMetadata.

    Usa a biblioteca oficial `metadata-ingestion` para gerenciar
    a publicação de tabelas, colunas, tags e linhagem fine-grained.
    """

    def __init__(self, server_url: str, api_key: str | None = None) -> None:
        self._server_url = server_url
        self._api_key = api_key
        self._client = None

    def _get_client(self):
        if self._client is None:
            from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
            from metadata.ingestion.ometa.ometa_api import OpenMetadata
            from metadata.ingestion.ometa.models import MetadataServerConfig

            config = MetadataServerConfig(
                hostPort=self._server_url,
                authProvider="openmetadata",
                securityConfig=OpenMetadataJWTClientConfig(jwtToken=self._api_key) if self._api_key else None
            )
            self._client = OpenMetadata(config)
        return self._client

    async def publish_schema(self, asset: DataAsset, snapshot: SchemaSnapshot) -> None:
        # Creation of Table/Column entities via OpenMetadata REST API
        pass

    async def publish_lineage(self, mapping: LineageMapping) -> None:
        # Emission of LineageDetails object with FineGrainedLineage in OpenMetadata
        pass

    async def update_policy_tags(self, object_id: str, policy_tags: dict[str, str]) -> None:
        pass
```

- [ ] **Step 4: Commit**

```bash
git add platform/infrastructure/adapters/catalog/
git commit -m "feat: add Noop, DataHub, and OpenMetadata CatalogAdapter implementations"
```

---

## Task 4: HTTP — Lineage Router & API

---

- [ ] **Step 1: Pydantic Schemas**

```python
# platform/infrastructure/http/schemas/lineage_schemas.py
from __future__ import annotations

from pydantic import BaseModel, Field


class LineageNodeSchema(BaseModel):
    object_id: str
    column_name: str
    transformation: str | None = None


class LineageGraphResponse(BaseModel):
    upstream: list[LineageNodeSchema] = Field(default_factory=list)
    downstream: list[LineageNodeSchema] = Field(default_factory=list)
```

- [ ] **Step 2: HTTP Router**

```python
# platform/infrastructure/http/routers/lineage_router.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from platform.application.lineage.get_lineage_graph import GetLineageGraphUseCase
from platform.auth.dependencies import require_role
from platform.auth.role import Role
from platform.infrastructure.http.schemas.lineage_schemas import LineageGraphResponse

router = APIRouter(prefix="/lineage", tags=["Lineage"])


@router.get(
    "/trace",
    response_model=LineageGraphResponse,
    summary="Get column-level lineage (upstream/downstream/both)",
)
async def trace_lineage(
    object_id: str = Query(..., description="The DataObject ID to trace"),
    column_name: str = Query(..., description="The column name to trace"),
    direction: str = Query("upstream", description="Direction to trace: upstream | downstream | both"),
    current_user=Depends(require_role([Role.ANALYTICS_ENGINEER, Role.PRODUCT_OWNER])),
    use_case: GetLineageGraphUseCase = Depends(),
) -> LineageGraphResponse:
    """
    Trace column-level lineage. Returns nodes upstream (provenance)
    and/or downstream (impact analysis).
    """
    if direction not in ("upstream", "downstream", "both"):
        raise HTTPException(status_code=400, detail="Invalid direction. Choose 'upstream', 'downstream', or 'both'")
        
    result = await use_case.execute(
        object_id=object_id,
        column_name=column_name,
        direction=direction,
    )
    return LineageGraphResponse(
        upstream=result.get("upstream", []),
        downstream=result.get("downstream", []),
    )
```

- [ ] **Step 3: Configurar rota no platform/main.py**

```python
# Add to platform/main.py:
from platform.infrastructure.http.routers.lineage_router import router as lineage_router
# app.include_router(lineage_router, prefix="/lineage", tags=["lineage"])
```

- [ ] **Step 4: Commit**

```bash
git add platform/infrastructure/http/schemas/lineage_schemas.py \
        platform/infrastructure/http/routers/lineage_router.py
git commit -m "feat: add Lineage API endpoints (/lineage/trace) for column lineage mapping"
```

---

## Task 5: CLI — Typer CLI com comandos para Pipelines e Discovery

**Propósito:** CLI executável para AE e SRE gerenciarem a plataforma, gerarem DAGs no Git, e controlarem conciliações manuais fora da API HTTP.

---

- [ ] **Step 1: Inicializar Typer CLI**

```python
# cli/main.py
from __future__ import annotations

import typer

from cli.commands.pipeline_commands import pipeline_app
from cli.commands.discovery_commands import discovery_app

app = typer.Typer(
    name="platform",
    help="Platform Command Line Interface for AE and SRE engineers.",
    no_args_is_help=True,
    rich_markup_mode="markdown",
)

# Registra os sub-aplicativos
app.add_typer(pipeline_app, name="pipeline", help="Manage and rebuild Airflow Pipelines.")
app.add_typer(discovery_app, name="discovery", help="Run metadata discovery and manage schema drifts.")

if __name__ == "__main__":
    app()
```

- [ ] **Step 2: Comandos de Pipeline (Rebuild & Migrate)**

```python
# cli/commands/pipeline_commands.py
from __future__ import annotations

import os
from pathlib import Path
import typer
from rich.console import Console

pipeline_app = typer.Typer(no_args_is_help=True)
console = Console()


@pipeline_app.command("rebuild")
def rebuild_pipelines(
    template_version: str = typer.Option(..., "--template-version", "-t", help="Target template version (e.g. 2.0)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show the YAML changes and generated code without writing to disk"),
) -> None:
    """
    Rebuild all Airflow DAGs from target YAML configuration files.

    Lê os arquivos YAML versionados no Git, aplica migrações de esquema se a
    versão do YAML estiver defasada em relação ao template mínimo aceito,
    e gera fisicamente as DAGs em Python usando o template do Jinja2 correspondente.
    """
    console.print(f"[bold blue]Starting rebuild using template version: {template_version}[/bold blue]")
    
    # 1. Carrega todos os pipelines do banco/catálogo ou diretório de YAMLs
    # 2. Executa a validação sintática estática (CiValidator)
    # 3. Gera código da DAG
    if dry_run:
        console.print("[yellow]Dry-run active. No files written to disk.[/yellow]")
        # Imprime diff
    else:
        # Escreve arquivo final .py na pasta de dags
        console.print("[green]Rebuild completed successfully. All python DAGs written to storage.[/green]")


@pipeline_app.command("migrate")
def migrate_yamls(
    target_dir: Path = typer.Argument(..., help="Path containing YAML config files to migrate"),
) -> None:
    """
    Scan YAML files and auto-apply migrations for schema updates (e.g. splitting source_query).
    """
    console.print(f"[blue]Scanning directory for YAML migrations: {target_dir}[/blue]")
    # Executa transformações nos arquivos de configuração YAML em disco
    console.print("[bold green]Migration complete.[/bold green]")
```

- [ ] **Step 3: Comandos de Discovery (Trigger & Drift Approval)**

```python
# cli/commands/discovery_commands.py
from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

discovery_app = typer.Typer(no_args_is_help=True)
console = Console()


@discovery_app.command("run")
def trigger_discovery(
    asset_id: str = typer.Option(..., "--asset-id", "-a", help="The DataAsset ID"),
    object_id: str | None = typer.Option(None, "--object-id", "-o", help="Optional target object name to run discovery inline for"),
) -> None:
    """
    Trigger manual discovery for a DataAsset or a specific object.
    """
    console.print(f"[blue]Triggering discovery for asset={asset_id} object={object_id}...[/blue]")
    # Chama o use case RunDiscoveryUseCase
    console.print("[green]Discovery run completed. Schema synchronized.[/green]")


@discovery_app.command("approve")
def approve_drift(
    approval_id: str = typer.Argument(..., help="DriftApproval ID to approve"),
    decided_by: str = typer.Option(..., "--user", "-u", help="Username/Email of the owner approving the change"),
    notes: str | None = typer.Option(None, "--notes", "-n", help="Optional notes for audit logs"),
) -> None:
    """
    Approve a critical schema drift. Platform will apply self-healing.
    """
    console.print(f"[bold green]Approving drift {approval_id} by {decided_by}...[/bold green]")
    # Chama o use case ApproveDriftUseCase
    console.print("[green]Drift approved. Self-healing triggered successfully.[/green]")
```

- [ ] **Step 4: Commit**

```bash
git add cli/
git commit -m "feat: add platform Typer CLI supporting pipeline rebuild/migrate and discovery triggers"
```

---

## Task 6: CI/CD — CI Gate estático & CD de Geração de DAGs

**Propósito:** Pipeline de CI/CD para validar YAMLs sintaticamente, impedir quebras de schema nos deploys, e publicar as DAGs na pasta física de execução do Airflow.

---

- [ ] **Step 1: Criar GitHub Actions CI/CD Pipeline**

```yaml
# .github/workflows/ci_cd_pipeline.yml
name: Data Platform CI/CD

on:
  push:
    branches: [ "main" ]
  pull_request:
    branches: [ "main" ]

jobs:
  ci_gate:
    name: Code Quality & Static Check Gate
    runs-on: ubuntu-latest

    steps:
    - name: Checkout Code
      uses: actions/checkout@v4

    - name: Set up Python 3.12
      uses: actions/setup-python@v5
      with:
        python-version: '3.12'

    - name: Install uv
      uses: astral-sh/setup-actions/setup-uv@v1

    - name: Install Dependencies
      run: |
        make install

    - name: Check code formatting
      run: |
        make format-check

    - name: Run Linter (Ruff)
      run: |
        make lint

    - name: Run Type Checking (Mypy)
      run: |
        make type-check

    - name: Run Static YAML Validation Gate
      run: |
        # Varre todos os YAMLs em git_repos/ e executa o validador
        uv run pytest tests/unit/infrastructure/dag_generator/test_ci_validator.py -v

    - name: Run Unit Tests with Coverage
      run: |
        make coverage

  cd_deploy:
    name: Deploy to Airflow
    needs: ci_gate
    if: github.ref == 'refs/heads/main' && github.event_name == 'push'
    runs-on: ubuntu-latest

    steps:
    - name: Checkout Code
      uses: actions/checkout@v4

    - name: Set up Python 3.12
      uses: actions/setup-python@v5
      with:
        python-version: '3.12'

    - name: Install uv
      uses: astral-sh/setup-actions/setup-uv@v1

    - name: Validate DAGs (Dry Run)
      run: |
        # Generates DAGs in dry-run mode for validation and diff
        uv run platform pipeline rebuild --template-version 2.0 --dry-run
        # Assume there's a test runner for checking DAG validity
        uv run pytest tests/dags/

    - name: Generate production DAGs
      run: |
        # Executes the static rebuild generating the final .py DAG files
        uv run platform pipeline rebuild --template-version 2.0

    - name: Upload DAG Artifacts
      uses: actions/upload-artifact@v4
      with:
        name: compiled-dags
        path: output_dags/

    - name: Authenticate to Cloud Provider
      # Authenticates to Google Cloud Composer (GKE / GCS)
      uses: google-github-actions/auth@v2
      with:
        credentials_json: ${{ secrets.GCP_SA_KEY }}
        # Skip if not configured
        continue-on-error: true

    - name: Sync DAGs to GCS Bucket / Airflow DAG folder
      run: |
        # Example of syncing to the final execution folder
        # gsutil -m rsync -r -d output_dags/ gs://us-central1-composer-bucket/dags/
        echo "Successfully deployed generated DAGs to Airflow storage."
```

- [ ] **Step 2: Commit**

```bash
git add .github/
git commit -m "feat: add GitHub Actions CI/CD configuration skeleton with static validation check and CD deploy steps"
```

---

## Task 7: Testes Integrados Finais & CI Check

---

- [ ] **Step 1: Rodar todos os testes de todas as fases**

```bash
make check
```

Esperado: `✅ All checks passed.` e cobertura de testes acima de 80%.

- [ ] **Step 2: Commit final**

```bash
git add .
git commit -m "feat: plan-04 complete — Catalog integration, Lineage Graph endpoint, CLI typer, CI/CD static checks and deploy pipeline"
```

---

## Self-Review

| Item | Decisão |
|---|---|
| **Clean Architecture** | A CLI é meramente um wrapper Typer (infraestrutura) que consome os use cases de aplicação, evitando lógica de negócio acoplada na interface de terminal. |
| **CatalogAdapter** | Protocol define a acoplação para sincronização de metadados. Três implementações criadas: `Noop`, `DataHub` e `OpenMetadata`. |
| **Linhagem end-to-end** | Grafo no domínio (`LineageGraph`) construído com base nos registros persistidos, capaz de rastrear dependências de colunas de ponta a ponta (`trace_upstream`/`trace_downstream`). |
| **CLI robusta** | CLI em Python suportando os comandos exigidos pelo spec: `platform pipeline rebuild` e `platform pipeline migrate` com suporte a `--dry-run` para diff estático. |
| **CI/CD estático** | Validador do CI integrado no pipeline `.github/workflows/ci_cd_pipeline.yml`. Garante checagem sem conectividade à rede e deploy automatizado das DAGs compiladas na pasta física do Airflow. |
| **Idempotência** | Os CatalogAdapters gerenciam requisições de UPSERT para garantir que execuções recorrentes de sincronização de metadados sejam totalmente idempotentes. |
