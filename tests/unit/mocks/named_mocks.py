from __future__ import annotations

from typing import Any

from app.domain.assets.asset_repository import AssetRepository
from app.domain.assets.data_asset import DataAsset
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_repository import PipelineRepository


class MockPipelineRepository(PipelineRepository):
    def __init__(self, initial_pipelines: list[Pipeline] | None = None) -> None:
        self._items: dict[str, Pipeline] = {p.id: p for p in (initial_pipelines or [])}

    async def save(self, pipeline: Pipeline) -> Pipeline:
        self._items[pipeline.id] = pipeline
        return pipeline

    async def find_by_id(self, pipeline_id: str) -> Pipeline | None:
        return self._items.get(pipeline_id)

    async def find_by_name(self, name: str) -> Pipeline | None:
        for p in self._items.values():
            if p.name == name:
                return p
        return None

    async def find_all(self) -> list[Pipeline]:
        return list(self._items.values())


class MockAssetRepository(AssetRepository):
    def __init__(self, initial_assets: list[DataAsset] | None = None) -> None:
        self._items: dict[str, DataAsset] = {a.id: a for a in (initial_assets or [])}

    async def save(self, asset: DataAsset) -> DataAsset:
        self._items[asset.id] = asset
        return asset

    async def find_by_id(self, asset_id: str) -> DataAsset | None:
        return self._items.get(asset_id)

    async def find_by_name(self, name: str) -> DataAsset | None:
        for a in self._items.values():
            if a.name == name:
                return a
        return None

    async def update_state(self, asset_id: str, state: Any) -> DataAsset:
        asset = self._items[asset_id]
        asset.state = state
        return asset

    async def update_endpoint(self, asset_id: str, endpoint_id: str) -> DataAsset:
        asset = self._items[asset_id]
        asset.endpoint_id = endpoint_id
        return asset

    async def update_scope(self, asset_id: str, scope: Any) -> DataAsset:
        asset = self._items[asset_id]
        asset.discovery_scope = scope
        return asset

    async def update(self, asset: DataAsset) -> DataAsset:
        self._items[asset.id] = asset
        return asset


class MockAuditLogRepository:
    def __init__(self) -> None:
        self.logs: list[dict[str, Any]] = []

    def save(self, **kwargs: Any) -> None:
        self.logs.append(kwargs)


class MockNamedUnitOfWork:
    def __init__(
        self,
        pipeline_repo: PipelineRepository | None = None,
        asset_repo: AssetRepository | None = None,
    ) -> None:
        self._pipelines = pipeline_repo or MockPipelineRepository()
        self._assets = asset_repo or MockAssetRepository()
        self._audit_logs = MockAuditLogRepository()
        self.committed = False
        self.rolled_back = False

    @property
    def pipelines(self) -> PipelineRepository:
        return self._pipelines

    @property
    def assets(self) -> AssetRepository:
        return self._assets

    @property
    def audit_logs(self) -> Any:
        return self._audit_logs

    @property
    def endpoints(self) -> Any:
        return None

    @property
    def objects(self) -> Any:
        return None

    @property
    def pipeline_runs(self) -> Any:
        return None

    @property
    def lineage(self) -> Any:
        return None

    @property
    def discovery_runs(self) -> Any:
        return None

    @property
    def drift_approvals(self) -> Any:
        return None

    async def __aenter__(self) -> MockNamedUnitOfWork:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        pass

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True
