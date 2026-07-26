from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.domain.assets.asset_repository import AssetRepository
from app.domain.discovery.discovery_run_repository import DiscoveryRunRepository
from app.domain.discovery.drift_approval_repository import DriftApprovalRepository
from app.domain.endpoints.endpoint_repository import EndpointRepository
from app.domain.lineage.lineage_repository import LineageRepository
from app.domain.objects.object_repository import DataObjectRepository
from app.domain.pipelines.pipeline_repository import PipelineRepository
from app.domain.pipelines.pipeline_run_repository import PipelineRunRepository
from app.domain.shared.audit_log_repository import AuditLogRepository


@runtime_checkable
class UnitOfWork(Protocol):
    """
    Unit of Work: groups repositories under a single transactional boundary.

    All use cases that perform writes must use a UoW to ensure atomicity.
    This covers: create asset + emit audit log + publish catalog + send notification.

    The UoW is a context manager: use it with `async with`:

    Example:
        async with uow:
            asset = await uow.assets.save(new_asset)
            await uow.commit()
        # Side effects (catalog, notifications) dispatched after commit
    """

    @property
    def assets(self) -> AssetRepository: ...

    @property
    def endpoints(self) -> EndpointRepository: ...

    @property
    def objects(self) -> DataObjectRepository: ...

    @property
    def pipelines(self) -> PipelineRepository: ...

    @property
    def pipeline_runs(self) -> PipelineRunRepository: ...

    @property
    def lineage(self) -> LineageRepository: ...

    @property
    def discovery_runs(self) -> DiscoveryRunRepository: ...

    @property
    def drift_approvals(self) -> DriftApprovalRepository: ...

    @property
    def audit_logs(self) -> AuditLogRepository: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...

    async def __aenter__(self) -> UnitOfWork: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None: ...
