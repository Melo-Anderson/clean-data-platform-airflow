from __future__ import annotations

from fastapi import Depends

from app.application.assets.activate_asset import ActivateAssetUseCase
from app.application.assets.register_asset import RegisterAssetUseCase
from app.application.assets.update_asset import UpdateAssetUseCase
from app.application.discovery.approve_drift_use_case import ApproveDriftUseCase
from app.application.discovery.discovery_provisioning_service import DiscoveryProvisioningService
from app.application.discovery.metadata_self_healing_service import MetadataSelfHealingService
from app.application.discovery.run_discovery_use_case import RunDiscoveryUseCase
from app.application.pipelines.record_pipeline_run_use_case import RecordPipelineRunUseCase
from app.application.pipelines.register_pipeline import RegisterPipelineUseCase
from app.application.pipelines.report_pipeline_run_use_case import ReportPipelineRunUseCase
from app.application.pipelines.trigger_pipeline_run import TriggerPipelineRunUseCase
from app.config import Settings, get_settings
from app.domain.discovery.services.policy_tag_inferrer import PolicyTagInferrer
from app.domain.discovery.services.schema_differ import SchemaDiffer
from app.domain.discovery.services.schema_drift_service import SchemaDriftService
from app.domain.pipelines.quality_gate_evaluator import QualityGateEvaluator
from app.infrastructure.adapters.catalog.catalog_factory import get_catalog_adapter
from app.infrastructure.adapters.notifications.noop_notification_adapter import (
    NoopNotificationAdapter,
)
from app.infrastructure.adapters.orchestration.airflow_orchestrator_adapter import (
    AirflowOrchestratorAdapter,
)
from app.infrastructure.adapters.secrets.secret_manager_factory import get_secret_manager
from app.infrastructure.dag_generator.dag_generator import DagGenerator
from app.infrastructure.discovery.discovery_runner_factory import DiscoveryRunnerFactoryImpl
from app.infrastructure.dwh_provisioners.dwh_provisioner_factory import get_dwh_provisioner
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork
from app.infrastructure.yaml_generator.pipeline_yaml_generator import PipelineYamlGenerator


def get_uow() -> SqlUnitOfWork:
    return SqlUnitOfWork(get_session_factory())


def get_register_pipeline_use_case(
    uow: SqlUnitOfWork = Depends(get_uow),
    settings: Settings = Depends(get_settings),
) -> RegisterPipelineUseCase:
    return RegisterPipelineUseCase(
        uow=uow,
        dwh_provisioner=get_dwh_provisioner(settings),
        dags_path=str(settings.resolved_dags_path),
        yaml_generator=PipelineYamlGenerator(),
        dag_generator=DagGenerator(),
    )


def get_trigger_pipeline_use_case(
    uow: SqlUnitOfWork = Depends(get_uow),
    settings: Settings = Depends(get_settings),
) -> TriggerPipelineRunUseCase:
    orchestrator = AirflowOrchestratorAdapter(
        airflow_url=settings.airflow_url,
        username=settings.airflow_username,
        password=settings.airflow_password,
    )
    return TriggerPipelineRunUseCase(
        uow=uow,
        orchestrator=orchestrator,
        yaml_generator=PipelineYamlGenerator(),
        dag_generator=DagGenerator(),
        dags_path=settings.dags_path,
    )


def get_report_pipeline_run_use_case(
    uow: SqlUnitOfWork = Depends(get_uow),
) -> ReportPipelineRunUseCase:
    return ReportPipelineRunUseCase(uow=uow, quality_gate=QualityGateEvaluator())


def get_record_pipeline_run_use_case(
    uow: SqlUnitOfWork = Depends(get_uow),
) -> RecordPipelineRunUseCase:
    return RecordPipelineRunUseCase(uow=uow)


def get_register_asset_use_case(
    uow: SqlUnitOfWork = Depends(get_uow),
    settings: Settings = Depends(get_settings),
) -> RegisterAssetUseCase:
    return RegisterAssetUseCase(
        uow=uow,
        catalog=get_catalog_adapter(settings),
        notifications=NoopNotificationAdapter(),
        dwh_provisioner=get_dwh_provisioner(settings),
    )


def get_activate_asset_use_case(
    uow: SqlUnitOfWork = Depends(get_uow),
    settings: Settings = Depends(get_settings),
) -> ActivateAssetUseCase:
    return ActivateAssetUseCase(
        uow=uow,
        catalog=get_catalog_adapter(settings),
        notifications=NoopNotificationAdapter(),
    )


def get_update_asset_use_case(
    uow: SqlUnitOfWork = Depends(get_uow),
    settings: Settings = Depends(get_settings),
) -> UpdateAssetUseCase:
    return UpdateAssetUseCase(
        uow=uow,
        catalog=get_catalog_adapter(settings),
        notifications=NoopNotificationAdapter(),
    )


def get_run_discovery_use_case(
    uow: SqlUnitOfWork = Depends(get_uow),
    settings: Settings = Depends(get_settings),
) -> RunDiscoveryUseCase:
    secret_manager = get_secret_manager(settings)
    factory = DiscoveryRunnerFactoryImpl(secret_manager=secret_manager)
    schema_differ = SchemaDiffer()
    tag_inferrer = PolicyTagInferrer()
    drift_service = SchemaDriftService(schema_differ, tag_inferrer)
    self_healing = MetadataSelfHealingService(uow=uow, object_service=None)
    provisioning_service = DiscoveryProvisioningService(uow)
    return RunDiscoveryUseCase(
        uow=uow,
        runner_factory=factory,
        drift_service=drift_service,
        self_healing=self_healing,
        provisioning_service=provisioning_service,
    )


def get_approve_drift_use_case(
    uow: SqlUnitOfWork = Depends(get_uow),
) -> ApproveDriftUseCase:
    return ApproveDriftUseCase(uow=uow)
