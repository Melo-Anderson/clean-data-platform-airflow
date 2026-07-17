# Especificação de Design: Auditoria Operacional de Runtime (Audit Logging)

**Data:** 2026-07-17
**Status:** Aprovado
**Contexto:** O estado declarativo (Assets, Endpoints, Pipelines) já é auditado via versionamento no Git (GitOps). Esta especificação foca estritamente em registrar **ações operacionais de runtime** executadas por usuários ou fluxos automatizados do Airflow no banco de dados.

## Objetivos e Requisitos

1. **Evitar Acoplamento:** As camadas internas de negócio (`app/domain` e `app/application`) devem permanecer puras, sem carregar referências a atores ou autenticação nas assinaturas de métodos.
2. **Auditoria Não Bloqueante:** Gravar logs em segundo plano (assincronamente) para minimizar a latência das chamadas de API usando `BackgroundTasks`.
3. **Escopo Focado:** Auditar apenas disparos manuais, execuções do Airflow e resultados do Quality Gate.

---

## Design Detalhado

### 1. Utilitário de Auditoria (`Background Task`)

**Arquivo:** `app/infrastructure/http/audit_helper.py`

Criação de uma função assíncrona que gerencia o ciclo de vida de sua própria sessão de banco de dados para evitar conflitos com a sessão da request HTTP principal.

```python
from __future__ import annotations

from typing import Any
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.repositories.sql_audit_log_repository import SqlAuditLogRepository

async def write_audit_log_task(
    actor_id: str,
    actor_email: str,
    event_type: str,
    entity_type: str,
    entity_id: str,
    payload: dict[str, Any],
    description: str,
) -> None:
    """Background task function that opens its own DB session to persist the audit log."""
    session_factory = get_session_factory()
    async with session_factory() as session:
        repo = SqlAuditLogRepository(session)
        repo.save(
            event_type=event_type,
            entity_type=entity_type,
            entity_id=entity_id,
            actor_id=actor_id,
            actor_email=actor_email,
            payload=payload,
            description=description,
        )
        await session.commit()
```

---

### 2. Integração nos Routers (API Boundary)

Os pontos de entrada HTTP interceptam o ator autenticado (`CurrentUser`) e agendam o log em segundo plano.

#### A. Trigger Pipeline Run (POST `/pipelines/{pipeline_id}/run`)
**Arquivo:** `app/infrastructure/http/routers/pipeline_router.py`

```python
from fastapi import BackgroundTasks
from app.infrastructure.http.audit_helper import write_audit_log_task

@router.post("/{pipeline_id}/run", response_model=PipelineRunResponse, status_code=status.HTTP_201_CREATED)
async def trigger_pipeline_run(
    request: Request,
    pipeline_id: str,
    body: TriggerRunRequest,
    background_tasks: BackgroundTasks,
    current_user: CurrentUser = Depends(require_permission("pipeline:trigger")),
) -> PipelineRunResponse:
    uow = SqlUnitOfWork(get_session_factory())
    orchestrator = AirflowOrchestratorAdapter()
    use_case = TriggerPipelineRunUseCase(uow=uow, orchestrator=orchestrator)

    run = await use_case.execute(pipeline_id=pipeline_id, triggered_by=body.triggered_by)

    background_tasks.add_task(
        write_audit_log_task,
        actor_id=current_user.id,
        actor_email=str(current_user.email),
        event_type="pipeline.triggered",
        entity_type="PipelineRun",
        entity_id=run.id,
        payload={"dag_run_id": run.dag_run_id, "triggered_by": body.triggered_by},
        description=f"Pipeline execution run triggered manually by user",
    )

    return PipelineRunResponse(...)
```

#### B. Trigger Discovery Run (POST `/discovery/assets/{asset_name}/run`)
**Arquivo:** `app/infrastructure/http/routers/discovery_router.py`

```python
@router.post("/assets/{asset_name}/run", response_model=DiscoveryRunResponse, status_code=status.HTTP_201_CREATED)
async def trigger_discovery_run(
    asset_name: str,
    body: TriggerDiscoveryRequest,
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission("catalog:view")),
) -> DiscoveryRunResponse:
    uow = SqlUnitOfWork(get_session_factory())
    # ... executa use case ...
    run = await use_case.execute(asset_id=asset.id, triggered_by=body.triggered_by)

    background_tasks.add_task(
        write_audit_log_task,
        actor_id=current_user.id,
        actor_email=str(current_user.email),
        event_type="discovery.triggered",
        entity_type="DiscoveryRun",
        entity_id=run.id,
        payload={"asset_id": asset.id},
        description=f"Metadata discovery execution triggered manually by user",
    )
    return DiscoveryRunResponse.model_validate(run)
```

#### C. Quality Gate & Fim de Execução (POST `/pipelines/{pipeline_id}/runs/{run_id}/quality-gate`)
Chamado de forma automatizada pelo callback do Airflow.
**Arquivo:** `app/infrastructure/http/routers/pipeline_router.py`

```python
@router.post("/{pipeline_id}/runs/{run_id}/quality-gate", response_model=QualityGateReportResponse)
async def report_quality_gate(
    pipeline_id: str,
    run_id: str,
    body: QualityGateReportRequest,
    background_tasks: BackgroundTasks,
) -> QualityGateReportResponse:
    uow = SqlUnitOfWork(get_session_factory())
    use_case = ReportPipelineRunUseCase(uow=uow)
    run = await use_case.execute(run_id=run_id, metrics=body.metrics)

    # Gravado assincronamente com ator de sistema
    background_tasks.add_task(
        write_audit_log_task,
        actor_id="airflow_worker",
        actor_email="worker@airflow.apache.org",
        event_type="pipeline.run_completed",
        entity_type="PipelineRun",
        entity_id=run.id,
        payload={"status": run.status.value, "violations": run.quality_violations or []},
        description=f"Pipeline run completed with status: {run.status.value}",
    )

    return QualityGateReportResponse(...)
```

---

## Plano de Verificação

### Testes de Integração API
- Criar novos testes em `tests/unit/infrastructure/http/test_audit_api.py` (ou nos respectivos testes de rotas) verificando que ao disparar uma rota auditada, a `BackgroundTasks` é alimentada com a chamada correta do `write_audit_log_task`.

### Teste E2E (Verificação Real de Gravação)
- Modificar o fluxo E2E existente (`test_platform_e2e.py`) para consultar a tabela de `audit_logs` no banco de dados e garantir que as linhas de auditoria reais com ator `"airflow_worker"` e os IDs de JWT corretos foram gravadas com sucesso após a execução do fluxo.
