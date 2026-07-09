# Arquitetura C4 — Plataforma de Dados

Este documento descreve a arquitetura da plataforma usando o modelo C4 (Context, Containers, Components, Code).

---

## Nível 1 — Contexto do Sistema

```mermaid
graph TD
    PO["👤 PO / PM\n(Dono do Produto)"]
    SRE["👤 SRE\n(Operações)"]
    AE["👤 Analytics Engineer\n(Engenheiro de Dados)"]
    AIRFLOW_CB["⚙️ Airflow\n(Callback interno)"]

    PLATFORM["🏗️ Plataforma de Dados\n(Este sistema)"]

    PO -->|"Registra Assets e Pipelines\nvia REST API"| PLATFORM
    SRE -->|"Ativa Assets, aprova drift\nvia REST API"| PLATFORM
    AE -->|"Registra Pipelines,\ndispara Runs"| PLATFORM
    AIRFLOW_CB -->|"Reporta métricas\nde execução (Quality Gate)"| PLATFORM

    PLATFORM -->|"Orquestra DAGs"| AIRFLOW["☁️ Apache Airflow 3"]
    PLATFORM -->|"Lê credenciais"| VAULT["🔐 OpenBao (Vault)"]
    PLATFORM -->|"Persiste metadados"| DB["🐘 PostgreSQL"]
    AIRFLOW -->|"Executa DAGs\ngeradas pela plataforma"| DB
```

---

## Nível 2 — Containers

```mermaid
graph TD
    CLIENT["👤 Usuário / Airflow Callback"]

    subgraph Docker Compose
        API["🐍 Platform API\n(FastAPI + Uvicorn)\n:8000"]
        SCHED["🕐 Airflow Scheduler\n+ DAG Processor"]
        WEB["🌐 Airflow Webserver\n:8080"]
        PG["🐘 PostgreSQL\n:5432\nBanco: platform_db + airflow"]
        VAULT["🔐 OpenBao\n:8200\nKV v2"]
        DAGS["📁 Volume Compartilhado\n./dags → /opt/airflow/dags"]
    end

    CLIENT -->|"REST HTTP"| API
    API -->|"Grava arquivos .py\nvia filesystem"| DAGS
    API -->|"SQLAlchemy async"| PG
    API -->|"HTTP REST v2"| WEB
    SCHED -->|"Lê arquivos .py"| DAGS
    SCHED -->|"Persiste metadata"| PG
    WEB -->|"Lê metadata"| PG
    API -->|"Lê credenciais"| VAULT
```

---

## Nível 3 — Componentes da Platform API

```mermaid
graph TD
    subgraph "HTTP Layer (app/infrastructure/http/)"
        ROUTER_PIPE["PipelineRouter\n/pipelines/"]
        ROUTER_ASSET["AssetRouter\n/assets/"]
        ROUTER_DISC["DiscoveryRouter\n/discovery/"]
        ROUTER_END["EndpointRouter\n/endpoints/"]
        AUTH["AuthMiddleware\nBearerToken → Role"]
    end

    subgraph "Application Layer (app/application/)"
        UC_REG["RegisterPipelineUseCase"]
        UC_TRIG["TriggerPipelineRunUseCase"]
        UC_QUAL["ReportPipelineRunUseCase"]
        UC_ASSET["RegisterAssetUseCase\nActivateAssetUseCase"]
        UC_DISC["RunDiscoveryUseCase"]
        UOW["UnitOfWork Protocol\n(porta de persistência)"]
        ORCH["OrchestratorPort\n(porta do Airflow)"]
    end

    subgraph "Domain Layer (app/domain/)"
        PIPELINE["Pipeline\n(Aggregate Root)"]
        ASSET["DataAsset\n(Aggregate Root)"]
        PRUN["PipelineRun\n(Entity)"]
        QR["QualityRule\n(Value Object)"]
        SC["ScheduleConfig\n(Value Object)"]
        PS["PipelineRunStatus\n(Enum)"]
    end

    subgraph "Infrastructure Layer (app/infrastructure/)"
        SQL_UOW["SqlUnitOfWork\nimplementa UnitOfWork"]
        SQL_REPO_P["SqlPipelineRepository"]
        SQL_REPO_R["SqlPipelineRunRepository\n(save: get + update ou insert)"]
        SQL_REPO_A["SqlAssetRepository"]
        AIRFLOW_ADAPT["AirflowOrchestratorAdapter\nHTTP REST v2 + retry + refresh"]
        DAG_GEN["DagGenerator\nJinja2 templates"]
        DISC_RUN["DatabaseDiscoveryRunner"]
        QUALITY["QualityGateEvaluator"]
        VAULT_CLIENT["OpenBaoClient"]
    end

    ROUTER_PIPE --> AUTH
    AUTH --> UC_REG
    AUTH --> UC_TRIG
    AUTH --> UC_QUAL

    UC_REG --> UOW
    UC_TRIG --> UOW
    UC_TRIG --> ORCH
    UC_TRIG --> DAG_GEN
    UC_QUAL --> UOW
    UC_QUAL --> QUALITY

    UOW --> SQL_UOW
    SQL_UOW --> SQL_REPO_P
    SQL_UOW --> SQL_REPO_R
    SQL_UOW --> SQL_REPO_A

    ORCH --> AIRFLOW_ADAPT
    DISC_RUN --> VAULT_CLIENT

    UC_REG --> PIPELINE
    UC_TRIG --> PRUN
    UC_QUAL --> PRUN
    PIPELINE --> QR
    PIPELINE --> SC
    PRUN --> PS
```

---

## Fluxo: Trigger de Pipeline Run (sequência interna)

```mermaid
sequenceDiagram
    participant Client
    participant PipelineRouter
    participant TriggerPipelineRunUseCase
    participant SqlUnitOfWork
    participant DagGenerator
    participant AirflowOrchestratorAdapter
    participant AirflowWebserver

    Client->>PipelineRouter: POST /pipelines/{id}/run
    PipelineRouter->>TriggerPipelineRunUseCase: execute(pipeline_id, triggered_by)
    TriggerPipelineRunUseCase->>SqlUnitOfWork: find Pipeline by id
    TriggerPipelineRunUseCase->>SqlUnitOfWork: save PipelineRun (status=RUNNING)
    TriggerPipelineRunUseCase->>DagGenerator: render Jinja2 template → .py file
    DagGenerator-->>TriggerPipelineRunUseCase: DAG file written to /opt/airflow/dags/
    TriggerPipelineRunUseCase->>AirflowOrchestratorAdapter: trigger_dag(dag_id, run_id)

    loop Retry up to 10x (404 = DAG not yet parsed)
        AirflowOrchestratorAdapter->>AirflowWebserver: POST /api/v2/dags/{dag_id}/dagRuns
        alt 404: Not yet parsed
            AirflowOrchestratorAdapter->>AirflowWebserver: POST /api/v2/dags/{dag_id}/refresh
            AirflowOrchestratorAdapter->>AirflowOrchestratorAdapter: sleep 5s, retry
        else 200: OK
            AirflowWebserver-->>AirflowOrchestratorAdapter: dagRunId
        end
    end

    AirflowOrchestratorAdapter-->>TriggerPipelineRunUseCase: dag_run_id
    TriggerPipelineRunUseCase->>SqlUnitOfWork: commit
    TriggerPipelineRunUseCase-->>PipelineRouter: PipelineRun
    PipelineRouter-->>Client: 201 {id, status: "running", dag_run_id}
```

---

## Fluxo: Quality Gate (callback do Airflow)

```mermaid
sequenceDiagram
    participant AirflowTask as Airflow Task (emit_monitoring_and_sla)
    participant PipelineRouter
    participant ReportPipelineRunUseCase
    participant QualityGateEvaluator
    participant SqlPipelineRunRepository

    AirflowTask->>PipelineRouter: POST /pipelines/{pid}/runs/{rid}/quality-gate\n{metrics: {...}}
    PipelineRouter->>ReportPipelineRunUseCase: execute(run_id, metrics)
    ReportPipelineRunUseCase->>SqlPipelineRunRepository: find_by_id(run_id)
    ReportPipelineRunUseCase->>QualityGateEvaluator: evaluate(metrics, quality_rules)
    alt Sem violações
        ReportPipelineRunUseCase->>SqlPipelineRunRepository: save(run.mark_success())
    else Com violações
        ReportPipelineRunUseCase->>SqlPipelineRunRepository: save(run.mark_quality_failed())
    end
    ReportPipelineRunUseCase-->>PipelineRouter: PipelineRun atualizado
    PipelineRouter-->>AirflowTask: 200 {run_id, status, violations}
```
