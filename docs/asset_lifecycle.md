# Ciclo de Vida do Pipeline e PipelineRun

## 1. A Entidade `Pipeline`

Um `Pipeline` representa a **intenção de mover ou transformar dados** de um DataAsset de origem para um destino, seguindo um agendamento e regras de qualidade configuradas.

O Pipeline é o **agregado central** do domínio de pipelines. Ele contém toda a configuração necessária para que a plataforma gere a DAG correspondente no Airflow.

### Tipos de Pipeline

| Tipo | Descrição |
|---|---|
| `ingestion` | Extrai dados de uma fonte e os move para a Landing Zone |
| `etl` | Transforma e carrega dados entre zonas (Clean → Refined) |
| `export` | Exporta dados processados para um sistema destino |

### Campos Principais

| Campo | Tipo | Descrição |
|---|---|---|
| `name` | string | Nome único. Vira o `dag_id` no Airflow. |
| `type` | PipelineType | `ingestion`, `etl` ou `export` |
| `owner` | EmailAddress | E-mail do responsável técnico |
| `schedule` | ScheduleConfig | Agendamento via cron ou modo data-driven |
| `source_asset_id` | UUID | Asset de origem |
| `quality_rules` | list[QualityRule] | Regras de qualidade avaliadas após cada run |
| `compute` | ComputeConfig | Motor de processamento (engine: `duckdb`, `spark`, `dataflow`) |
| `dataset_uri` | str (propriedade) | URI Airflow 3 do pipeline: `platform://pipeline/{id}` |

---

## 2. Quality Rules (Regras de Qualidade)

Regras de qualidade são avaliadas **após cada execução** pelo Quality Gate. Uma violação marca o run como `quality_failed`.

### Tipos de Regra Suportados

| Tipo | Descrição | Parâmetros |
|---|---|---|
| `not_null` | Garante que a coluna não contenha nulos | `column` |
| `row_count_min` | Garante volume mínimo de linhas | `value` (int) |
| `unique` | Garante unicidade na coluna | `column` |
| `accepted_values` | Garante que os valores pertencem a um conjunto | `column`, `value` (lista) |
| `referential_integrity` | Garante integridade referencial | `column` |
| `checksum` | Verifica integridade via hash | `column` |

---

## 3. A Entidade `PipelineRun`

O `PipelineRun` é o **registro operacional de cada execução** de um Pipeline. É criado quando a execução é disparada e atualizado após o Quality Gate.

### Estados do PipelineRun

```
        trigger_pipeline_run
               │
               ▼
           [RUNNING]
               │
               │ Callback: emit_monitoring_and_sla
               │
       ┌───────┴───────────┐
       │                   │
  (tarefas OK)      (tarefa mandatória falhou)
       │                   │
       ▼                   ▼
  Quality Gate         [FAILED]
       │
  ┌────┴────┐
  │         │
(pass)   (violação)
  │         │
  ▼         ▼
[SUCCESS] [QUALITY_FAILED]

(opcional falhado + mandatório OK) → [PARTIAL]
```

| Status | Descrição |
|---|---|
| `running` | Run em execução no Airflow |
| `success` | Todas as tarefas passaram e o quality gate foi aprovado |
| `failed` | Ao menos uma tarefa mandatória falhou |
| `quality_failed` | Execução bem-sucedida, mas métricas violaram as regras configuradas |
| `partial` | Tarefas mandatórias passaram, mas tarefas opcionais (observabilidade) falharam |

### Campos de Observabilidade

| Campo | Descrição |
|---|---|
| `started_at` | Momento em que o run foi disparado |
| `finished_at` | Momento em que o quality gate foi reportado |
| `failed_task` | ID da primeira tarefa mandatória que falhou (triage de causa raiz) |
| `optional_failures` | Lista de tasks opcionais que soft-falharam |
| `quality_violations` | Lista de mensagens de violação do quality gate |
| `metrics` | Métricas produzidas pelo compute engine (rows, bytes, checksum) |
| `sla_breached` | `true` se o pipeline excedeu o `sla_minutes` configurado |
| `last_run_at` | Última execução (sempre atualizado, inclusive em falha) |
| `last_success_at` | Última execução bem-sucedida (atualizado somente em SUCCESS/PARTIAL) |

---

## 4. Fluxo Completo: Do Registro ao Quality Gate

```
POST /pipelines/            ← Registra o Pipeline (cria no banco)
POST /pipelines/{id}/run    ← Dispara execução:
                               1. Cria PipelineRun com status=running
                               2. Grava arquivo .py da DAG em /opt/airflow/dags
                               3. Chama POST Airflow /api/v2/dags/{dag_id}/dagRuns

                           [Airflow executa o DAG]
                               ├── pre_flight (discovery, drift check)
                               ├── compute_group (submit → monitor → validate)
                               └── emit_monitoring_and_sla → chama POST /quality-gate

POST /pipelines/{pid}/runs/{rid}/quality-gate
                           ← Recebe métricas do callback do Airflow
                           ← Avalia QualityRules configuradas
                           ← Atualiza PipelineRun para SUCCESS ou QUALITY_FAILED
```
