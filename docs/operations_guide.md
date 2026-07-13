# Guia de Operações — Plataforma de Dados

Este guia descreve como operar a plataforma localmente, simular o ciclo de vida completo e executar os testes.

---

## 1. Pré-requisitos

- Docker Desktop (com WSL2 habilitado no Windows)
- `uv` (gerenciador de pacotes Python)
- Git
- **Pre-commit hooks** (obrigatório para manter CI verde):
  ```bash
  uv sync --all-extras
  uv run pre-commit install
  ```
  Após instalado, `ruff format`, `ruff check` e `mypy` rodam automaticamente em cada `git commit`.

---

## 2. Iniciando o Ambiente

```bash
# Subir todos os containers em background (Airflow, PostgreSQL, OpenBao, Platform API)
docker compose up -d --build

# Acompanhar inicialização do Airflow
docker compose logs -f airflow-init

# Acessar a UI do Airflow (admin/admin)
# http://localhost:8080

# Acessar a documentação da API (Swagger)
# http://localhost:8000/docs
```

O container `airflow-init` roda as migrações do banco de metadados do Airflow e cria o usuário admin automaticamente.

---

## 3. Containers e Acesso a Shell

| Container | Serviço | Comando |
|---|---|---|
| `platform-api` | FastAPI | `docker exec -it airflow-data-platform-sdd-platform-api-1 bash` |
| `airflow-scheduler` | Scheduler + DAG Processor | `docker exec -it airflow-data-platform-sdd-airflow-scheduler-1 bash` |
| `airflow-webserver` | Webserver REST API | `docker exec -it airflow-data-platform-sdd-airflow-webserver-1 bash` |
| `postgres` | Banco de dados | `docker exec -it airflow-data-platform-sdd-postgres-1 bash` |
| `openbao` | Cofre de credenciais | `docker exec -it airflow-data-platform-sdd-openbao-1 sh` |

---

## 4. Bancos de Dados

O PostgreSQL hospeda dois bancos:

| Banco | Conteúdo |
|---|---|
| `platform_db` | Assets, Endpoints, DataObjects, Pipelines, PipelineRuns, Discovery Runs |
| `airflow` | Metadata do Airflow (DAGs, TaskInstances, DagRuns) |

### Conectando ao `platform_db`
```bash
# Do host:
psql -h localhost -p 5432 -U airflow -d platform_db

# De dentro do container:
docker exec -it airflow-data-platform-sdd-postgres-1 psql -U airflow -d platform_db
```

### Tabelas Úteis no `platform_db`

```sql
-- Assets registrados
SELECT id, name, state, owner_email FROM data_assets;

-- Endpoints cadastrados
SELECT id, name, type, credential_ref FROM endpoints;

-- Pipelines registrados
SELECT id, name, type FROM pipelines;

-- Execuções de pipeline (dashboard operacional)
SELECT id, pipeline_name, status, started_at, finished_at, sla_breached
FROM pipeline_runs ORDER BY started_at DESC;

-- Quality violations
SELECT id, pipeline_name, status, quality_violations
FROM pipeline_runs WHERE status = 'quality_failed';
```

---

## 5. Fluxo Completo de Operação (Passo a Passo)

### Passo 1: Registrar Credenciais no OpenBao

```bash
curl --header "X-Vault-Token: root" \
     --request POST \
     --data '{"data": {"username": "airflow", "password": "airflow", "host": "postgres", "port": "5432", "database": "platform_db"}}' \
     http://localhost:8200/v1/secret/data/postgres
```

### Passo 2: Registrar um Endpoint de Banco de Dados

```bash
curl -X POST "http://localhost:8000/endpoints/database" \
     -H "Authorization: Bearer sre" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "sales-db-prod",
       "credential_ref": "secret/postgres",
       "technical_description": "Banco de produção de vendas"
     }'
```

### Passo 3: Registrar um DataAsset (DRAFT)

```bash
curl -X POST "http://localhost:8000/assets/" \
     -H "Authorization: Bearer po_pm" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "sales-database-asset",
       "description": "Fonte de dados de vendas",
       "owner_email": "owner@company.com",
       "tags": ["vendas", "postgres"],
       "policy_tags": [],
       "discovery_schedule": "0 0 * * *",
       "discovery_scope_include": ["*"],
       "discovery_scope_exclude": []
     }'
```

### Passo 4: Ativar o Asset (DRAFT → ACTIVE)

Requer papel **SRE**. Vincula o Endpoint ao Asset e dispara a Discovery automática.

```bash
curl -X POST "http://localhost:8000/assets/sales-database-asset/activate?endpoint_name=sales-db-prod" \
     -H "Authorization: Bearer sre"
```

### Passo 5: Registrar um Pipeline

```bash
curl -X POST "http://localhost:8000/pipelines/" \
     -H "Authorization: Bearer po_pm" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "sales-daily-ingest",
       "pipeline_type": "ingestion",
       "owner_email": "owner@company.com",
       "source_asset_id": "<UUID do Asset>",
       "cron_schedule": "0 6 * * *"
     }'
```

### Passo 6: Disparar a Execução

```bash
curl -X POST "http://localhost:8000/pipelines/<pipeline_id>/run" \
     -H "Authorization: Bearer po_pm" \
     -H "Content-Type: application/json" \
     -d '{"triggered_by": "manual"}'
```

Este endpoint:
1. Cria um `PipelineRun` com `status=running`
2. Gera o arquivo `.py` da DAG em `./dags/`
3. Dispara o `dagRun` via API REST do Airflow

---

## 6. Executando os Testes

### Testes Unitários (sem Docker)

```bash
uv run pytest tests/unit/ -v
```

### Verificação de Tipos

```bash
uv run mypy app/
```

### Formatação de Código

Verifique se a formatação está de acordo com as regras (usado no CI):
```bash
uv run ruff format --check .
```

Aplique as correções de formatação automaticamente:
```bash
uv run ruff format .
```

### Linting

Verifique problemas de análise estática e aplique correções:
```bash
uv run ruff check . --fix
```

### Testes E2E (requer todos os containers rodando)

```bash
# Os testes E2E rodam dentro do container e2e-tests automaticamente:
docker compose run --rm e2e-tests

# Ou manualmente do host (requer AIRFLOW_URL e API_URL configurados):
uv run pytest tests/e2e/ -v
```

---

## 7. Forçando Resserialização de DAGs (Diagnóstico)

Se uma DAG gerada não aparecer no Airflow após a escrita do arquivo físico:

```bash
# Dentro do container do webserver:
docker exec airflow-data-platform-sdd-airflow-webserver-1 airflow dags reserialize

# Verificar se a DAG foi carregada:
docker exec airflow-data-platform-sdd-airflow-webserver-1 airflow dags list
```

> **Nota**: Em ambiente de desenvolvimento, a variável `AIRFLOW__CORE__STORE_SERIALIZED_DAGS: 'False'` faz o Webserver ler os arquivos diretamente do disco. Em produção, habilite a serialização (`True`) com intervalos mínimos para melhor performance.

---

## 8. Adicionando Novos Recursos

Ao adicionar novos domínios ou features, siga a ordem:

1. **Domain First**: Crie entidades em `app/domain/`. Use `@dataclass(kw_only=True)` para entidades e `@dataclass(frozen=True)` para Value Objects.
2. **Protocolo**: Defina a interface (Protocol) em `app/application/` se a nova feature precisar de I/O externo.
3. **Use Case**: Implemente a lógica de negócio em `app/application/`. Dependa apenas do Protocol, nunca de SQLAlchemy diretamente.
4. **Infrastructure**: Implemente repositórios, modelos SQLAlchemy e adaptadores em `app/infrastructure/`.
5. **Migração**: Crie a migration Alembic para alterações de schema.
6. **Testes**: Escreva os testes unitários primeiro (TDD). Em seguida, rode a suite E2E completa.

```bash
uv run pytest tests/ -v
uv run mypy app/
uv run ruff check app/
```

---

## 9. Observabilidade e Saúde (Monitoring & Health Probes)

A API expõe métricas e status de integridade essenciais para orquestração em Kubernetes e coleta pelo Prometheus.

### Endpoints de Saúde (Probes)

- **Liveness Probe** (`GET /health`):
  - Retorna `{ "status": "ok" }`.
  - Usado para detectar se a aplicação travou. **Não** faz chamadas de I/O ou checagens no Banco/Vault para evitar falhas em cascata.
  
- **Readiness Probe** (`GET /health/ready`):
  - Retorna o estado atual das dependências críticas.
  - Executa testes ativos: `SELECT 1` no banco de dados e `/v1/sys/health` no HashiCorp Vault.
  - Exemplo de resposta:
    ```json
    {
      "status": "ready",
      "components": {
        "database": "up",
        "vault": "up"
      }
    }
    ```
  - Se o Vault não estiver configurado no ambiente, o status é reportado como `"not_configured"`.

### Coleta de Métricas (Prometheus Scrape)

- **Scrape Endpoint** (`GET /metrics`):
  - Expõe métricas padronizadas do formato exposition do Prometheus.
  - Coleta histograma de latência de requests HTTP (`http_request_duration_seconds`) com labels `method`, `path`, e `status`.
  - Coleta contador de execuções de pipeline (`platform_pipeline_runs_total`).
  - Lógica implementada via `PrometheusMetricsAdapter` sob o desacoplamento da porta `TelemetryPort`.
