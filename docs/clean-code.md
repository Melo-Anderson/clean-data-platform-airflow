# Boas Práticas de Desenvolvimento — Plataforma de Dados

Este documento unifica as práticas de **Clean Code**, **Clean Architecture**, **DDD** e **TDD** mapeadas diretamente para este projeto. Serve como guia normativo para qualquer desenvolvedor (humano ou IA) que evoluir a plataforma.

---

## 1. Clean Architecture — Estrutura em Camadas

A plataforma é estruturada em três camadas com dependências em uma única direção: **de fora para dentro**.

```
┌──────────────────────────────────────────────┐
│  Infrastructure (app/infrastructure/)        │
│  ├── HTTP (FastAPI Routers, Schemas)          │
│  ├── Persistence (SQLAlchemy Models, Repos)  │
│  ├── Airflow (Adapters, Callbacks, Templates)│
│  └── External (Airflow Adapter, OpenBao)     │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │  Application (app/application/)        │  │
│  │  ├── Use Cases (RegisterPipeline, ...) │  │
│  │  └── Ports/Protocols (UnitOfWork, ...) │  │
│  │                                        │  │
│  │  ┌──────────────────────────────────┐  │  │
│  │  │  Domain (app/domain/)            │  │  │
│  │  │  ├── Entities (Pipeline, Asset)  │  │  │
│  │  │  ├── Value Objects (EmailAddress)│  │  │
│  │  │  └── Enums (PipelineRunStatus)   │  │  │
│  │  └──────────────────────────────────┘  │  │
│  └────────────────────────────────────────┘  │
└──────────────────────────────────────────────┘
```

### Regras de Dependência

| De | Pode depender de | Não pode depender de |
|---|---|---|
| `domain/` | Nada além de stdlib Python | `application/`, `infrastructure/` |
| `application/` | `domain/`, Protocols | `infrastructure/` (apenas interfaces) |
| `infrastructure/` | `application/`, `domain/`, libs externas | — |

> **Violação clássica a evitar**: usar `sqlalchemy` ou `httpx` diretamente em `domain/` ou `application/`. Toda I/O deve ficar em `infrastructure/`.

---

## 2. Domain-Driven Design (DDD)

### Entidades vs. Value Objects

| Conceito | Características | Exemplos no projeto |
|---|---|---|
| **Entidade** | Tem identidade (`id`), mutável | `Pipeline`, `DataAsset`, `PipelineRun` |
| **Value Object** | Sem identidade, imutável (`frozen=True`) | `EmailAddress`, `QualityRule`, `ScheduleConfig` |

**Regra**: entidades usam `@dataclass(kw_only=True)`. Value objects usam `@dataclass(frozen=True)`.

### Agregados

O `Pipeline` é o **aggregado-raiz** do domínio de pipelines. Ele encapsula:
- `QualityRule` (lista de regras)
- `ComputeConfig` (configuração do motor)
- `ScheduleConfig` (configuração de agendamento)
- `ExtractionConfig` (objetos de origem)

**Regra**: acesse sempre pelo aggregado-raiz. Nunca manipule um `QualityRule` diretamente — passe pelo `Pipeline`.

### Protocolos como Portas (Ports)

As interfaces entre camadas são definidas como `Protocol` Python:

```python
# app/application/unit_of_work.py
class UnitOfWork(Protocol):
    pipelines: PipelineRepository
    pipeline_runs: PipelineRunRepository
    assets: AssetRepository
    ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
```

Isso garante que os Use Cases nunca dependam de SQLAlchemy diretamente. A implementação (`SqlUnitOfWork`) vive em `infrastructure/`.

---

## 3. Test-Driven Development (TDD)

### Pirâmide de Testes

```
           ▲
          /E2E\         ← tests/e2e/ — testes de ponta a ponta contra Docker Compose
         /─────\
        / Integ \       ← (futuro) testes de repositório contra banco real
       /─────────\
      /   Unit    \     ← tests/unit/ — testes de use cases com mocks
     /─────────────\
```

### Regras para Testes Unitários

- **Mocks**: use classes nomeadas explicitamente, não lambdas ou `MagicMock` anônimos.
  ```python
  class MockUoW:
      pipelines = MockPipelineRepo()
      pipeline_runs = MockPipelineRunRepo()
  ```
- **Fixtures assíncronas**: retornam `AsyncGenerator`, não o tipo direto.
  ```python
  @pytest_asyncio.fixture
  async def db_session() -> AsyncGenerator[AsyncSession, None]:
      ...
      yield session
  ```
- **F.I.R.S.T**: Fast, Independent, Repeatable, Self-validating, Timely.
  - Cada teste deve ser executável isoladamente.
  - Nenhum teste deve depender do resultado de outro.

### Regras para Testes E2E

- O arquivo `.py` da DAG só é criado quando o `POST /pipelines/{id}/run` é chamado. Portanto, sempre **dispare o run antes** de chamar `_wait_and_unpause_dag`.
- A suite E2E usa `docker exec ... airflow dags reserialize` via subprocess para forçar o scheduler a reconhecer novas DAGs sem depender do intervalo de polling.

### Rigor de Engenharia em Testes

#### 1. Testes Baseados em Propriedades (Hypothesis)
- Usados para validar invariantes de lógica de negócio complexa (como Value Objects e algoritmos de diferenciação de schemas como `DiscoveryScope` e `SchemaDiffer`).
- **Configuração de Perfis**:
  - Perfil `dev`: Execução local padrão com `max_examples=50` (rápido).
  - Perfil `ci`: Execução no GitHub Actions com `max_examples=500` (exaustivo) via variável de ambiente: `HYPOTHESIS_PROFILE=ci`.

#### 2. Testes de Caos e Injeção de Falhas (Respx)
- Simulam falhas reais em chamadas HTTP (Timeouts, HTTP 503, etc.) para testar adaptadores externos de infraestrutura (`AirflowOrchestratorAdapter`, `BaoSecretManagerAdapter`).
- Usados para validar comportamentos de retentativas (retry) e abertura de disjuntores (Circuit Breakers).

#### 3. Testes de Mutação (Mutmut)
- Validação da qualidade dos testes inserindo bugs artificiais em `app/domain/` e `app/application/`.
- Execução local via Makefile: `make mutation-test`.

---

## 4. Clean Code

### Tamanho e Responsabilidade

- Funções: **4 a 20 linhas**. Divida se ultrapassar.
- Arquivos: **máximo 300 linhas**. Separe por responsabilidade se ultrapassar.
- Uma responsabilidade por módulo (**SRP**).

### Nomenclatura

- Nomes específicos e únicos. Prefira nomes que retornem menos de 5 hits no grep do projeto.
- Evite: `data`, `handler`, `Manager`, `Utils`.
- Use o vocabulário do domínio: `PipelineRun`, `QualityGateEvaluator`, `DriftClassifier`.

### Tipos

- Tipagem explícita em todo o código. Proibido `Any` sem justificativa.
- Proibido `Dict`, `List`, `Optional` legacy. Use `dict`, `list`, `X | None`.
- Funções assíncronas com `async def` e retorno explícito.

### Tratamento de Erros

- Use a hierarquia de exceções de domínio definida em `app/domain/shared/exceptions.py`:
  - `PlatformNotFoundError`: recurso não encontrado (mapeia para HTTP 404).
  - `PlatformForbiddenError`: acesso não permitido por RBAC (mapeia para HTTP 403).
  - `PlatformUnauthorizedError`: falha de autenticação/JWT (mapeia para HTTP 401).
  - `PlatformValidationError`: violação de regra de entrada/formato (mapeia para HTTP 422).
  - `ValueError`: validação interna de construtor/Value Object.
  - `RuntimeError`: falha de infraestrutura externa não recuperável (mapeia para HTTP 500).
- Mensagens de erro devem incluir **o valor ofensivo e o que era esperado**.
  ```python
  raise PlatformNotFoundError(f"Pipeline not found: {pipeline_id}")  # Correto
  raise PlatformNotFoundError("Pipeline not found")                  # Errado
  ```

### Comentários

- Escreva **POR QUÊ**, não **O QUÊ**.
- Docstrings em métodos públicos: intenção + comportamento de borda.
- Preserve comentários de intenção em refatorações. Eles documentam decisões de design.

---

## 5. Padrões de Infraestrutura

### Repositórios

- Todo repositório implementa o Protocol correspondente em `domain/`.
- O método `save()` usa `session.get()` para checar existência antes de inserir — evita `duplicate key` em operações de update.
- Após `flush()`, sempre chame `await session.refresh(model)` para recarregar colunas geradas pelo banco (`created_at`, `updated_at`) sem disparar `MissingGreenlet`.

### Unit of Work

- Todo use case recebe um `UnitOfWork` por injeção de dependência.
- O `UoW` é o único ponto de `commit()` e `rollback()`. Use Cases nunca acessam a sessão diretamente.
- **Pattern:** `async with uow: ... await uow.commit()`.
- **Invariante de Lifecycle:** As propriedades dos repositórios (`uow.assets`, `uow.objects`, `uow.pipelines`, etc.) só devem ser acessadas **dentro** do bloco `async with uow:`. Acessá-las fora do contexto dispara `RuntimeError("SqlUnitOfWork must be used as an async context manager")`. Serviços instanciados no setup de rotas devem adiar o acesso às propriedades do UoW (lazy resolution).

### Funções Puras em Infraestrutura

- Lógicas de manipulação de formato, normalização de caminhos ou descompactação de envelopes HTTP (ex: `_normalize_vault_path()`, `_unpack_vault_response()`) devem ser extraídas como **funções puras de módulo** fora das classes de adaptadores.
- Isso permite testar a lógica de parsing isoladamente em testes unitários sem necessidade de instanciar mocks complexos de rede.

### Adaptadores Externos

- Todo sistema externo (Airflow, OpenBao) é acessado via Adapter em `infrastructure/adapters/`.
- Adapters implementam retry com backoff para resiliência a falhas transitórias (usando `tenacity`).
- O `AirflowOrchestratorAdapter` realiza até 10 tentativas com delay de 5s, chamando `POST /api/v2/dags/{dag_id}/refresh` em cada 404 para forçar reserialization.

---

## 6. Glossário de Decisões Arquiteturais

| Decisão | Racional |
|---|---|
| DAGs geradas via Jinja2, não hardcoded | Permite onboarding de novos pipelines sem alterar código Python |
| `STORE_SERIALIZED_DAGS: False` em dev | Evita delays de serialização em testes. Em produção, habilitar com intervalo mínimo. |
| `ComputeJobAdapter` como Protocol | Permite troca do motor de compute (DuckDB, Spark, RestApi) sem alterar a DAG ou o Use Case |
| Credenciais apenas no OpenBao | Nunca armazenar senha em banco de dados da plataforma ou em variáveis de ambiente do Airflow |
| `PipelineRun` separado de `Pipeline` | Pipeline é configuração (imutável por run). PipelineRun é estado operacional (muda a cada execução) |
| `JwtConfig` isolado no `JwtValidator` | Princípios ISP e DIP: o validador de JWT recebe apenas `JwtConfig` com as chaves necessárias, não o objeto `Settings` completo |

---

## 7. Polimorfismo e Contratos do Sistema

Para garantir que a plataforma seja agnóstica de ferramentas e nuvem, todas as integrações com I/O externa e lógica variável usam o **Polimorfismo de Interface** via `Protocol` Python. Os contratos devem respeitar estritamente as assinaturas sob pena de quebra em tempo de execução.

### Principais Interfaces e Contratos

#### 1. Resolução de Credenciais (`SecretManagerPort`)
*   **Protocolo:** `app/application/shared/secret_manager_port.py`
*   **Contrato:** `async def resolve(self, ref: str) -> dict[str, str]`
*   **Regra:** Deve resolver referências seguras (ex: `secret/postgres`) de forma assíncrona. Retorna um dicionário plano de credenciais.
*   **Implementações:** `BaoSecretManagerAdapter` (Vault/OpenBao real) e `NoopSecretManagerAdapter` (Testes/Dev).

#### 2. Motores de Execução (`ComputeJobAdapter`)
*   **Protocolo:** `app/infrastructure/airflow_callbacks/compute_job_adapter.py`
*   **Contratos principais:**
    *   `def submit_job(self, pipeline_id: str, pipeline_type: str, config: dict[str, Any]) -> str` (Retorna `job_id` síncrono e não-bloqueante)
    *   `def poll_job_status(self, job_id: str) -> ComputeJobResult` (Verifica conclusão)
    *   `def cancel_job(self, job_id: str) -> None`
*   **Regra:** Qualquer motor (DuckDB, REST API, Spark) deve implementar esses métodos para ser acoplável nas tasks da DAG do Airflow.
*   **Implementações:** `DuckDbComputeAdapter`, `RestApiComputeAdapter`.

#### 3. Autodescoberta de Metadados (`DiscoveryRunner`)
*   **Protocolos:** `app/application/discovery/discovery_runner.py`
    *   `DiscoveryRunner`: `async def run(self, asset_id: str, scope_include: list[str] | None, scope_exclude: list[str] | None, endpoint: Endpoint) -> list[SchemaSnapshot]`
    *   `DiscoveryRunnerFactory`: `def get_runner(self, endpoint_type: str) -> DiscoveryRunner`
*   **Regra:** O factory resolve o runner baseado no tipo do Endpoint (`database`, `mongodb`, `rest_api`). O runner deve extrair a estrutura física e retornar uma lista de `SchemaSnapshot`.
*   **Implementações:** `DatabaseDiscoveryRunner`, `MongoDbDiscoveryRunner`, `RestApiDiscoveryRunner`.

#### 4. Catálogos de Metadados (`CatalogAdapter`)
*   **Protocolo:** `app/application/shared/adapters/catalog_adapter.py`
*   **Contrato:** `async def upsert_schema(self, object_name: str, schema_version: CatalogSchemaVersion) -> None`
*   **Regra:** Sincroniza schemas atualizados com repositórios externos.
*   **Implementação:** `NoopCatalogAdapter`.

#### 5. Orquestração (`OrchestratorPort`)
*   **Protocolo:** `app/application/pipelines/orchestrator_port.py`
*   **Contrato:**
    *   `async def trigger_dag(self, pipeline_id: str, run_id: str) -> str`
    *   `async def check_dag_run_status(self, pipeline_id: str, dag_run_id: str) -> PipelineRunStatus`
*   **Implementação:** `AirflowOrchestratorAdapter` e `LoggingOrchestratorAdapter`.
