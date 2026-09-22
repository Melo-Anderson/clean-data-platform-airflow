# Airflow Modern Data Platform — Architectural Showcase & Study Lab

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Airflow-3.0_Ready-green)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-teal)
![Dataform](https://img.shields.io/badge/Dataform-BigQuery_Native-blue)
![Architecture](https://img.shields.io/badge/Architecture-DDD_%7C_Clean-purple)
![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub_Actions-orange)

Este repositório é um **projeto pessoal de laboratório e estudo aprofundado de arquitetura de plataformas de dados modernas**. O foco central da iniciativa não é o tuning de performance em escala extrema imediata, mas sim servir como uma **bancada experimental e de exploração conceitual** para projetar uma estrutura genérica, altamente desacoplada e extensível utilizando princípios de **Domain-Driven Design (DDD)** e **Clean Architecture**.

A plataforma foi projetada para testar e comparar múltiplos padrões do ecossistema de dados — permitindo plugar, validar e alternar componentes analíticos, de governança e de orquestração sem acoplar a lógica de negócio ao orquestrador ou a um vendor específico.

---

### 🤖 Desenvolvimento Assistido por IA (Spec-Driven Development)
Todo o software foi concebido e codificado utilizando a metodologia de **Spec-Driven Development (SDD)**, com uma abordagem orientada a especificações formais, contratos estritos, agentes autônomos e automação de workflow. A construção foi orquestrada por meio de ferramentas especializadas como o ecossistema [Superpowers](https://github.com/obra/superpowers/tree/main) (para isolamento de tarefas técnicas de TDD e debugging), o [Strategist Skill](https://github.com/SergioLacerda/strategist-skill/) e o [SDD Harness](https://sergiolacerda.github.io/sdd-harness/).

Uma das principais lições práticas deste projeto é que **o design estrutural, a clareza arquitetural e contratos bem definidos são infinitamente mais determinantes para a sustentabilidade do código do que o custo ou o poder bruto da LLM utilizada**. Neste projeto foi utilizada ativamente a família de modelos **Gemini (do 3.5 Flash ao 3.8 Flash)**, além de assistentes como Claude e ChatGPT para apoio conceitual e validações de julgamento. Mesmo empregando modelos eficientes e de custo acessível para a geração guiada de código, a aderência a boas práticas consolidadas de engenharia de software (*Clean Code*, *Clean Architecture* e *DDD*) permitiu construir uma base modular e de fácil evolução.

---

### 🔬 Laboratório de Exploração vs. Produção Real (Trade-offs e Próximos Passos)
Por ter sido desenhada como um ambiente de testes e experimentação técnica exaustiva, a plataforma deliberadamente integrou uma ampla variedade de ferramentas e adapters (FastAPI, Airflow 3, BigQuery, Dataform, dbt, DuckDB, MongoDB, OmniBeam em Go, OpenBao/Vault, LangGraph, OpenTelemetry).

Em uma análise crítica e diagnóstica sobre a evolução do sistema:
- **Superfície de Exploração:** A amplitude de integrações serviu ao propósito didático e de prova de conceito para avaliar diferentes paradigmas de compute, catálogo e governança.
- **Transição para Produção ("Golden Path"):** Em um cenário de implementação corporativa real, uma refatoração essencial seria **enxugar o escopo**, podando adaptadores secundários e estabelecendo um conjunto estrito e robusto de ferramentas *core/gold* (por exemplo: BigQuery + Dataform/dbt + Airflow), além de desacoplar o filesystem compartilhado em favor de Object Storage (GCS) e execução isolada (K8s Pods / Cloud Run).

---

## 🏗️ Visão Geral da Arquitetura & Modularidade

A plataforma resolve o acoplamento excessivo tradicional em engenharia de dados ao isolar as regras de negócio e os metadados do orquestrador físico (Apache Airflow 3). O design segue rigorosa separação em camadas:

1. **Domain (`app/domain`)**: O coração da plataforma, contendo entidades puras (`Pipeline`, `DataAsset`, `PipelineRun`, `DataObject`, `DataElement`) e Value Objects sem nenhuma dependência de frameworks.
2. **Application (`app/application`)**: Casos de uso (`RegisterPipeline`, `RunDiscovery`, `ExecutePipelineRun`, `SyncTransformationCatalog`) e definições de portas (`UnitOfWork`, `SecretManagerPort`, `ComputeJobAdapter`, `DwhLoaderPort`) expressas como `Protocols` Python.
3. **Infrastructure (`app/infrastructure`)**: Adaptadores que implementam os protocolos (SQLAlchemy Repositories, OpenBao/Vault Client, DuckDB Engine, BigQuery Loader, adaptadores Dataform e dbt).

### Capacidade de Evolução e Motores Integrados
* **Secret Management:** A resolução de credenciais é feita via `SecretManagerPort`. O projeto implementa um adaptador para o **OpenBao (Vault)**, facilmente intercambiável com GCP Secret Manager ou AWS Secrets Manager.
* **Metadata Discovery:** Mapeamento automático de schemas para Bancos Relacionais (`database`), NoSQL (**MongoDB** via driver assíncrono `motor`), **APIs REST** (via parsing OpenAPI) e **Filesystem** (com amostragem DuckDB).
* **Motores de Transformação e Modelagem (Dataform & dbt):**
  * **Google Dataform (`dataform`):** Motor nativo GCP para modelagem declarativa em SQLX, compilando dependências em grafo direcionado (DAG), aplicando asserções de qualidade nativas e customizadas, e sincronizando tabelas e colunas com o catálogo interno da plataforma via `DataformCompilationParser` e `DataformCatalogAdapter`.
  * **dbt Core (`dbt`):** Motor modular de transformação SQL para materialização em camadas **Medallion (Bronze ➔ Staging ➔ Silver ➔ Gold)**, modelagem dimensional Kimball (`dim_*`, `fct_*`) e regras de detecção analítica (`gold_fraud_alerts`).
* **Compute Engines de Ingestão:**
  * **[OmniBeam](https://github.com/Melo-Anderson/omnibeam-go) (`omnibeam`):** Motor em Go + Apache Beam para parsing e conversão concorrente de arquivos brutos (JSON, CSV, NDJSON) para Parquet com injeção automática de metadados (`_ingested_at`, `_source_file`) e idempotência via hash MD5.
  * **DuckDB (`duckdb`):** Motor analítico embutido para processamento rápido em memória e cargas de desenvolvimento local.
  * **REST API (`rest_api`):** Ingestão paralela com suporte a paginação assíncrona (`page_number`, `offset_limit`, `cursor`).
* **DWH Loading (Carga em Data Warehouses):** Padrão **Write-Audit-Publish** com adaptadores desacoplados (`DwhLoaderPort`) para carregar arquivos estruturados (Parquet/Avro) para destinos modernos como **Google Cloud BigQuery**.
* **Orquestração Reativa com Airflow 3 Assets:** Encadeamento orientado a eventos e linhagem de dados orientada a ativos (`platform://asset/...`), disparando pipelines downstream em cascata à medida que novas partições são publicadas.

---

## 📖 Central de Documentação do Projeto

Para explorar as especificações conceituais e os aprendizados do laboratório, consulte os guias em `docs/`:

### 🚀 Visão & Governança
* **[Visão da Plataforma (docs/vision.md)](docs/vision.md):** O problema de negócio, objetivos, pilares e limites de escopo.
* **[Ciclo de Vida e Qualidade (docs/asset_lifecycle.md)](docs/asset_lifecycle.md):** Quality gates, estados operacionais e ciclo de feedback com o Airflow.
* **[Stakeholders e Acessos (docs/stakeholders.md)](docs/stakeholders.md):** Matriz de governança por perfil (PO/PM, SRE, Analytics Engineer).

### 🏗️ Arquitetura, Engenharia & Regras
* **[Regras de Negócio e Fluxos (docs/business_rules.md)](docs/business_rules.md):** Separação entre `DataAsset` lógico e `Endpoint` físico, fluxos core de descoberta e linhagem.
* **Modelagem C4 ([Contexto](docs/c4_model/context.md) / [Containers](docs/c4_model/containers.md)):** Diagramas C4 da plataforma.
* **[Guia de Clean Code & DDD (docs/clean-code.md)](docs/clean-code.md):** Princípios de código limpo, camadas hexagonais e testes com TDD.
* **[Decisões de Arquitetura - ADRs (docs/adr/README.md)](docs/adr/README.md):** Registro formal de decisões técnicas e trade-offs.

### ⚙️ Operação & DevOps
* **[Guia de Operações Local (docs/operations_guide.md)](docs/operations_guide.md):** Instruções de subida do cluster Docker Compose, banco `platform_db`, CLI e endpoints.
* **[Perfis de Executor do Airflow (docs/operations/executor-profiles.md)](docs/operations/executor-profiles.md):** LocalExecutor vs. CeleryExecutor vs. KubernetesExecutor.
* **[Guia de Automação de CI/CD (docs/ci_cd_guide.md)](docs/ci_cd_guide.md):** Pipeline de integração contínua (Ruff, Mypy) e compilação de DAGs.

---

## 🧪 Cobertura de Testes e Validação de Integrações

O projeto é respaldado por uma suíte de testes que assegura o funcionamento desacoplado das peças:

* **Testes de Unidade (`tests/unit`):** Validam a lógica pura de domínio, casos de uso e parsing de compilação do Dataform/dbt sem dependências de I/O externo.
* **Testes de Integração (`tests/integration`):** Testam a camada de persistência com PostgreSQL real e geradores de templates Jinja para DAGs.
* **Testes E2E (`tests/e2e`):** Executados no ambiente Docker Compose cobrindo:
  * Resolução de segredos no **OpenBao (Vault)**.
  * Descoberta automatizada de esquemas via **Metadata Discovery**.
  * Cargas DuckDB exportando Parquet com geração de metadados (`metrics.json` e `schema.json`).

---

## 🛠️ Iniciando o Ambiente

Suba o ecossistema local de estudo com:

1. **Configurar variáveis de ambiente:**
   ```bash
   cp .env.example .env
   # Edite o .env se desejar ativar BigQuery real ou ajustar segredos
   ```
2. **Inicializar o ambiente:**
   ```bash
   docker compose up -d --build
   ```
3. **Acessar as interfaces:**
   - **Airflow UI:** `http://localhost:8080` (admin/admin)
   - **Swagger / OpenAPI (API):** `http://localhost:8000/docs`
   - **OpenBao (Vault):** `http://localhost:8200` (token `PLATFORM_VAULT_TOKEN`)

4. **Executar os testes:**
   - Testes Unitários e de Integração:
     ```bash
     uv run pytest -m "not e2e" -v
     ```
   - Testes E2E completos:
     ```bash
     docker compose run --rm e2e-tests
     ```

---

## 🧠 Geração Declarativa via AI Harness

Para a geração automatizada de especificações YAML de pipelines a partir de intenção em linguagem natural (utilizando LangGraph, contratos Pydantic e validação determinística via Guardrails), a arquitetura conecta-se ao projeto externo dedicado:

👉 **[Pipeline Harness AI (github.com/Melo-Anderson/pipeline-harness-ai)](https://github.com/Melo-Anderson/pipeline-harness-ai)**

---

## 🔐 Autenticação GCP & BigQuery

Para testar as cargas no BigQuery e o Dataform com o Google Cloud:

1. **Autenticação Padrão (Recomendado para Dev Local):**
   ```bash
   gcloud auth application-default login
   ```
2. **Uso de Service Account (Opcional):**
   Armazene a chave JSON **obrigatoriamente fora do repositório** e aponte no `.env`:
   ```bash
   GOOGLE_APPLICATION_CREDENTIALS=/caminho/fora/do/repo/sua-chave.json
   PLATFORM_GCP_PROJECT=seu-gcp-project-id
   PLATFORM_DWH_PROVISIONER_ADAPTER=bigquery
   ```
