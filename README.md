# Airflow Modern Data Platform — Architectural Showcase

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Airflow-3.0_Ready-green)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-teal)
![Architecture](https://img.shields.io/badge/Architecture-DDD_%7C_Clean-purple)
![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub_Actions-orange)

Este repositório é um **projeto pessoal focado no design de arquitetura de plataformas de dados modernas**. O objetivo principal não é o tuning de performance em escala extrema, mas sim a criação de uma estrutura conceitual, genérica e altamente desacoplada, utilizando **Domain-Driven Design (DDD)** e **Clean Architecture**.

A plataforma foi projetada para ser flexível e evolutiva, permitindo a fácil substituição e adição de novas ferramentas e regras de negócio sem alterar o núcleo do domínio.

---

## 🏗️ Visão Geral da Arquitetura & Modularidade

A plataforma resolve o acoplamento excessivo que costuma ocorrer em ambientes de engenharia de dados ao isolar a lógica de negócio do orquestrador (Apache Airflow 3). O design segue a separação em camadas:

1.  **Domain (`app/domain`)**: O coração da plataforma, contendo entidades puras (`Pipeline`, `DataAsset`, `PipelineRun`) e Value Objects sem nenhuma dependência de frameworks.
2.  **Application (`app/application`)**: Casos de uso (`RegisterPipeline`, `RunDiscovery`) e definições de portas (`UnitOfWork`, `SecretManagerPort`) expressas como `Protocols` Python.
3.  **Infrastructure (`app/infrastructure`)**: Adaptadores que implementam os protocolos (SQLAlchemy Repositories, OpenBao/Vault Client, DuckDB Local Compute Engine).

### Capacidade de Evolução
*   **Secret Management:** A resolução de credenciais é feita via `SecretManagerPort`. O projeto implementa um adaptador para o **OpenBao (Vault)**, mas pode facilmente plugar serviços como AWS Secrets Manager ou Google Secret Manager.
*   **Metadata Discovery:** Mapeamento automático de schemas. Atualmente implementado para Bancos Relacionados (`database`), mas a estrutura genérica de interfaces aceita extensões rápidas para APIs REST, Buckets de Arquivos (GCS/S3) ou servidores SFTP.
*   **Compute Engines (Ingestão/ETL/Export):** Através do `ComputeJobAdapter`, a execução física é abstraída. A plataforma roda com o **DuckDbComputeAdapter** (processamento assíncrono em background thread local), mas está pronta para receber adaptadores de Spark, Snowflake ou Google Dataflow.
*   **Processamento Completo:** A arquitetura suporta conceitualmente pipelines de Ingestão (Landing), transformação de dados (ETL entre Clean/Refined) e Exportação para sistemas externos, tudo governado e monitorado pela mesma API.

---

## 📖 Central de Documentação do Projeto

Para entender as especificações detalhadas do projeto, navegue pelas documentações técnicas oficiais na pasta `docs/`:

### 🚀 Visão & Governança
*   **[Visão da Plataforma (docs/vision.md)](docs/vision.md):** O problema de negócio resolvido, objetivos e escopo do projeto.
*   **[Ciclo de Vida de Ingestão e Qualidade (docs/asset_lifecycle.md)](docs/asset_lifecycle.md):** Regras de qualidade (quality gates), estados operacionais de runs e ciclo de feedback com o Airflow.
*   **[Stakeholders e Governança de Acesso (docs/stakeholders.md)](docs/stakeholders.md):** Matriz de permissões por perfil (PO/PM, SRE, Analytics Engineer) e governança de ativação.

### 🏗️ Arquitetura & Engenharia
*   **[Regras de Negócio e Fluxos (docs/business_rules.md)](docs/business_rules.md):** Modela conceitualmente a separação entre `DataAsset` (lógico) e `Endpoint` (conectividade física via Vault/OpenBao), detalhando também os fluxos core (descoberta de metadados, pipelines, quality gates e linhagem).
*   **[Arquitetura do Sistema C4 (docs/architecture_c4.md)](docs/architecture_c4.md):** Diagramas C4 de contexto, containers e componentes, além de diagramas de sequência de operações.
*   **[Guia de Clean Code & DDD (docs/clean-code.md)](docs/clean-code.md):** Normas de código limpo, camadas do hexágono, uso de Value Objects e TDD.

### ⚙️ Operação & DevOps
*   **[Guia de Operações Local (docs/operations_guide.md)](docs/operations_guide.md):** Bootstrap do cluster local via Docker Compose, uso do banco `platform_db`, comandos de CLI e API.
*   **[Guia de Automação de CI/CD (docs/ci_cd_guide.md)](docs/ci_cd_guide.md):** Funcionamento do pipeline de integração contínua (Ruff, Mypy) e compilação/sincronização de DAGs.

---

## 🧪 Cobertura de Testes e Validação de Integrações

O projeto é guiado por testes rigorosos que garantem o correto funcionamento dos fluxos sem acoplamento operacional:

*   **Testes de Unidade (`tests/unit`):** Testam a lógica pura de domínio e casos de uso isolados de I/O por meio de stashes/mocks nomeados de banco e segurança.
*   **Testes de Integração (`tests/integration`):** Validam persistência contra banco em memória e geração de código.
*   **Testes E2E (`tests/e2e`):** Rodam no ambiente Docker Compose e garantem o funcionamento integrado de:
    *   Resolução segura de segredos em tempo de execução via **OpenBao (Vault)**.
    *   Conexão física e mapeamento automático via **Discovery Runner** (Database).
    *   Disparos de ingestão assíncrona pelo **DuckDbComputeAdapter** que lê tabelas PostgreSQL e exporta arquivos Parquet consolidados e estruturados junto com arquivos de metadados (`metrics.json` e `schema.json`).

---

## 🛠️ Iniciando o Ambiente

Suba todo o ecossistema local com um único comando:

1.  **Inicializar ambiente:**
    ```bash
    docker compose up -d --build
    ```
2.  **Acessar ferramentas:**
    -   **Airflow UI:** `http://localhost:8080` (admin/admin)
    -   **Documentação Swagger (API):** `http://localhost:8000/docs`
    -   **OpenBao (Vault):** `http://localhost:8200` (token: `root`)

3.  **Executar os testes:**
    -   Apenas Testes Unitários/Integração (independentes de Docker):
        ```bash
        uv run pytest -m "not e2e" -v
        ```
    -   Testes E2E Completos (dentro da rede Docker Compose):
        ```bash
        docker compose run --rm e2e-tests
        ```
