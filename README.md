# SDD Airflow Data Platform

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Airflow-3.0_Ready-green)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-teal)
![Architecture](https://img.shields.io/badge/Architecture-DDD_%7C_Clean-purple)
![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub_Actions-orange)

Uma plataforma avançada e desacoplada de orquestração de dados construída em torno do **Airflow 3**, utilizando **Domain-Driven Design (DDD)** e **Clean Architecture** para garantir testabilidade estrita, isolamento de regras de negócio e escalabilidade.

---

## 📖 Central de Documentação do Projeto

Abaixo estão listados os manuais e especificações técnicas oficiais da plataforma localizados na pasta `docs/`. Cada guia possui uma finalidade distinta no ciclo de vida de desenvolvimento e operação:

### 🚀 Visão & Governança

*   **[Visão da Plataforma (docs/vision.md)](docs/vision.md)**
    *   *Descrição:* Apresenta o problema de negócio (redundância, falta de governança de metadados, latência de discovery) e os pilares fundamentais da solução. Delimita o escopo e os objetivos da plataforma.
*   **[Ciclo de Vida de Ingestão e Qualidade (docs/asset_lifecycle.md)](docs/asset_lifecycle.md)**
    *   *Descrição:* Especifica detalhadamente as regras de negócio de qualidade (quality gates), os estados operacionais de cada pipeline run (`running`, `success`, `failed`, `quality_failed`) e o fluxo de feedback com o Airflow.
*   **[Stakeholders e Governança de Acesso (docs/stakeholders.md)](docs/stakeholders.md)**
    *   *Descrição:* Mapeia os papéis dos usuários da plataforma (PO/PM, SRE, Analytics Engineer) com uma matriz de permissões rígida por Bearer Token e o fluxo detalhado de ativação de ativos.

### 🏗️ Arquitetura & Engenharia

*   **[Modelo de Ativos e Metadados (docs/business_assets.md)](docs/business_assets.md)**
    *   *Descrição:* Modela conceitualmente a separação entre `DataAsset` (regra lógica) e `Endpoint` (conectividade física via Vault/OpenBao). Define o processo de descoberta automatizada de metadados e classificação de schema drift.
*   **[Arquitetura do Sistema C4 (docs/architecture_c4.md)](docs/architecture_c4.md)**
    *   *Descrição:* Apresenta o diagrama arquitetural do projeto nos Níveis 1 (Contexto), 2 (Containers) e 3 (Componentes da API), detalhando os diagramas de fluxo de sequência para execuções e validações do Quality Gate.
*   **[Guia de Clean Code & DDD (docs/clean-code.md)](docs/clean-code.md)**
    *   *Descrição:* O manual técnico definitivo unificando as diretrizes de código limpo, separação de camadas do Hexágono, uso correto de Value Objects e Entidades do DDD, e padrões para a escrita de testes rápidos (F.I.R.S.T).

### ⚙️ Operação & DevOps

*   **[Guia de Operações Local (docs/operations_guide.md)](docs/operations_guide.md)**
    *   *Descrição:* Passo a passo para bootstrap do cluster via Docker Compose, operações comuns de banco de dados (`platform_db` vs `airflow`), comandos CLI e fluxo manual completo para registro e ativação de pipelines.
*   **[Guia de Automação de CI/CD (docs/ci_cd_guide.md)](docs/ci_cd_guide.md)**
    *   *Descrição:* Detalha o funcionamento do pipeline de Integração Contínua (CI) e Entrega Contínua (CD) no GitHub Actions, os gates de análise estática (Ruff, Mypy), a exclusão automática de testes E2E e a compilação/sincronização estática das DAGs.

---

## 🚀 Funcionalidades Principais

*   **Orquestração Declarativa:** DAGs do Airflow não são escritas manualmente; são compiladas estaticamente via CLI do projeto a partir de arquivos YAML de configuração.
*   **Isolamento pelo DDD:** Toda a lógica central (DataAssets, PipelineRuns) está protegida contra dependências de frameworks operacionais.
*   **Self-Healing & Drift Detection:** Descoberta automática de metadados. Mudanças críticas de schema na origem geram alertas automáticos de drift e podem bloquear execuções dependendo da política.
*   **Quality Gates Integrados:** Validações automatizadas contra nulos, unicidade, formato e integridade no final de cada execução, alterando o status do run para `quality_failed` se violado.
*   **Cofre de Credenciais:** Integração nativa com OpenBao (Vault) para resolução segura de credenciais em tempo de execução.

---

## 🛠️ Iniciando o Ambiente

### Simulação Local (Docker)
Suba todo o ecossistema com um único comando:

1.  **Inicializar ambiente:**
    ```bash
    docker compose up -d --build
    ```
2.  **Acessar ferramentas:**
    -   **Airflow UI:** `http://localhost:8080` (admin/admin)
    -   **Documentação Swagger (API):** `http://localhost:8000/docs`
    -   **OpenBao (Vault):** `http://localhost:8200` (token: `root`)

---

## 🧪 Suite de Testes

Os testes são divididos de forma inteligente para execução otimizada no CI:

```bash
# Executar apenas testes de unidade e integração (adequado para CI local)
.venv\Scripts\pytest -m "not e2e" -v

# Executar testes E2E (requer Docker rodando)
.venv\Scripts\pytest -m "e2e" -v
```
