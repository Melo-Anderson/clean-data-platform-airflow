# SDD Airflow Data Platform

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Airflow-3.0_Ready-green)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-teal)
![Architecture](https://img.shields.io/badge/Architecture-DDD_%7C_Clean-purple)

An advanced, decoupled Data Platform built around **Airflow 3**, leveraging **Domain-Driven Design (DDD)** and **Clean Architecture** to ensure maintainability, testability, and scalability.

## 🚀 Key Features

- **Decoupled Airflow Architecture:** Instead of Airflow dynamically parsing heavy YAML files (which overloads the scheduler), this platform uses a dedicated CLI to statically compile YAML configuration into native Python DAGs (`dags/`).
- **Domain-Driven Design (DDD):** The core business rules (Pipeline Runs, Discovery Runs, Lineage Graphs) are entirely isolated from external frameworks.
- **Self-Healing Data Discovery:** Automated metadata inference, PII tagging, and schema drift detection. Critical schema changes trigger approval workflows before automatically evolving the platform.
- **Column-Level Lineage Graph:** A dedicated directed acyclic graph (DAG) implementation for upstream and downstream lineage traversal, ready for DataHub or OpenMetadata integration.
- **Async-First API:** High-performance REST APIs built with FastAPI to interact with platform metadata, trigger discoveries, and query lineage graphs.

## 🧠 Architecture Highlights

1. **`app/domain/`**: Pure Python dataclasses representing business concepts (`LineageGraph`, `DiscoveryRun`, `DataAsset`). Completely agnostic to infrastructure.
2. **`app/application/`**: Use cases (e.g., `PublishLineageToCatalogUseCase`) that orchestrate domain entities. Depends strictly on abstract protocols (like `UnitOfWork`).
3. **`app/infrastructure/`**: Concrete implementations of databases (SQLAlchemy async mappers), external catalog adapters (`DataHubCatalogAdapter`), and the FastAPI web layer.
4. **`cli/`**: Typer-based CLI for pipeline compilation and discovery operations.

## 🛠 Getting Started

### Local Simulation (Docker)

We provide a complete Docker Compose environment to simulate the platform locally.

1. **Start the environment:**
   ```bash
   docker compose up -d
   ```
2. **Access the tools:**
   - **Airflow UI:** `http://localhost:8080` (admin/admin)
   - **Platform API:** `http://localhost:8000/docs`

### Operations & Usage

For a comprehensive guide on how to build DAGs, run discovery, and evolve the platform, please refer to our **[Operations Guide](docs/operations_guide.md)**.

## 🧪 Testing

The platform enforces strict type checking and maintains exhaustive test coverage.

```bash
uv run pytest tests/ -v
make type-check
```

## 🤝 Contributing

When contributing to this repository, please ensure that your changes respect the **Clean Architecture** boundaries. Business logic must remain in `app/domain` and `app/application` without importing third-party operational frameworks (like FastAPI or SQLAlchemy).
