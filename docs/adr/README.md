# Architecture Decision Records (ADRs)

Este diretório contém as decisões arquiteturais da plataforma de dados.
Cada ADR documenta o contexto, as alternativas consideradas e a decisão tomada.

## Formato

Cada ADR segue o template:
- **Status**: Proposed | Accepted | Deprecated | Superseded
- **Contexto**: Por que essa decisão foi necessária?
- **Alternativas Consideradas**: O que mais foi avaliado?
- **Decisão**: O que foi escolhido e por quê?
- **Consequências**: Quais trade-offs foram aceitos?

## Índice

| ADR | Título | Status |
|-----|--------|--------|
| [ADR-001](ADR-001-clean-architecture-ports-adapters.md) | Arquitetura Limpa (Ports & Adapters) e Domain-First Design | Aprovado |
| [ADR-002](ADR-002-metadata-database-engine.md) | Motor de Banco de Dados de Metadados (SQLite/Dev vs PostgreSQL/Prod) | Aprovado |
| [ADR-003](ADR-003-dag-generation-shared-volume.md) | Modelo de Geração de DAGs via Filesystem Compartilhado e Jinja2 Templates | Aprovado |
| [ADR-004](ADR-004-compute-engine-duckdb-vs-spark.md) | Motor de Compute: DuckDB vs Spark | Aprovado |
| [ADR-005](ADR-005-secret-management-openbao.md) | Gerenciamento de Secrets: OpenBao | Aprovado |
| [ADR-006](ADR-006-api-versioning-strategy.md) | Estratégia de Versionamento de API | Aprovado |
| [ADR-007](ADR-007-security-identity-jwt-rbac.md) | Autenticação JWT Assimétrica (RS256) e RBAC Granular com Cache In-Memory | Aprovado |
| [ADR-008](ADR-008-observability-readiness-strategy.md) | Estratégia de Monitoramento e Observabilidade com Prometheus e Probes Dedicados | Aprovado |
| [ADR-009](ADR-009-etl-pipeline-quality-gates.md) | Garantias de Rigor e Qualidade (Quality Gates) em Pipelines de ETL | Aprovado |
| [ADR-010](ADR-010-resilience-circuit-breaker.md) | Resiliência em Integrações de API e Repositórios usando Circuit Breakers | Aprovado |
| [ADR-011](ADR-011-mongodb-schema-discovery.md) | Estratégia de Schema Discovery Híbrido para MongoDB (Validator vs Sampling) | Aprovado |
