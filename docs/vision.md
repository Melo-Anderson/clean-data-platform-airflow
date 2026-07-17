# Visão da Plataforma de Dados

## Problema

As empresas que possuem múltiplas fontes de dados heterogêneas (bancos relacionais, APIs REST, buckets de armazenamento, sistemas legados) enfrentam três desafios recorrentes:

1. **Redundância de engenharia**: cada nova ingestão é construída do zero, sem reaproveitamento.
2. **Falta de observabilidade**: não há visibilidade centralizada de quando um dado foi atualizado pela última vez, se houve falha ou se a qualidade está dentro do esperado.
3. **Ausência de governança automática**: mudanças no schema de origem não são detectadas nem classificadas, causando falhas silenciosas nos pipelines downstream.

## Solução

A **Plataforma de Dados** é uma plataforma de orquestração de pipelines guiada por metadados. Em vez de escrever DAGs manualmente para cada pipeline, o engenheiro de dados registra um `DataAsset` e um `Pipeline` via API, e a plataforma gera e gerencia toda a infraestrutura de execução de forma automática.

### Pilares

| Pilar | Descrição |
|---|---|
| **Orquestração Declarativa** | Pipelines são declarados via API REST. DAGs do Airflow são geradas automaticamente via templates Jinja2. |
| **Discovery Automático** | Ao ativar um Asset, a plataforma varre a fonte (relacional ou NoSQL como MongoDB) e detecta tabelas, colunas, tipos e desvios de schema. |
| **Controle de Qualidade (Quality Gate)** | Após cada execução, métricas são avaliadas contra regras configuradas. O run é marcado como `quality_failed` se houver violações. |
| **Observabilidade Operacional** | Cada execução gera um `PipelineRun` com status, duração, falhas e métricas. Um dashboard de saúde fica sempre disponível. |
| **Portabilidade de Compute** | O motor de processamento é plugável via protocolo `ComputeJobAdapter`. A plataforma suporta Spark, Dataflow e DuckDB. |

## Objetivos

- Reduzir desenvolvimento manual de pipelines de engenharia de dados
- Facilitar onboarding de novas fontes sem reescrever DAGs
- Padronizar ingestão, transformação e exportação de dados
- Garantir qualidade dos dados via quality gates configuráveis
- Entregar observabilidade nativa de todas as execuções

## Fora de Escopo

- BI / Dashboards de negócio
- Machine Learning / Feature Store
- Catálogo corporativo externo (ex: Dataplex, DataHub)
- Streaming / Real-time (plataforma é batch-first)
