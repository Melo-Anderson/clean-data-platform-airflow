# ADR-001: Motor de Compute — DuckDB vs Apache Spark

**Status:** Accepted  
**Data:** 2026-07-10  

## Contexto

A plataforma precisa de um motor de execução de jobs de transformação de dados
(ETL, ingestão, exportação) que seja simples de operar em desenvolvimento local
e escalável em produção.

## Alternativas Consideradas

| Alternativa | Prós | Contras |
|-------------|------|---------|
| **Apache Spark** | Escala horizontal, ecossistema maduro | Requer cluster, operação complexa, latência de startup |
| **Google Cloud Dataflow** | Gerenciado, integra com GCP | Lock-in de vendor, custo por job |
| **DuckDB** | Embutido, zero infra, SQL completo, suporte a Parquet | Single-node (sem escala horizontal nativa) |

## Decisão

Usar **DuckDB como motor padrão** para desenvolvimento e workloads single-node.
A interface `ComputeJobAdapter` (Protocol) garante que qualquer motor pode ser
substituído sem alterar DAGs ou Use Cases.

## Consequências

- ✅ Zero infraestrutura adicional para dev e pipelines de médio porte
- ✅ Troca de motor sem reescrita de código (polimorfismo via Protocol)
- ⚠️ Jobs acima de ~50GB de dados precisarão migrar para Spark/Dataflow
- ⚠️ Paralelismo é limitado à memória da máquina host
