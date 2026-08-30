# ADR-016: Pipelines de Transformação dbt Core e Arquitetura Medalhão com Orquestração Reativa

## Status
Aceito (Accepted)

## Contexto
Após a ingestão dos dados brutos na camada Bronze do Data Warehouse (BigQuery), faz-se necessária uma esteira de transformação modular, testável e versionável para limpar, deduplicar, padronizar e construir modelos dimensionais e analíticos (detecção de fraude).
Além disso, disparar transformações por horários fixos (cron) cria atrasos desnecessários ou riscos de concorrência com ingestões em andamento.

## Decisão
Adotamos o **dbt Core (1.12+)** como motor padrão para pipelines de tipo `transformation`, estruturado em Arquitetura Medalhão e orquestrado reativamente por **Airflow 3 Assets**:
1. **Camadas Medallion:**
   - **Staging (`stg_*`):** Views virtuais no BigQuery com tipagem defensiva (`SAFE_CAST`) e padronização.
   - **Silver (`slv_*`):** Tabelas físicas deduplicadas com `QUALIFY ROW_NUMBER() = 1`, particionadas por `_ingested_at` e clusterizadas por chaves de negócio.
   - **Gold (`dim_*`, `fct_*`, `gold_fraud_alerts`):** Modelagem dimensional Kimball com Surrogate Keys determinísticas (geradas via MD5 hash) e tabelas analíticas para detecção de anomalias e tipologias de fraude (Multi-Accounting, CPA Farming, Velocity Deposit Spikes, Immediate Withdrawal without Play).
2. **Orquestração Reativa por Eventos:**
   - DAGs de transformação no Airflow 3 declaram `schedule=[Asset("platform://asset/<source>")]` e emitem `outlets=[Asset("platform://asset/<dest>")]`.
   - Encadeamento automatizado em cascata: `Bronze Ingestion ➔ Silver ETL ➔ Gold Analytics`.
3. **Quality Gates Integrados:**
   - Avaliação do arquivo de execução `run_results.json` para transformar falhas de testes singulares e de esquema dbt em reprovação do Quality Gate da plataforma.

## Consequências
### Positivas
- Engenharia de transformação declarativa, versionada e modular com documentação e testes embutidos.
- Eliminação do acoplamento temporal: pipelines rodam assim que os dados a montante ficam prontos.
- Isolamento total de regras de negócio analíticas em SQL padrão dbt.

### Negativas / Mitigações
- Exige compilação prévia de manifests dbt (`manifest.json`) para inspeção de linhagem na API da plataforma (mitigado pelo adaptador `DbtManifestParser`).
