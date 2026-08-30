# ADR-015: Motor de Ingestão de Arquivos de Alta Performance (OmniBeam em Go + Apache Beam)

## Status
Aceito (Accepted)

## Contexto
A plataforma necessita processar lotes contínuos de arquivos semiestruturados (JSON, NDJSON) e estruturados (CSV, TSV) originados de sistemas transacionais e de terceiros na zona de landing.
Abordagens tradicionais baseadas em scripts Python monolíticos ou PySpark para arquivos de pequeno a médio porte (dezenas a centenas de megabytes) incorrem em alto consumo de memória, overhead de inicialização de JVM/clusters e lentidão no parsing de strings complexas.

## Decisão
Adotamos o **OmniBeam**, um motor compilado nativo em Go baseado no SDK do Apache Beam (Direct Runner), desacoplado via porta `ComputeJobAdapter` (`OmniBeamComputeAdapter`):
1. **Compilação Nativa e Baixo Consumo de Recursos:** Binário estático embutido nos containers de worker do Airflow (`/usr/local/bin/omnibeam-pipeline`), com inicialização quase instantânea e consumo mínimo de memória.
2. **Pipelines de Transformação Concorrente:** Leitura de arquivos de entrada com parsing paralelo, conversão para esquema Arrow/Parquet tipado e escrita de arquivos consolidados no diretório de staging.
3. **Injeção Automática de Metadados:** Injeção das colunas técnicas de auditoria `_ingested_at` (timestamp UTC) e `_source_file` (nome do arquivo original) diretamente no Parquet de saída.
4. **Idempotência e Rastreabilidade:** Emissão de `metrics.json` contendo row count, byte size e checksum MD5 para registro individual em `PipelineRunFile`.

## Consequências
### Positivas
- Alta vazão de processamento para arquivos JSON e CSV sem necessidade de infraestrutura pesada (Spark).
- Idempotência granular por arquivo e governança total via metadados de auditoria.
- Compatibilidade direta com o `BigQueryDwhLoader` para carregamento em lote no dataset Bronze.

### Negativas / Mitigações
- Para arquivos na casa de múltiplos terabytes por partição, a esteira pode ser escalada via Beam Runners distribuídos (ex: Google Cloud Dataflow) reutilizando a mesma especificação de pipeline.
