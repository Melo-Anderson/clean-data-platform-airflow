# Glossário da Plataforma de Dados

## DataAsset
Entidade conceitual de negócio estável que agrupa informações de governança (descrições, tags de segurança) e faz referência a um Endpoint de origem. É cadastrado por perfis de negócio (PO, PM, Analytics Engineer) e governa a linhagem e permissões do ativo.

## Endpoint
Representação técnica que armazena os dados físicos de conectividade (URLs, hosts, portas) e referências a credenciais de segurança em cofres externos.

## Metadata Discovery (Autodescoberta)
Mecanismo automático ativado no cadastro do DataAsset para varrer o Endpoint e inferir sua estrutura técnica (schemas, constraints, chaves, arquivos).

## DataObject
Representa uma tabela, arquivo, coleção ou entidade lógica sob a jurisdição de um DataAsset.

## DataElement
Atributo ou campo individual contido em um DataObject.

## Estados de Ciclo de Vida (Lifecycle States)
Estados governados de um DataAsset ao longo de sua existência: `Draft`, `Active`, `Deprecated` e `Archived`.

## Pipeline
Definição conceitual da rotina de dados (Ingestão, Transformação/ETL ou Exportação) associada a um DataAsset, definindo a frequência (cron), o motor de processamento (compute config) e as regras de qualidade a serem testadas.

## PipelineRun
Instância que rastreia uma execução específica de um Pipeline em tempo real. Possui estados operacionais (`running`, `success`, `failed`, `quality_failed`) e registra métricas físicas (ex: linhas gravadas) e violações.

## QualityGate
Mecanismo de governança executado ao final de cada PipelineRun. Ele avalia as métricas físicas coletadas contra as regras de qualidade (`QualityRule`) definidas no Pipeline, alterando o status do run para `quality_failed` se houver violações críticas.

## DiscoveryRun
Instância executada de um ciclo de autodescoberta de metadados para um `DataAsset`. Rastreia os estados (`pending`, `running`, `completed`, `failed`) e armazena os metadados varridos.

## SchemaSnapshot
Fotografia pontual e imutável da estrutura técnica de colunas, tipos e restrições de um objeto em um `DiscoveryRun`, servindo como baseline para comparação de drift.

## DriftApproval
Registro de governança que retém uma alteração crítica de schema até a aprovação formal por um perfil autorizado (SRE/PO) via API.

## DriftEvent
Evento individual de alteração de schema identificado na comparação entre o `SchemaSnapshot` atual e o baseline anterior.

## PolicyTagSuggestion
Sugestão de tag de classificação de segurança e privacidade (ex: PII, CPF, Cartão de Crédito) inferida automaticamente a partir do nome ou tipo do campo.

## Harness Engine
Motor de geração declarativa de pipelines a partir de prompts em linguagem natural, utilizando um grafo de estados LangGraph com guardrails de validação HTTP externa.

## QualityGate
Mecanismo de governança executado ao final de cada PipelineRun. Ele avalia as métricas físicas coletadas contra as regras de qualidade (`QualityRule`) definidas no Pipeline, alterando o status do run para `quality_failed` se houver violações críticas.

## Schema Drift
Desvio estrutural identificado pelo processo de Discovery ao comparar a estrutura física atual da fonte de dados com a última versão registrada no catálogo de schemas via `SchemaSnapshot`. Categorizado em:
- **Informativo:** Alteração sem impacto em tipos ou colunas (ex: atualização de comentário/descrição). Não bloqueia pipelines.
- **Compatível:** Adição de colunas opcionais (`NULLABLE`). Notifica os responsáveis e permite execução.
- **Crítico:** Alterações que quebram o código consumidos (ex: remoção de colunas, alteração de tipo de dados ou chave primária). Bloqueia a execução do pipeline até aprovação explícita via `DriftApproval`.

## Unit of Work (UoW)
Padrão de design que agrupa operações de banco de dados em uma única transação lógica, garantindo atomicidade (tudo é persistido com sucesso ou revertido via rollback).

## Portas e Adaptadores (Hexagonal)
Abstração que isola a lógica central da plataforma de I/O de infraestrutura. A porta é a interface (`Protocol`), e o adaptador é a implementação concreta (ex: `BaoSecretManagerAdapter` acoplado na porta `SecretManagerPort`).

## OmniBeam Engine
Motor proprietário de ingestão e processamento de arquivos em streaming/batch, desenvolvido em Go e Apache Beam, responsável pelo parsing de alta performance de arquivos semiestruturados (JSON) e estruturados (CSV) para Parquet com injeção automática de metadados de auditoria.

## dbt Core Transformation
Mecanismo de transformação de dados modular baseado em SQL utilizando dbt Core 1.12+, permitindo testes automatizados de dados, compilação de linhagem e materialização de views e tabelas dimensionais e analíticas.

## Medallion Architecture (Arquitetura Medalhão)
Padrão de organização em camadas de dados:
- **Bronze (Raw):** Dados brutos ingeridos com metadados de auditoria (`_ingested_at`, `_source_file`) e rastreamento de arquivo.
- **Staging (Views):** Camada de tipagem segura (`SAFE_CAST`) e padronização com custo zero de storage.
- **Silver (Clean/Deduplicated):** Tabelas físicas limpas e deduplicadas via `QUALIFY ROW_NUMBER() = 1`, particionadas e clusterizadas.
- **Gold (Analytics/Dimensional):** Modelagem dimensional Kimball (`dim_*`, `fct_*`) e tabelas especializadas para detecção de fraudes (`gold_fraud_alerts`).

## Airflow 3 Asset Scheduling
Modelo de agendamento reativo orientado a dados no Airflow 3, onde DAGs declaram ativos produzidos via `outlets` (`platform://asset/<name>`) e DAGs dependentes são disparadas automaticamente via `schedule=[Asset(...)]` sem necessidade de agendamentos cron sobrepostos.

## PipelineRunFile
Entidade de rastreamento individual de arquivos processados por uma execução de pipeline, registrando o caminho físico, nome, tamanho em bytes, timestamp de modificação (`mtime`), hash MD5 para idempotência e status operacional (`PROCESSED`).

## Surrogate Key (MD5 Determinística)
Chave artificial única gerada por hash MD5 determinístico a partir de chaves naturais de negócio (via `dbt_utils.generate_surrogate_key`), garantindo integridade e consistência dimensional no Data Warehouse.
