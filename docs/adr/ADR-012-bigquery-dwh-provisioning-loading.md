# ADR 012: Provisionamento Automático DWH e Carga em Lote com BigQuery (ADC & Clean Arch)

## Status
Aprovado

## Contexto
Para garantir o ciclo de vida completo de governança de dados da plataforma, a gravação de metadados no banco relacional da plataforma (`platform_db`) deve refletir a infraestrutura física no Data Warehouse (DWH).

Anteriormente, o registro de `DataAsset` e `Pipeline` criava apenas registros em banco de dados, sem provisionar os datasets e tabelas reais no Google BigQuery. Além disso, as tarefas de execução necessitavam de uma implementação física de carregamento em lote (`DwhLoaderAdapter`) resiliente, capaz de carregar arquivos Parquet/Avro em staging de forma nativa e segura, sem expor chaves de serviço no código fonte ou no controle de versão.

## Decisão

Adotamos a seguinte arquitetura para integração com o Google BigQuery e Cloud DWHs:

1. **Desacoplamento via Clean Architecture (`DwhProvisionerAdapter`)**:
   - Criamos o contrato `DwhProvisionerAdapter` (`ensure_dataset_exists` e `ensure_table_exists`) em `app/application/shared/adapters/dwh_provisioner_adapter.py`.
   - `RegisterAssetUseCase` aciona `ensure_dataset_exists` para provisionar o Dataset no BigQuery no cadastro do Asset, aplicando labels de governança (`managed_by`, `owner`, `tags`).
   - `RegisterPipelineUseCase` aciona `ensure_table_exists` para provisionar a Tabela e seu schema técnico no cadastro do Pipeline.

2. **Carregamento em Lote Nativo (`BigQueryDwhLoader`)**:
   - Implementamos a carga física via `google.cloud.bigquery.Client.load_table_from_uri` a partir do caminho de staging (GCS/local Parquet/Avro), utilizando a disposição `WRITE_APPEND`.
   - O adaptador suporta fallback gracioso de ambiente (`DummyBQ`) para que a plataforma e a suíte de testes continuem operantes mesmo em ambientes leves sem o pacote `google-cloud-bigquery` pré-instalado.

3. **Política Estrita de Credenciais Zero (ADC & Workload Identity)**:
   - Toda inicialização de cliente BigQuery utiliza **Application Default Credentials (ADC)** em produção e Cloud Run/GKE/Composer, eliminando o uso de senhas ou chaves JSON salvas na plataforma.
   - Em desenvolvimento local, se utilizado arquivo de Service Account, a variável `GOOGLE_APPLICATION_CREDENTIALS` deve apontar obrigatoriamente para um diretório fora da pasta do projeto.
   - O `.gitignore` foi fortalecido com bloqueios explícitos (`*-sa-key.json`, `*credentials*.json`, `*service_account*.json`).

## Consequências
- **Positivas**:
  - **Governança Física e Linhagem desde o Dia Zero**: Datasets e tabelas existem no BigQuery assim que o Asset e Pipeline são cadastrados na API.
  - **Segurança Máxima**: Nenhuma chave privada de Service Account é exposta ou versionada no Git.
  - **Alta Performance de Ingestão**: O carregamento utiliza a API nativa de lote do BigQuery (`load_table_from_uri`), ideal para grandes volumes de dados.
- **Negativas**:
  - Requer que a conta de serviço do ambiente (ou usuário via `gcloud auth application-default login`) possua as permissões de IAM necessárias (`bigquery.datasets.create`, `bigquery.tables.create`, `bigquery.jobs.create`) no projeto GCP configurado via `PLATFORM_GCP_PROJECT`.
