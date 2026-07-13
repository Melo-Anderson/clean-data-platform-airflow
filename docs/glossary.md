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

## Schema Drift
Desvio estrutural identificado pelo processo de Discovery ao comparar a estrutura física atual da fonte de dados com a última versão registrada no catálogo de schemas (`CatalogSchemaVersion`). Pode ser classificado como Informativo, Compatível ou Crítico.

## Unit of Work (UoW)
Padrão de design que agrupa operações de banco de dados em uma única transação lógica, garantindo atomicidade (tudo é persistido com sucesso ou revertido via rollback).

## Portas e Adaptadores (Hexagonal)
Abstração que isola a lógica central da plataforma de I/O de infraestrutura. A porta é a interface (`Protocol`), e o adaptador é a implementação concreta (ex: `BaoSecretManagerAdapter` acoplado na porta `SecretManagerPort`).
