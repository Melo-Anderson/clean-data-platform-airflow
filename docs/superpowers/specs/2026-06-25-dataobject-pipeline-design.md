# Spec: DataObject, DataElement e Pipeline — Definição Conceitual, Geração de DAG e Observabilidade

**Data:** 2026-06-25
**Autor:** Analytics Engineer
**Status:** Em revisão
**Relacionado:** [2026-06-25-data-asset-design.md](./2026-06-25-data-asset-design.md)

---

## 1. Contexto e Escopo

Este documento especifica o modelo conceitual de `DataObject` e `DataElement`, o mecanismo de geração de `Pipeline` (DAG no Airflow) a partir de configuração YAML, o ciclo de self-healing baseado em schema evolution, as validações de CI, e as práticas de observabilidade e monitoração da plataforma.

**Convenção de nomenclatura:** identificadores, enums, chaves YAML e termos técnicos da plataforma em inglês. Documentação em português.

**Fora de escopo neste documento:**
- Métricas de negócio e dashboards de BI.
- Configuração de infraestrutura de workers (Spark/Dataflow provisionado fora da plataforma).

---

## 2. Entidades

### 2.1 DataObject

Representa uma entidade lógica de dados vinculada a um `DataAsset`. Pode ser uma tabela, arquivo, coleção, recurso de API ou fluxo ETL. Distingue explicitamente o papel de **origem** (`SOURCE`) do papel de **destino** (`DESTINATION`).

| Atributo | Tipo | Descrição |
|---|---|---|
| `id` | UUID | Identificador único |
| `asset_id` | UUID (ref) | DataAsset pai |
| `name` | String | Nome do objeto (ex: `customers`, `orders.parquet`) |
| `type` | Enum | `TABLE` \| `VIEW` \| `FILE` \| `API_RESOURCE` \| `COLLECTION` |
| `role` | Enum | `SOURCE` \| `DESTINATION` \| `BOTH` |
| `description` | String | Preenchida pelo AE ou herdada do Discovery (`auto_generated: true`) |
| `policy_tags` | Lista[Enum] | Herdadas do DataAsset pai; refinadas por DataElement |
| `pipeline_id` | UUID (ref) | Pipeline gerada no cadastro |
| `last_run` | Timestamp | Última execução de pipeline |
| `last_success` | Timestamp | Última execução bem-sucedida |
| `freshness_status` | Enum | `FRESH` \| `STALE` \| `UNKNOWN` |

**Regra de cadastro por tipo de pipeline:**

| Tipo de Pipeline | Origem | Destino |
|---|---|---|
| `ingestion` | Definido pelo AE a partir do Discovery | Auto-gerado pela plataforma (schema espelhado) |
| `etl` | Declarado explicitamente (pode ser N objetos) | Declarado explicitamente; criado se não existir |
| `export` | Declarado explicitamente | API, arquivo ou tabela — declarado explicitamente |

---

### 2.2 DataElement

Campo ou atributo individual de um `DataObject`. Herdado do Discovery em ingestões; definido manualmente ou calculado em ETL/export.

| Atributo | Tipo | Fonte | Override pelo AE |
|---|---|---|---|
| `name` | String | Discovery / Manual | Não (auditável) |
| `source_type` | Enum | Discovery | Não (imutável para rastreabilidade) |
| `destination_type` | Enum | Discovery → override | **Sim** — AE pode forçar tipo no destino |
| `required` | Boolean | Discovery → override | **Sim** — AE pode forçar no destino |
| `nullable` | Boolean | Discovery → override | **Sim** |
| `description` | String | Discovery (auto) → editável | **Sim** |
| `policy_tag` | Enum | Inferida pelo Discovery → confirmada | **Sim** |
| `auto_generated` | Boolean | Indica se descrição/tag foi inferida automaticamente | — |
| `is_computed` | Boolean | `true` para campos calculados em ETL (sem `source_type`) | — |

**Regra de override destrutivo:** sobrescrever `destination_type` de forma incompatível com `source_type` (ex: `STRING → INTEGER`) gera aviso no CI e exige confirmação explícita do AE antes do deploy.

---

## 3. Pipeline

Uma `Pipeline` é gerada automaticamente quando o Analytics Engineer finaliza o cadastro de um `DataObject`. É a representação da DAG no Airflow, derivada do YAML de configuração versionado no Git.

### 3.1 Tipos de Pipeline

| `type` | Descrição |
|---|---|
| `ingestion` | Extrai dados de uma origem e carrega em um destino auto-gerado (DWH) |
| `etl` | Transforma dados de N origens para M destinos, orquestrando dbt ou Dataform |
| `export` | Extrai dados processados para API, arquivo ou sistema externo |

### 3.2 YAML de Configuração

O YAML é a fonte de verdade da pipeline. Ele é gerado pelo formulário da plataforma — **nenhum usuário edita YAML diretamente**. O YAML versionado no Git é o artefato que o CI/CD usa para gerar a DAG Python correspondente.

```yaml
schema_version: "1.0"

pipeline:
  id: "uuid"
  name: "oracle_customers_to_bigquery"
  type: "ingestion"           # ingestion | etl | export
  owner: "analytics_engineer_email"

  schedule:
    mode: "cron"              # cron | trigger | trigger_with_gate
    cron: "0 6 * * *"
    depends_on:               # usado em trigger e trigger_with_gate
      - pipeline_id: "uuid-upstream-1"
        require_same_day: true  # gate: upstream deve ter rodado no mesmo dia calendário

  source:
    asset_id: "uuid-asset"
    objects:
      - object_id: "uuid-obj-source"
        load_strategy: "incremental"  # full_load | incremental | cdc
        watermark_column: "updated_at"
        page_size: 1000
        partition_column: "created_at"
        compression: "snappy"
        encoding: "utf-8"
        sensor_query: |              # opcional — query que valida pré-condição de extração
          SELECT CASE WHEN status = 'DONE' THEN 1 ELSE 0 END
          FROM batch_control
          WHERE process_date = CURRENT_DATE
          AND process_name = 'DAILY_CLOSE'
        sensor_query_timeout_minutes: 120  # tempo máximo aguardando retorno verdadeiro
        sensor_query_poke_interval_seconds: 60
        extraction_query: |          # opcional — sobrescreve a query gerada automaticamente
          SELECT id, name, email, updated_at
          FROM customers
          WHERE region = 'LATAM'
          AND deleted_at IS NULL

  destination:
    asset_id: "uuid-dest-asset"     # omitido em ingestion (auto-gerado)
    objects:
      - object_id: "uuid-obj-dest"
        create_if_not_exists: true

  transform:
    engine: "dbt"                   # dbt | dataform | none
    ref: "models/dim_customers.sql" # referência ao arquivo no repositório Git

  compute:
    engine: "spark"                 # spark | dataflow | default
    config:
      num_workers: 4
      machine_type: "n1-standard-4"

  quality:
    metrics:
      - type: "not_null"
        column: "customer_id"
      - type: "row_count_min"
        value: 1000
      - type: "unique"
        column: "customer_id"

  airflow:
    retries: 3
    retry_delay_minutes: 5
    execution_timeout_minutes: 120
    sla_minutes: 90
    tags: ["core", "customers", "daily"]
    pool: "default_pool"

  discovery_task:
    enabled: true
    on_critical_change: "block"     # block | self_heal | alert_only
```

---

### 3.3 Modos de Ativação

| `schedule.mode` | Comportamento |
|---|---|
| `cron` | Executa no horário configurado, independente de outras pipelines |
| `trigger` | Executa após sucesso confirmado de todas as pipelines em `depends_on` |
| `trigger_with_gate` | Executa 1x/dia no cron configurado, mas só avança se todas as pipelines em `depends_on` já completaram com sucesso no mesmo dia calendário |

---

### 3.4 Estrutura da DAG Gerada

```
[discovery_check]
       │
       ▼
  [sensor_query_1?]  [sensor_query_2?]  ...  (opcional — aguarda pré-condição por objeto)
       │                    │
       └────────┬───────────┘
                ▼
  [extract_source_1]  [extract_source_2]  ...  (paralelo por objeto de origem)
       │                    │
       └────────┬───────────┘
                ▼
         [transform]          (dbt/Dataform ou none)
                │
                ▼
           [load_destination]
                │
                ▼
         [quality_check]
                │
                ▼
       [emit_lineage_event]
```

> Tasks `sensor_query` são geradas apenas quando `sensor_query` está definido no objeto de origem. Cada objeto de origem tem seu próprio sensor independente — um objeto pode iniciar a extração mesmo que outro ainda esteja aguardando seu sensor (configurável via `wait_for_all_sensors: true|false` na pipeline).

---

### 3.5 Rebuild de DAGs

Quando o template de DAG é atualizado para uma nova versão, a plataforma oferece o comando de rebuild:

```bash
platform pipeline rebuild --template-version 2.0 [--dry-run]
```

- Lê todos os YAMLs versionados no repositório.
- Aplica migrações automáticas para YAMLs com `schema_version` defasada.
- Regenera as DAGs Python com o novo template.
- `--dry-run` exibe o diff completo sem aplicar — recomendado para revisão via PR antes do deploy em produção.

**Compatibilidade de schema_version:** cada versão do template declara a versão mínima de YAML suportada. YAMLs abaixo do mínimo são migrados automaticamente ou sinalizados para revisão manual.

---

### 3.6 Campos de Extração Avançada por Objeto de Origem

#### `sensor_query` — Sensor de Pré-condição de Negócio

Query SQL definida pelo Analytics Engineer que é executada periodicamente na origem **antes** de iniciar a extração. A task de sensor avança apenas quando a query retorna um valor verdadeiro (`1`, `TRUE`, ou resultado não vazio).

Uso típico: origens com processamento batch onde a extração só é segura após o fechamento do ciclo (ex: fechamento diário de caixa, carga de staging concluída, arquivo gerado).

| Campo relacionado | Tipo | Padrão | Descrição |
|---|---|---|---|
| `sensor_query` | SQL String | `null` | Query de validação. Ausente = sem sensor. |
| `sensor_query_timeout_minutes` | Integer | `60` | Tempo máximo aguardando retorno verdadeiro. Ao expirar, a task falha. |
| `sensor_query_poke_interval_seconds` | Integer | `60` | Intervalo entre re-execuções da query no sensor. |

**Regra:** o `sensor_query_timeout_minutes` não pode exceder `airflow.execution_timeout_minutes` — validado no CI.

#### `extraction_query` — Query Customizada de Extração

Permite ao Analytics Engineer substituir a query de extração gerada automaticamente pela plataforma por uma query customizada. Útil quando a extração padrão não atende (ex: joins necessários na origem, filtros de negócio complexos, views não expostas pelo Discovery).

| Campo | Tipo | Padrão | Descrição |
|---|---|---|---|
| `extraction_query` | SQL String | `null` | Query customizada. Ausente = plataforma gera automaticamente com base em `load_strategy` e `watermark_column`. |

**Regras:**
- Quando `extraction_query` está definido, `load_strategy: incremental` ainda é respeitado — o AE é responsável por incluir o filtro de watermark na query customizada.
- A plataforma valida a sintaxe SQL estaticamente no CI, mas **não executa** a query na origem durante o CI.
- Colunas selecionadas na `extraction_query` devem corresponder aos DataElements declarados no DataObject. Divergências geram aviso no CI.

---

## 4. Self-Healing e Schema Evolution

### 4.1 Discovery Task

A primeira task de cada DAG executa uma varredura leve na origem (sem queries pesadas) para detectar mudanças estruturais antes da extração.

### 4.2 Classificação de Mudanças e Ações

| Mudança detectada na origem | Classificação | `self_heal` | `block` | Sempre |
|---|---|---|---|---|
| Campo novo adicionado | Informativa | Adiciona no destino + alerta | Adiciona + alerta | — |
| Campo removido | Informativa | Mantém no destino como nullable + alerta | Aborta + revisão no catálogo | — |
| Alargamento de tipo (`INT → BIGINT`) | Informativa | Altera schema no destino + alerta | Alerta sem alterar | — |
| Tabela / arquivo novo na origem | Informativa | Registra no catálogo como `unmapped` | Registra + alerta | — |
| Tipo incompatível (`STRING → INTEGER`) | **Crítica** | — | — | **Sempre bloqueia** + aprovação obrigatória |
| `nullable → required` | **Crítica** | — | — | **Sempre bloqueia** + aprovação obrigatória |
| Origem indisponível | **Crítica** | — | — | Aborta + alerta + retries conforme `airflow.retries` |

> **Mudanças críticas** sempre bloqueiam, independente do valor de `on_critical_change`. A aprovação é feita pelo owner do DataAsset no catálogo de metadados — não requer intervenção manual no Airflow.

---

## 5. Validações no CI

O CI executa antes do deploy, respeitando a **margem de segurança** (sem conectividade real à origem):

| Validação | O que verifica |
|---|---|
| **YAML schema** | Campos obrigatórios presentes, tipos corretos, `schema_version` compatível |
| **Referências** | `asset_id`, `object_id`, `pipeline_id` em `depends_on` existem no catálogo |
| **Ciclos de dependência** | `depends_on` não forma grafo circular entre pipelines |
| **Cron syntax** | Expressão cron válida e não conflitante com `trigger_with_gate` |
| **Compute config** | Valores dentro dos limites definidos pela plataforma |
| **Transform ref** | Arquivo declarado em `transform.ref` existe no repositório Git |
| **DAG gerada** | DAG Python resultante é importável sem erros de sintaxe no Airflow |
| **Quality rules** | Regras referenciam colunas existentes nos DataElements declarados |
| **Override destrutivo** | Aviso quando `destination_type` incompatível com `source_type` |
| **Sensor query syntax** | `sensor_query` é SQL válido sintaticamente (parse estático, sem execução) |
| **Extraction query syntax** | `extraction_query` é SQL válido sintaticamente (parse estático, sem execução) |
| **Sensor timeout** | `sensor_query_timeout_minutes` ≤ `airflow.execution_timeout_minutes` |

> **Fora do CI** (executado apenas em runtime): conectividade real, autenticação, volume de dados, performance de extração, resultado real do `sensor_query`.

---

## 6. Observabilidade e Monitoração

### 6.1 Camadas de Observabilidade

```
┌─────────────────────────────────────────────────────────┐
│ L1 — Infraestrutura                    (SRE)            │
│  CPU, memória, disco, latência de rede dos workers      │
├─────────────────────────────────────────────────────────┤
│ L2 — Orquestração                      (SRE + AE)       │
│  DAG runs, task durations, success/failure rates        │
│  SLA breaches, retries, queue depth do Airflow          │
├─────────────────────────────────────────────────────────┤
│ L3 — Pipeline                          (Analytics Eng.) │
│  Rows processed, bytes transferidos, tempo por task     │
│  Schema drift detectado, self-heal aplicado             │
│  Load strategy executada, partições processadas         │
├─────────────────────────────────────────────────────────┤
│ L4 — Qualidade de Dados                (AE + PO/PM)     │
│  Quality metrics por pipeline                           │
│  Rejeições na Trusted Zone, anomalias de volume         │
│  Alertas de drift confirmados/pendentes de aprovação    │
├─────────────────────────────────────────────────────────┤
│ L5 — Catálogo e Linhagem               (PO/PM + AE)     │
│  Última execução por DataObject, freshness do dado      │
│  Linhagem end-to-end: origem → destino                  │
│  Estado do DataAsset (Draft/Active/Deprecated)          │
└─────────────────────────────────────────────────────────┘
```

### 6.2 Boas Práticas por Camada

#### L2 — Orquestração
- SLA por pipeline declarado no YAML (`airflow.sla_minutes`). Airflow notifica em caso de breach antes do prazo.
- Alertas configurados por canal (Slack, email, webhook) para falha, retry esgotado e sucesso após retry.
- Dead-letter tracking para DAGs que excedem `max_retries` — visível no catálogo como `execution_failed`.

#### L3 — Pipeline
- Cada task emite métricas estruturadas: `rows_read`, `rows_written`, `rows_rejected`, `duration_seconds`, `bytes_transferred`.
- Logs estruturados em JSON para ingestão em ferramentas de APM (ex: Datadog, Google Cloud Logging).
- `discovery_task` emite sempre um `discovery_report` — mesmo quando não há mudanças, confirmando a integridade da origem.

#### L4 — Qualidade
- Quality checks executam **após** o load, antes de marcar a execução como `success`.
- Falha em quality check marca a task com estado próprio `quality_failed` — não bloqueia retries da pipeline, mas **bloqueia pipelines downstream** que dependem desta.
- Relatório de qualidade agregado por DataObject disponível no catálogo de metadados.

#### L5 — Catálogo
- Cada execução de pipeline atualiza automaticamente `last_run`, `last_success` e `freshness_status` no DataObject.
- Linhagem gerada automaticamente a partir das relações declaradas no YAML (`source.objects → destination.objects`).
- Dashboard de saúde por DataAsset visível para PO/PM sem acesso técnico ao Airflow.
- `freshness_status` calculado com base em `last_success` vs `schedule` esperado:
  - `FRESH`: última execução dentro do intervalo esperado.
  - `STALE`: última execução fora do intervalo esperado.
  - `UNKNOWN`: nenhuma execução registrada.
