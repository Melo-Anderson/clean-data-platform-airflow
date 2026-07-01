# Spec: Arquitetura de Software da Plataforma de Dados

**Data:** 2026-06-27
**Autor:** Equipe de Plataforma
**Status:** Em revisão
**Relacionado:**
- [2026-06-25-data-asset-design.md](./2026-06-25-data-asset-design.md)
- [2026-06-25-dataobject-pipeline-design.md](./2026-06-25-dataobject-pipeline-design.md)

---

## 1. Contexto e Princípios

Esta spec descreve a arquitetura de software da plataforma de dados, cobrindo a separação de responsabilidades entre os componentes, o modelo de domínio em texto, o modelo C4 descritivo e os ADRs das principais decisões arquiteturais.

**Princípios invioláveis:**
- **Git como única fonte de verdade** — nenhuma configuração vive fora do repositório.
- **Airflow é executor puro** — nunca armazena configuração; lê DAGs geradas pelo CI.
- **Separação estrita de responsabilidades** — API, CLI, Git e Airflow têm fronteiras não sobrepostas.
- **Arquitetura plugável** — integrações externas (catálogo, secret manager, notification channels) são implementadas via adapters com interface definida. O núcleo nunca depende de implementações concretas externas.

---

## 2. Modelo C4 — Descritivo

### Nível 1 — Contexto do Sistema

A plataforma de dados é o sistema central que conecta **pessoas de negócio e engenheiros** às fontes e destinos de dados da organização. Ela interage com:

- **Usuários de Negócio (PO, PM):** cadastram DataAssets e definem governança via formulário web ou API.
- **Analytics Engineers:** cadastram DataObjects, DataElements e configuram Pipelines.
- **SREs:** provisionam Endpoints e gerenciam credenciais via Vault externo.
- **Sistemas de Origem:** bancos de dados relacionais, APIs, servidores SFTP, buckets de arquivos, ferramentas ETL.
- **Sistemas de Destino:** Data Warehouses (BigQuery, Databricks, Redshift) e sistemas de exportação.
- **Ferramentas de Catálogo Externas (opcional):** DataHub, OpenMetadata — conectadas via adapter plugável.
- **Secret Manager Externo:** Vault, AWS Secrets Manager, GCP Secret Manager — referenciado por `credential_ref`.

---

### Nível 2 — Containers

| Container | Tecnologia | Responsabilidade |
|---|---|---|
| **API (Plataforma)** | Python / FastAPI | Cadastro de entidades, validações de negócio, geração de YAML, commit no Git, integrações externas |
| **CLI (Plataforma)** | Python / Typer ou Click | Operações de plataforma: rebuild de DAGs, migrate de schema_version, sync de catálogo |
| **Banco de Dados Operacional** | PostgreSQL | Estado persistente de DataAssets, Endpoints, DataObjects, DataElements, execuções, metadados do Discovery |
| **Repositório Git** | Git (GitHub/GitLab/Bitbucket) | Fonte de verdade para YAMLs de pipeline e DAGs geradas — imutável para Airflow |
| **CI/CD Pipeline** | GitHub Actions / GitLab CI | Valida YAMLs, gera DAGs Python, executa testes estáticos, faz deploy das DAGs no Airflow |
| **Apache Airflow** | Airflow | Lê DAGs do repositório e executa pipelines. Nunca armazena configuração. |
| **Adapter de Catálogo** | Python (interface plugável) | Propaga metadados, linhagem e eventos para ferramenta de catálogo externa (opcional) |
| **Adapter de Notificação** | Python (interface plugável) | Envia alertas para Slack, email ou webhook |

**Fluxo principal:**

```
[Formulário/API]
     │ valida + gera YAML
     ▼
   [Git]  ◄── fonte de verdade
     │ CI/CD detecta mudança
     ▼
[CI/CD Pipeline]
     │ valida + gera DAG Python
     ▼
  [Airflow DAGs folder]
     │ Airflow scheduler lê
     ▼
  [Airflow Executor]
     │ emite execução + métricas
     ▼
   [API / DB] ◄── atualiza catálogo + linhagem
     │
     ▼
[Adapter de Catálogo] ◄── propaga para ferramenta externa (opcional)
```

---

### Nível 3 — Componentes da API (Monolito Modular)

A API é um monolito modular em FastAPI, organizado por domínio. Cada módulo tem seus próprios roteadores, serviços, repositórios e schemas — sem acoplamento direto entre módulos.

| Módulo | Responsabilidade principal |
|---|---|
| `assets` | CRUD de DataAssets, validações de negócio, gerenciamento de estado (Draft → Active) |
| `endpoints` | CRUD de Endpoints polimórficos, validação de `credential_ref`, handoff para SRE |
| `objects` | CRUD de DataObjects e DataElements, herança de policy_tags, override de tipos |
| `pipelines` | Criação e gerenciamento de Pipelines, geração de YAML, commit no Git |
| `discovery` | Execução e agendamento de Discovery, inferência de PolicyTags, detecção de drift |
| `dag_generator` | Renderização de templates de DAG a partir de YAML, validação de schema_version, rebuild |
| `catalog` | Linhagem, freshness_status, catálogo de metadados, integração com adapter externo |
| `notifications` | Envio de alertas via adapter plugável (Slack, email, webhook) |
| `auth` | Autenticação e autorização por papel (PO/PM, Analytics Engineer, SRE) |

**Padrão interno de cada módulo:**
```
module/
  router.py       # endpoints HTTP (FastAPI)
  service.py      # lógica de negócio
  repository.py   # acesso ao banco de dados (PostgreSQL via SQLAlchemy)
  schemas.py      # contratos de entrada/saída (Pydantic)
  adapters/       # implementações de interfaces externas
```

---

## 3. Domínios e Entidades Principais

Descrição textual dos domínios e suas relações. (Formato texto para otimização de tokens.)

### Domínio: Governança de Ativos (`assets`)
- **DataAsset**: entidade raiz de um domínio de negócio. Durável, com ciclo de vida (`Draft → Active → Deprecated → Archived`). Possui `discovery_scope`, `discovery_schedule` e referência a um `Endpoint`.
- **Endpoint**: entidade técnica polimórfica com base comum e subtipos (`DatabaseEndpoint`, `RestApiEndpoint`, `SftpEndpoint`, `CloudBucketEndpoint`, `EtlFlowEndpoint`). Gerenciado por SRE. Armazena `credential_ref` (nunca a credencial em si).
- **Relação**: DataAsset 1:1 Endpoint. Endpoint pode ser substituído sem recadastro do DataAsset.

### Domínio: Catalogação de Objetos (`objects`)
- **DataObject**: entidade lógica de dados dentro de um DataAsset. Role `SOURCE | DESTINATION | BOTH`. Tipos: `TABLE`, `VIEW`, `FILE`, `API_RESOURCE`, `COLLECTION`. Vinculado a uma Pipeline.
- **DataElement**: campo/atributo de um DataObject. Possui `source_type` (imutável, vindo do Discovery) e `destination_type` (sobrescrevível pelo AE). Carrega `policy_tag` inferida ou confirmada.
- **Relação**: DataAsset 1:N DataObject. DataObject 1:N DataElement.

### Domínio: Pipelines (`pipelines`)
- **Pipeline**: representa a DAG gerada. Tem `type` (`ingestion | etl | export`), `schedule.mode` (`cron | trigger | trigger_with_gate`), lista de objetos de origem e destino, configurações de compute, quality e airflow.
- **PipelineDependency**: relação N:N entre Pipelines para modelar o grafo de dependências. Inclui flag `require_same_day` para o modo `trigger_with_gate`.
- **PipelineRun**: registro histórico de cada execução — status, métricas, `rows_read`, `rows_written`, `duration_seconds`.
- **Relação**: DataObject 1:1 Pipeline. Pipeline N:N Pipeline (dependências).

### Domínio: Discovery (`discovery`)
- **DiscoveryRun**: registro de cada execução do Discovery — data, tipo de gatilho (`initial | scheduled | manual | scope_changed`), resultado (`success | failed`), diff de mudanças detectadas.
- **SchemaDriftEvent**: registro de cada mudança detectada — tipo de mudança, classificação (`informative | critical`), status de aprovação (`pending | approved | rejected`), responsável pela aprovação.
- **Relação**: DataAsset 1:N DiscoveryRun. DiscoveryRun 1:N SchemaDriftEvent.

### Domínio: Catálogo e Linhagem (`catalog`)
- **LineageEdge**: aresta direcional entre DataObjects — `source_object_id → destination_object_id`, associada a uma Pipeline. Compõe o grafo de linhagem.
- **MetadataSnapshot**: snapshot dos metadados coletados pelo Discovery para um DataObject em um momento específico. Armazenado no banco operacional; propagado ao catálogo externo via adapter.
- **Relação**: DataObject N:N DataObject via LineageEdge.

---

## 4. ADRs — Architecture Decision Records

### ADR-001: Git como única fonte de verdade para configurações de pipeline

**Status:** Aceito

**Contexto:** A plataforma precisa garantir que as configurações de pipeline (YAMLs) sejam auditáveis, versionáveis e imutáveis para o Airflow.

**Decisão:** O repositório Git é a única fonte de verdade para YAMLs de pipeline e DAGs Python geradas. A API gera e commita o YAML; o CI/CD gera a DAG; o Airflow apenas lê. Nenhuma configuração é armazenada no banco do Airflow.

**Consequências:**
- ✅ Auditabilidade total via histórico do Git.
- ✅ Rollback de configuração via `git revert`.
- ✅ Review de mudanças via Pull Request.
- ⚠️ Latência entre cadastro e deploy (tempo do CI/CD pipeline).
- ⚠️ Requer CI/CD confiável como parte crítica da infraestrutura.

---

### ADR-002: Armazenamento de metadados híbrido com adapter plugável

**Status:** Aceito

**Contexto:** A plataforma precisa persistir o estado operacional (DataAssets, Pipelines, execuções) e também oferecer funcionalidades ricas de catálogo (linhagem visual, busca semântica) sem criar dependência direta de ferramentas externas.

**Decisão:** PostgreSQL armazena o estado operacional. Um adapter plugável (`CatalogAdapter`) com interface definida propaga eventos de linhagem e metadados para ferramentas externas (DataHub, OpenMetadata, etc.). O núcleo da plataforma só conhece a interface — nunca a implementação concreta.

**Interface do adapter:**
```python
class CatalogAdapter(Protocol):
    def emit_lineage(self, edge: LineageEdge) -> None: ...
    def emit_metadata(self, snapshot: MetadataSnapshot) -> None: ...
    def emit_schema_drift(self, event: SchemaDriftEvent) -> None: ...
```

**Consequências:**
- ✅ Plataforma funciona standalone sem ferramenta externa.
- ✅ Troca de ferramenta de catálogo sem mudança no núcleo.
- ✅ Múltiplos adapters podem ser ativados simultaneamente.
- ⚠️ Sincronização eventual — o catálogo externo pode estar ligeiramente defasado.

---

### ADR-003: API monolito modular como backend central

**Status:** Aceito

**Contexto:** A plataforma precisa de uma API para servir o formulário de cadastro, integrações externas e operações de geração de YAML. Microsserviços aumentariam a complexidade operacional desnecessariamente no estágio inicial.

**Decisão:** Uma única aplicação FastAPI organizada em módulos por domínio (`assets`, `pipelines`, `discovery`, `catalog`, `dag_generator`, etc.). Cada módulo tem fronteiras claras (router, service, repository, schemas) sem acoplamento direto entre módulos — comunicação via interfaces de serviço.

**Consequências:**
- ✅ Operação simples (único deploy, único banco de dados).
- ✅ Refatoração para microsserviços possível no futuro sem reescrever negócio.
- ⚠️ Módulos mal isolados podem criar acoplamento acidental — requer disciplina de revisão de código.

---

### ADR-004: Endpoints polimórficos com herança de schema

**Status:** Aceito

**Contexto:** A plataforma precisa suportar tipos radicalmente diferentes de origem de dados (banco relacional, API REST, SFTP, bucket de arquivos, ETL). Cada tipo tem campos obrigatórios distintos e regras de validação próprias.

**Decisão:** `BaseEndpoint` define os campos comuns (`id`, `tipo`, `credential_ref`). Cada subtipo (`DatabaseEndpoint`, `RestApiEndpoint`, etc.) estende a base com seus campos específicos. No banco de dados, implementado com **table-per-hierarchy** ou **table-per-type** (decisão de implementação). Na API, discriminado via campo `tipo`.

**Consequências:**
- ✅ Validações específicas por tipo garantidas em CI e API.
- ✅ Extensível — novos tipos de endpoint não requerem mudança no schema base.
- ⚠️ Migrações de banco mais complexas ao adicionar novos subtipos.

---

### ADR-005: Geração de DAG config-driven com schema_version e rebuild

**Status:** Aceito

**Contexto:** O template de DAG vai evoluir ao longo do tempo. Pipelines existentes não devem ser recadastradas quando o template muda.

**Decisão:** O YAML de cada pipeline inclui `schema_version`. O gerador de DAG é um componente versionado que sabe migrar YAMLs de versões anteriores. O comando `platform pipeline rebuild` re-gera todas as DAGs com o template atualizado, aplicando migrações de schema automaticamente.

**Consequências:**
- ✅ Evolução do template sem recadastro.
- ✅ `--dry-run` permite revisão do impacto antes de aplicar.
- ⚠️ Migrações de `schema_version` precisam ser mantidas e testadas a cada evolução do template.

---

### ADR-006: Self-healing com classificação de impacto de schema drift

**Status:** Aceito

**Contexto:** Mudanças na estrutura das origens são inevitáveis. A plataforma não pode parar completamente a cada mudança, mas também não pode propagar mudanças destrutivas silenciosamente.

**Decisão:** Mudanças são classificadas em **informativas** (auto-corrigidas + alerta) e **críticas** (sempre bloqueiam + aprovação obrigatória do owner). A classificação `crítica` é aplicada quando a mudança pode corromper dados existentes no destino: `nullable → required` e mudanças de tipo incompatíveis.

**Consequências:**
- ✅ Pipelines resilientes a mudanças aditivas na origem.
- ✅ Proteção contra corrupção de dados no destino.
- ⚠️ Owner do Asset precisa estar acessível para aprovar mudanças críticas — processo de escalada necessário para casos de ausência.
