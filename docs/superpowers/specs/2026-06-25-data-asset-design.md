# Spec: DataAsset — Definição Conceitual, Ciclo de Vida e Governança

**Data:** 2026-06-25
**Autores:** PO / Analytics Engineer
**Status:** Em revisão

---

## 1. Contexto e Escopo

Este documento especifica o modelo conceitual do **DataAsset** na plataforma de dados — a entidade central que representa um domínio de negócio como fonte ou destino de dados. A especificação cobre a separação de responsabilidades entre a entidade de negócio (`DataAsset`) e a entidade técnica (`Endpoint`), o fluxo de cadastro em duas fases, o motor de Autodescoberta de Metadados (Discovery) e a matriz de controle de acesso.

**Fora de escopo neste documento:**
- Especificação do `DataObject` e `DataElement` (definidos em spec futura).
- Métricas gerais de plataforma, guardrails, monitoração e observabilidade detalhada.

---

## 2. Entidades

### 2.1 DataAsset

Entidade de negócio durável que representa um domínio de dados. O cadastro é estável e raramente alterado. Gerenciado por **PO / PM / Analytics Engineer**.

| Atributo | Tipo | Mutabilidade | Quem altera | Descrição |
|---|---|---|---|---|
| `id` | UUID | Imutável | — | Identificador único |
| `nome` | String | Raramente | PO/PM/AE | Nome do domínio (ex: `vendas`, `rh_folha`) |
| `descricao` | String | Sim | PO/PM/AE | Propósito do ativo de dados |
| `owner` | Papel | Sim | PO/PM/AE | Responsável de negócio (PO, PM ou Analytics Engineer) |
| `tags` | Lista[String] | Sim | PO/PM/AE | Classificação temática (ex: `financeiro`, `core`) |
| `policy_tags` | Lista[Enum] | Restrito + log | PO/PM/AE | Classificação de sensibilidade herdada por DataObjects (ex: `PII`, `Restrito`, `Público`) |
| `estado` | Enum | Controlado | Fluxo | `Draft → Active → Deprecated → Archived` |
| `endpoint_id` | UUID (ref) | Raramente / SRE | SRE | Referência ao Endpoint provisionado |
| `discovery_schedule` | Cron String | Sim | PO/PM/AE | Frequência de re-execução periódica do Discovery |
| `discovery_scope` | Objeto | Sim | PO/PM/AE | Filtros de inclusão/exclusão de DataObjects no Discovery |

#### 2.1.1 Discovery Scope

O `discovery_scope` define o que o Discovery deve ou não varrer dentro da origem. É configurável pelo usuário de negócio sem envolvimento do SRE.

```yaml
discovery_scope:
  include:          # se vazio, inclui tudo
    - clientes
    - pedidos
  exclude:          # padrões glob suportados
    - temp_*
    - audit_*
```

**Regra:** alterar o `discovery_scope` pode desencadear um novo Discovery manual ou agendado, mas **nunca requer alteração no Endpoint ou credenciais**.

---

### 2.2 Endpoint (Polimórfico)

Entidade técnica que isola configurações físicas de conectividade e autenticação. Gerenciado exclusivamente pelo **SRE**. Usuários de negócio visualizam apenas o `id` e o `tipo` — nunca credenciais ou parâmetros técnicos.

#### Base Comum (`BaseEndpoint`)

| Atributo | Tipo | Descrição |
|---|---|---|
| `id` | UUID | Identificador único |
| `tipo` | Enum | `DATABASE`, `REST_API`, `SFTP`, `CLOUD_BUCKET`, `ETL_FLOW` |
| `credential_ref` | String | Nome do segredo no Vault/Secret Manager (nunca o valor real) |
| `descricao_tecnica` | String | Notas internas do SRE |

#### Subtipos e Campos Específicos

| Subtipo | Campos adicionais |
|---|---|
| `DatabaseEndpoint` | `host`, `porta`, `database`, `driver` (ex: oracle, postgres, mysql) |
| `RestApiEndpoint` | `base_url`, `auth_type`, `headers_ref` |
| `SftpEndpoint` | `host`, `porta`, `caminho_raiz`, `private_key_ref` |
| `CloudBucketEndpoint` | `provider` (S3/GCS/Azure), `bucket`, `prefixo`, `regiao` |
| `EtlFlowEndpoint` | `ferramenta` (Fivetran, Airbyte, etc.), `flow_id` |

**Regra de imutabilidade:** o `endpoint_id` referenciado no `DataAsset` não é substituído diretamente. Em caso de migração, um novo Endpoint é provisionado e associado; o anterior é desvinculado mas preservado para auditoria.

---

## 3. Ciclo de Vida do DataAsset

### 3.1 Estados

```
[Cadastro pelo PO/PM/AE]
         │
         ▼
      ┌───────┐
      │ DRAFT │──── Endpoint pendente de provisionamento pelo SRE
      └───┬───┘
          │ SRE provisiona Endpoint
          │ CI valida (campos, formato, credential_ref, cron)
          │ Runtime valida (conectividade real, autenticação)
          │ Discovery inicial executado com sucesso
          ▼
      ┌────────┐
      │ ACTIVE │──── Discovery agendado, pipelines ativos
      └───┬────┘
          │ Owner marca para obsolescência
          ▼
      ┌────────────┐
      │ DEPRECATED │──── Notificação enviada a consumidores cadastrados
      └─────┬──────┘     Novos pipelines não podem usar como origem
            │ Aprovação para encerramento
            ▼
      ┌──────────┐
      │ ARCHIVED │──── Ingestão paralisada, histórico em cold storage
      └──────────┘
```

### 3.2 Fluxo de Cadastro em 2 Fases

**Fase 1 — Negócio (PO / PM / Analytics Engineer):**
1. Preenche `nome`, `descricao`, `tags`, `policy_tags`, `discovery_schedule`, `discovery_scope`.
2. Asset criado em estado `Draft`.
3. Solicitação automática enviada ao SRE para provisionamento do Endpoint.

**Fase 2 — Técnica (SRE):**
1. SRE recebe notificação com contexto do Asset (nome, domínio, tipo de fonte esperado).
2. Seleciona o `tipo` do Endpoint e preenche os campos do subtipo correspondente.
3. Define a `credential_ref` apontando para o segredo no Vault/Secret Manager.
4. **CI valida** — campos obrigatórios do subtipo, formato da `credential_ref`, cron syntax, schema do Endpoint.
5. **Runtime valida** — conectividade real, autenticação, permissão de leitura na origem.
6. Se tudo OK → Discovery inicial executado → Asset transita para `Active`.
7. Se falha runtime → Asset permanece em `Draft` com erro detalhado para o SRE. **Falhas capturáveis pelo CI nunca chegam ao runtime.**

---

## 4. Autodescoberta de Metadados (Discovery)

### 4.1 Gatilhos de Execução

| Gatilho | Quem dispara | Observação |
|---|---|---|
| Cadastro inicial | Automático (pós-provisionamento do Endpoint) | Obrigatório para `Draft → Active` |
| Schedule recorrente | Automático via `discovery_schedule` | Detecta schema drift |
| Mudança no `discovery_scope` | PO/PM/AE via ação explícita | Re-varre com novos filtros |
| Re-execução manual | PO/PM/AE | Para troubleshooting ou sync forçado |

### 4.2 O que coleta por tipo de Endpoint

| Tipo | Metadados coletados |
|---|---|
| `DATABASE` | Tabelas, views, schemas, colunas, tipos primitivos, PKs/FKs, constraints, comentários nativos |
| `REST_API` | Endpoints disponíveis, schemas request/response (via OpenAPI/Swagger quando disponível) |
| `SFTP` | Arquivos presentes, extensões, tamanhos, timestamps de modificação |
| `CLOUD_BUCKET` | Arquivos/partições, formatos (Parquet, CSV, JSON), criptografia em repouso, schema inferido |
| `ETL_FLOW` | Fluxos disponíveis, campos de origem e destino declarados pela ferramenta |

### 4.3 Inferência Inteligente

#### PolicyTags Inferidas
O Discovery detecta padrões semânticos em nomes de campos e valores para sugerir classificações de sensibilidade:
- Campos com nomes como `cpf`, `email`, `senha`, `token`, `birth_date`, `cartao` → sugestão `PII` ou `Restrito`.
- Cada sugestão acompanha um **nível de confiança** (`alta / média / baixa`).
- O owner do Asset confirma ou rejeita as sugestões antes de serem aplicadas.

#### Descrições Auto-Geradas
Quando a origem não possui comentários ou documentação nos campos:
- O Discovery gera uma descrição semântica baseada no nome do campo, tipo de dado e contexto do Asset.
- Exemplo: campo `dt_nasc` em asset `RH_Colaboradores` → *"Data de nascimento do colaborador."*
- Descrições auto-geradas são marcadas com `auto_gerado: true` e editáveis pelo usuário de negócio a qualquer momento.

### 4.4 Detecção de Drift e Notificações

| Tipo de mudança detectada | Classificação | Ação da plataforma |
|---|---|---|
| Campo removido | **Crítica** | Alerta ao owner + aprovação obrigatória antes de atualizar catálogo |
| Tipo de dado alterado | **Crítica** | Alerta ao owner + aprovação obrigatória |
| Tabela / arquivo removido | **Crítica** | Alerta ao owner + aprovação obrigatória |
| Campo novo adicionado | **Informativa** | Notificação ao owner — entra no catálogo como `não mapeado` |
| Tabela / arquivo novo | **Informativa** | Notificação ao owner — entra no catálogo como `não mapeado` |
| Mudança de tamanho / partição | **Informativa** | Registro silencioso no catálogo |

**Aprovação de mudanças críticas:** exige confirmação explícita do owner do Asset. A plataforma não auto-aprova mudanças que possam inviabilizar extrações existentes.

---

## 5. Controle de Acesso e Papéis

### 5.1 Matriz RACI

| Ação | PO / PM | Analytics Engineer | SRE | Plataforma (auto) |
|---|---|---|---|---|
| Criar DataAsset (campos de negócio) | R | R | I | — |
| Editar `discovery_scope` e `schedule` | R | R | I | — |
| Editar `policy_tags` | R | R | I | — |
| Aprovar mudanças críticas de schema | R | R | I | Notifica |
| Criar / editar Endpoint | — | — | R | — |
| Visualizar `credential_ref` e dados técnicos | — | — | R | — |
| Acessar conteúdo real do segredo (Vault) | — | — | R | — |
| Executar Discovery (automático) | — | — | — | R |
| Re-executar Discovery manualmente | A | R | I | — |
| Deprecar / Arquivar Asset | R | A | I | — |
| Visualizar catálogo de metadados | R | R | R | — |
| Confirmar/rejeitar PolicyTags inferidas | R | R | — | Sugere |
| Editar descrições auto-geradas | R | R | — | Gera sugestão |

> **R** = Responsável | **A** = Aprova | **I** = Informado

### 5.2 Visibilidade por Papel

```
┌─────────────────────────────────────────────────────────┐
│ DataAsset — visível para todos os papéis                │
│  nome, descricao, owner, tags, policy_tags, estado      │
│  discovery_scope, discovery_schedule                    │
│  endpoint_id (apenas o ID — sem conteúdo técnico)       │
├─────────────────────────────────────────────────────────┤
│ Endpoint — visível SOMENTE para SRE                     │
│  tipo, host, porta, credential_ref, campos do subtipo   │
└─────────────────────────────────────────────────────────┘
```

### 5.3 Regras de Negócio de Acesso

1. O SRE **não pode editar campos de negócio** do DataAsset — separação bidirecional de responsabilidades.
2. Mudanças em `policy_tags` são registradas com timestamp e autor (auditabilidade).
3. Em caso de migração de Endpoint, um novo é provisionado e associado. O anterior é desvinculado, mas **nunca apagado** (preservação para auditoria).
4. Aprovação de mudanças críticas de schema exige confirmação do **owner do Asset** — não é válida auto-aprovação pela plataforma.
