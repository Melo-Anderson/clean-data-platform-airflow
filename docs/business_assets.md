# Ativos de Dados de Negócio (DataAssets e Endpoints)

## 1. Entidade `DataAsset`

O `DataAsset` é a **representação lógica de alto nível de uma fonte ou domínio de dados**. Ele representa o contrato de negócio sobre os dados: quem é o dono, qual é a finalidade, quais políticas de segurança se aplicam.

Um `DataAsset` **não armazena configuração de conexão**. A conexão física é responsabilidade do `Endpoint`.

### Atributos

| Atributo | Tipo | Descrição |
|---|---|---|
| `id` | UUID | Identificador único gerado automaticamente |
| `name` | string | Nome único do ativo (ex: `vendas-database-asset`) |
| `description` | string | Descrição do domínio de negócio |
| `owner_email` | EmailAddress | E-mail do responsável pelo ativo |
| `tags` | list[str] | Tags de classificação temática |
| `policy_tags` | list[str] | Tags de segurança herdadas por todos os DataObjects (ex: `PII`, `Restrito`) |
| `state` | AssetState | Estado atual do ciclo de vida |
| `endpoint_id` | UUID? | Referência ao Endpoint físico (preenchido na ativação) |
| `discovery_schedule` | cron | Agendamento de execução da autodescoberta |
| `discovery_scope_include` | list[str] | Padrões de objetos a incluir (ex: `["*"]` para tudo) |
| `discovery_scope_exclude` | list[str] | Padrões de objetos a excluir |

### Estados do Ciclo de Vida

| Estado | Descrição |
|---|---|
| `DRAFT` | Ativo cadastrado conceitualmente. Nenhuma ingestão ocorre. Endpoint ainda não vinculado. |
| `ACTIVE` | Ativo homologado. Endpoint vinculado, Discovery concluído. Pipelines podem ser registrados e executados. |
| `DEPRECATED` | Ativo marcado para obsolescência. Continua operando, mas novos pipelines não devem usá-lo. |
| `ARCHIVED` | Ingestão encerrada. Apenas dados históricos disponíveis para consulta regulatória. |

A transição `DRAFT → ACTIVE` exige papel **SRE** e ocorre via `POST /assets/{name}/activate?endpoint_name=...`.

---

## 2. Entidade `Endpoint`

O `Endpoint` representa a **configuração técnica de acesso** a uma fonte de dados. Ele isola credenciais e detalhes de conectividade do DataAsset.

### Tipos de Endpoint suportados

| Tipo | Uso |
|---|---|
| `database` | Conexão com bancos relacionais (PostgreSQL, Oracle, MySQL) |
| `api` | Fontes REST/HTTP |
| `sftp` | Fontes de arquivos via SFTP |
| `bucket` | Cloud Storage (GCS, S3) |

### Atributos

| Atributo | Descrição |
|---|---|
| `id` | UUID gerado automaticamente |
| `name` | Nome único do endpoint (ex: `sales-db-prod`) |
| `type` | Tipo de conexão |
| `credential_ref` | Caminho do segredo no cofre de credenciais (ex: `secret/postgres`) |
| `technical_description` | Descrição técnica da conexão |

As credenciais reais **nunca são armazenadas na plataforma**. A `credential_ref` aponta para o **OpenBao (Vault)** onde as credenciais são recuperadas em tempo de execução.

---

## 3. Autodescoberta de Metadados (Discovery)

Ao **ativar** um DataAsset, a plataforma dispara automaticamente um ciclo de **Metadata Discovery**. A descoberta também pode ser disparada manualmente.

### O que a Discovery faz:

1. **Conecta-se à fonte** usando as credenciais do Endpoint recuperadas do OpenBao.
2. **Varre os objetos** (tabelas, views, arquivos) dentro do escopo definido no Asset.
3. **Cria ou atualiza `DataObjects`** com nome, tipo, descrição e lista de elementos (colunas/campos).
4. **Versiona o schema** em `CatalogSchemaVersion`, mantendo histórico completo de mudanças.
5. **Detecta drift** comparando o schema atual com a versão anterior.

### Classificação de Drift

| Severidade | Exemplos | Ação Padrão |
|---|---|---|
| **Informativo** | Novo índice, comentário adicionado | Notificação, pipeline continua |
| **Compatível** | Coluna nova com default nullable | Notificação, pipeline continua |
| **Crítico** | Coluna obrigatória removida, tipo alterado incompativelmente | Bloqueia pipeline até aprovação do SRE |

A política de ação em mudanças críticas é configurada por pipeline no campo `on_critical_change`: `"block"` (padrão) ou `"warn"`.
