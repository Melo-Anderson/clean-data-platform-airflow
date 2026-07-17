# Regras de Negócio e Fluxos da Plataforma de Dados

Este documento especifica o modelo conceitual e as regras de negócio governando as entidades lógicas (`DataAsset`, `Endpoint`, `DataObject`, `Pipeline`, `PipelineRun`) e os fluxos de processamento essenciais da plataforma.

---

## 1. Modelo de Ativos de Dados (DataAssets e Endpoints)

### Entidade `DataAsset`
O `DataAsset` é a **representação lógica de alto nível de uma fonte ou domínio de dados**. Ele representa o contrato de negócio sobre os dados: quem é o dono, qual é a finalidade, quais políticas de segurança se aplicam.

Um `DataAsset` **não armazena configuração de conexão**. A conexão física é de responsabilidade exclusiva do `Endpoint`.

#### Atributos do DataAsset:
- `id` (UUID): Identificador único gerado automaticamente.
- `name` (string): Nome único do ativo (ex: `vendas-database-asset`).
- `description` (string): Descrição do domínio de negócio.
- `owner_email` (EmailAddress): E-mail do responsável pelo ativo.
- `tags` (list[str]): Tags de classificação temática.
- `policy_tags` (list[str]): Tags de segurança herdadas por todos os DataObjects (ex: `PII`, `Restrito`).
- `state` (AssetState): Estado atual do ciclo de vida (`DRAFT`, `ACTIVE`, `DEPRECATED`, `ARCHIVED`).
- `endpoint_id` (UUID): Referência ao Endpoint físico.
- `discovery_schedule` (cron): Agendamento de execução da autodescoberta.
- `discovery_scope_include` / `exclude` (list[str]): Padrões de objetos a incluir/excluir (suporta glob-patterns e exclusão seletiva via `scope_exclude`).

> **Regra de Transição:** A transição `DRAFT → ACTIVE` exige papel **SRE** e ocorre via `POST /assets/{name}/activate?endpoint_name=...`.

### Entidade `Endpoint`
O `Endpoint` representa a **configuração técnica de acesso** a uma fonte de dados. Ele isola credenciais e detalhes de conectividade do DataAsset.
- **Tipos Suportados:** `database` (Postgres, Oracle, MySQL), `nosql` (MongoDB), `api` (REST), `sftp`, `bucket` (GCS, S3).
- **Segurança:** As credenciais reais **nunca são armazenadas na plataforma**. O atributo `credential_ref` aponta para o **OpenBao (Vault)** onde as credenciais são recuperadas em tempo de execução.

---

## 2. Fluxos Principais da Plataforma

### Fluxo A: Autodescoberta de Metadados (Metadata Discovery)
Ao **ativar** um DataAsset, a plataforma dispara automaticamente um ciclo de **Metadata Discovery**:
1.  **Conexão Segura:** Conecta-se à fonte usando as credenciais do Endpoint recuperadas do OpenBao.
2.  **Varredura física (com Exclusão de Escopo):** Mapeia a estrutura técnica de acordo com o `scope_include` e filtra ativamente chaves/tabelas/coleções descritas no `scope_exclude` (glob-patterns).
3.  **Abordagem Híbrida (SQL vs NoSQL/MongoDB):**
    -   **Bancos Relacionais (SQL):** Lê esquemas técnicos diretamente de catálogos nativos do SGBD (informações sobre chaves primárias, estrangeiras e tipos de colunas).
    -   **Bancos NoSQL (MongoDB):** Tenta ler o validador `$jsonSchema` definido na coleção para extração precisa e barata. Se inexistente, cai para a estratégia de **Amostragem Dinâmica**, buscando `$sample` de 100 documentos para inferir a união dos tipos presentes.
4.  **Provisionamento automático:** Cria ou atualiza os `DataObjects` no banco de metadados da plataforma.
5.  **Versionamento:** Grava uma nova versão do schema em `CatalogSchemaVersion`.
6.  **Detecção de Drift:** Compara o schema obtido com a versão anterior do catálogo.

#### Classificação de Drift:
*   **Informativo:** Alterações sem impacto operacional (ex: comentários). Notifica, execuções continuam.
*   **Compatível:** Alterações retrocompatíveis (ex: coluna opcional adicionada). Notifica, execuções continuam.
*   **Crítico:** Alterações que quebram código (ex: coluna removida, mudança de tipo). Bloqueia a execução do pipeline até que o SRE aprove via chamada à API (`POST /discovery/runs/{id}/approve-drift`).

---

### Fluxo B: Registro e Onboarding de Pipelines (`RegisterPipeline`)
O registro de um pipeline vincula um ativo de dados a um fluxo estruturado de processamento e governança:
1.  **Validação de Exclusividade:** Garante que o nome do pipeline seja único em toda a plataforma.
2.  **Autoprovisionamento do Destino:** Ao registrar o pipeline, a plataforma cria automaticamente os `DataObjects` de destino no banco de metadados com base na assinatura de saída configurada (garantindo governança e linhagem desde o dia zero).
3.  **Regra de Ativação:** O pipeline só pode ser cadastrado se o `DataAsset` de origem associado estiver no estado `ACTIVE`.

---

### Fluxo C: Execução e Integração com Airflow (`TriggerPipelineRun`)
O ciclo operacional de disparo e execução segue o padrão desacoplado:
1.  **Disparo:** O cliente chama a API em `POST /pipelines/{id}/run`.
2.  **Gravação do Estado:** A API cria um registro operacional `PipelineRun` com status `running` no banco de dados da plataforma.
3.  **Geração Dinâmica:** A plataforma compila o template Jinja2 correspondente ao tipo do pipeline, gerando dinamicamente um arquivo físico de DAG Python (ex: `e2e-ingest-pipeline.py`) dentro da pasta `./dags/`.
4.  **Invocação do Orquestrador:** A API se comunica via HTTP com a API REST do Airflow para notificar e disparar a DAG (`POST /api/v2/dags/{dag_id}/dagRuns`).
5.  **Forçar Reserialization (Dev):** Para evitar atrasos de carregamento no ambiente local, a chamada executa um comando de reserialização forçada no container do Airflow.

---

### Fluxo D: Quality Gates e Métricas
O controle de integridade dos dados pós-execução é realizado por callbacks do próprio orquestrador:
1.  **Envio de Métricas:** Ao terminar o processamento físico dos dados, a task final do Airflow envia as estatísticas (contagem de nulos, unicidades, contagem de linhas) para o endpoint `POST /pipelines/{pid}/runs/{rid}/quality-gate`.
2.  **Validação Estatística:** A plataforma compara as métricas recebidas contra as regras de qualidade (`QualityRule`) definidas na configuração do pipeline (ex: `id` não pode ter nulos, `row_count` deve ser maior que 0).
3.  **Transição de Status:**
    *   Se as regras passarem: O `PipelineRun` é atualizado para `success`.
    *   Se houver violações: O `PipelineRun` é atualizado para `quality_failed`, salvando a lista detalhada de falhas.

---

### Fluxo E: Linhagem de Dados (Data Lineage)
A linhagem rastreia o caminho lógico das informações:
- A plataforma grava registros em tabelas de relacionamento vinculando quais elementos de origem (`DataElement` da tabela de origem) alimentam os elementos de destino.
- A linhagem é atualizada automaticamente a cada ciclo de atualização de schema ou alteração de pipeline, permitindo auditorias de impacto e rastreamento de sensibilidade de dados (ex: verificar onde dados do tipo `PII` estão sendo gravados).
