# Lista de Pendências e Auditoria de Padrões (TO-DOs)

Este documento centraliza as pendências de desenvolvimento identificadas no repositório e confronta a estrutura do projeto com as regras normativas de design estabelecidas no [Guia de Clean Code](clean-code.md).

---

## 1. TO-DOs (Pendências de Desenvolvimento)

### ⚙️ Funcionalidades e Adaptadores de Infraestrutura
- `[ ]` **Implementação de Cloud Discovery:** O ciclo de `Metadata Discovery` está atualmente restrito a fontes de banco de dados (`app/infrastructure/discovery/database_runner.py`). Falta implementar runners para Buckets (S3/GCS) e servidores SFTP.
- `[ ]` **Integração com Catálogo Real (DataHub / OpenMetadata):** Atualmente, os adaptadores para ferramentas de catálogo estão stubados como `noop`. É necessário desenvolver os clients HTTP para sincronizar os `CatalogSchemaVersion` com estas plataformas de mercado.
- `[ ]` **Adaptador de Notificação Real (Slack):** O adaptador de alertas e SLA está stubado como `noop`. Falta implementar a integração Webhook com o Slack para alertar o SRE de drifts críticos e falhas de qualidade.
- `[ ]` **Adaptadores de ETL e Exportação no DuckDB:** Implementar a lógica para os fluxos de transformação entre camadas (Clean -> Refined) e exportação para destinos externos via adaptador DuckDB.

### 🧪 Cobertura de Testes
- `[ ]` **Testes de Mock para Outros Tipos de Ingestão:** Criar suites de testes de unidade e integração para validar os contratos de novos runners (SFTP e Buckets) simulando as conexões.
- `[ ]` **Testes E2E de Sincronização de Metadados:** Integrar o teste de qualidade com os catalogadores nos testes E2E para certificar a ingestão no OpenMetadata/DataHub de forma automatizada.

---

## 2. Auditoria e Conformidade com o Clean Code

Confrontamos a estrutura atual de arquivos e classes com as regras do `clean-code.md` para identificar possíveis desvios:

### ⚠️ Regra de Nomenclatura: "Evite: `data`, `handler`, `Manager`"
A regra de nomenclatura de Clean Code sugere evitar termos genéricos para garantir especificidade. No entanto, no projeto atual:

1.  **Utilização de `Manager`:**
    *   `app/application/shared/secret_manager_port.py` (Classe `SecretManagerPort`)
    *   `app/infrastructure/adapters/secrets/bao_secret_manager_adapter.py` (Classe `BaoSecretManagerAdapter`)
    *   `app/infrastructure/adapters/secrets/secret_manager_factory.py` (Classe `SecretManagerFactory`)
    *   *Análise:* O uso do termo `Manager` aqui é perfeitamente justificado por ser a nomenclatura de mercado amplamente estabelecida para cofres de credenciais (*Secret Managers*), sobrepondo-se ao veto genérico do Clean Code em prol da clareza técnica.

2.  **Utilização de `data`:**
    *   `app/domain/objects/data_object.py` (Classe `DataObject`)
    *   `app/infrastructure/persistence/models/data_object_model.py` (Classe `DataObjectModel`)
    *   `app/infrastructure/persistence/models/data_asset_model.py` (Classe `DataAssetModel`)
    *   *Análise:* Embora `data` seja desencorajado por ser vago, na Engenharia de Dados este termo faz parte da linguagem ubíqua do domínio (ex: *Data Asset*, *Data Object*). O uso é aceitável, pois está alinhado às práticas de **Domain-Driven Design (DDD)**.

### ✅ Conformidade Geral
- **Tamanho de Funções e Módulos:** Todas as funções cruciais (como a extração do DuckDB e use cases) estão sob o limite máximo de 20 linhas e arquivos sob 300 linhas de código limpo.
- **Acoplamento de Dependências:** Não existem imports de infraestrutura (SQLAlchemy/httpx) dentro de `domain` ou `application`, respeitando a regra de dependências unidirecionais.
