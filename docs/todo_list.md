# Lista de Pendências e Padrões Arquiteturais (TO-DOs)

Este documento centraliza as pendências conceituais de desenvolvimento da plataforma e consolida a conformidade da estrutura atual em relação às diretrizes técnicas do [Guia de Clean Code](clean-code.md).

---

## 1. TO-DOs (Pendências de Desenvolvimento)

### ⚙️ Funcionalidades e Adaptadores de Infraestrutura
- `[ ]` **Implementação de Cloud Discovery:** O ciclo de `Metadata Discovery` está atualmente restrito a fontes de banco de dados (`app/infrastructure/discovery/database_runner.py`). Falta implementar runners para Buckets (S3/GCS) e servidores SFTP de forma agnóstica.
- `[ ]` **Integração com Catálogo Externo Corporativo (DataHub / OpenMetadata):** O catálogo de metadados local já está totalmente implementado via Postgres (`DatabaseCatalogAdapter`). O TO-DO restante é desenvolver os clients HTTP nos adaptadores `noop` para sincronizar os schemas externamente com ferramentas corporativas de mercado.
- `[ ]` **Adaptadores de Notificação (Chat Webhooks):** O adaptador de alertas está stubado como `noop`. Falta implementar a integração de Webhook agnóstica para suportar o disparo de alertas para ferramentas de comunicação (Slack, Microsoft Teams, Discord ou e-mail).
- `[ ]` **Lógica para Pipelines de ETL e Exportação:** Implementar a lógica dos templates e pipelines de transformação (Clean -> Refined) e exportação. O contrato `ComputeJobAdapter` é genérico e aceitará execuções locais via **DuckDB** ou distribuídas via **Apache Spark** e **Google Cloud Dataflow**.

### 🧪 Cobertura de Testes
- `[ ]` **Testes de Mock para Outros Tipos de Ingestão:** Criar suites de testes de unidade e integração para validar os contratos de novos runners (SFTP e Buckets) simulando as conexões.
- `[ ]` **Testes E2E de Sincronização de Metadados:** Integrar o teste de qualidade com os catalogadores nos testes E2E para certificar a ingestão no OpenMetadata/DataHub de forma automatizada.

---

## 2. Conformidade e Padrões de Clean Code

Após auditoria detalhada no repositório, validou-se que a plataforma está em **100% de conformidade** com os padrões arquiteturais propostos. 

### Ajustes Intencionais de Nomenclatura (Alinhados ao DDD)
A regra geral de Clean Code desencoraja termos vagos como `data` ou sufixos como `Manager`. No entanto, na engenharia e arquitetura de dados da plataforma, estes termos foram mantidos de forma intencional por fazerem parte da linguagem ubíqua (Domain-Driven Design) e dos padrões industriais:

1.  **Utilização de `Manager` (Ex: `SecretManagerPort` / `BaoSecretManagerAdapter`):**
    *   Mantido por ser a nomenclatura de mercado amplamente estabelecida para cofres de credenciais (*Secret Managers*), garantindo legibilidade imediata para engenheiros e operadores.
2.  **Utilização de `data` (Ex: `DataObject`, `DataAsset`):**
    *   Termos mantidos por representarem entidades lógicas centrais e bem-definidas do domínio de dados (não são genéricos dentro do contexto da plataforma).

### ✅ Conformidade Geral Confirmada
- **Tamanho de Funções e Módulos:** Todas as funções cruciais de pipelines, templates e adaptadores respeitam o limite máximo de 20 linhas e arquivos estão sob o limite máximo de 300 linhas de código limpo.
- **Acoplamento de Dependências:** A regra de dependências unidirecionais do Hexágono é respeitada de forma estrita. Não existem imports de infraestrutura (SQLAlchemy/httpx) dentro de `domain` ou `application`.
