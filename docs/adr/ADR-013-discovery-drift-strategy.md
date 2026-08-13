# ADR 013: Estratégia de Autodescoberta de Metadados e Classificação de Schema Drift

## Status
Aprovado

## Contexto
Em plataformas modernas de dados, alterações não gerenciadas nos esquemas das fontes físicas (bancos SQL, MongoDB, APIs REST) geram quebras silenciosas em pipelines de ingestão e ETL. Era necessário estabelecer uma estratégia formal para:
1. Mapear metadados de fontes heterogêneas (SQL, NoSQL, REST).
2. Versionar fotografias imutáveis de schema (`SchemaSnapshot`).
3. Classificar desvios de estrutura (`SchemaDrift`) por nível de severidade.
4. Governar a aprovação de alterações de schema com quebra de compatibilidade.

## Decisão
1. **Modelagem de Entidades de Discovery:**
   - `DiscoveryRun`: Rastreia cada ciclo executado de varredura.
   - `SchemaSnapshot`: Registra imutavelmente o estado técnico das colunas, tipos e restrições.
   - `DriftEvent`: Notifica alterações entre o snapshot atual e o baseline.
   - `DriftApproval`: Gerencia solicitações pendentes de alteração de schema.

2. **Classificação de Drift e Fluxo de Governança:**
   - **Informativo:** Alterações em comentários ou metadados descritivos. Notifica, execuções prosseguem.
   - **Compatível:** Adição de colunas opcionais (`NULLABLE`). Notifica os responsáveis e prossegue.
   - **Crítico:** Remoção de colunas, alteração de tipos ou chaves primárias. Bloqueia a execução do pipeline até aprovação explícita do SRE via `POST /v1/discovery/approvals/{approval_id}/decision`.

## Consequências
- **Positivas**:
  - Prevenção de corrupção de dados e falhas em cascata no Data Warehouse.
  - Rastreabilidade completa das alterações de schema com fluxo formal de aprovação.
- **Negativas**:
  - Requer intervenção manual do SRE para desbloqueio de pipelines afetados por drifts críticos.
