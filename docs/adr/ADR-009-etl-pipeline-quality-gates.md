# ADR 009: Garantias de Rigor e Qualidade (Quality Gates) em Pipelines de ETL

## Status
Aprovado

## Contexto
O processo de execução de pipelines requer garantias de conformidade com contratos de dados e prevenção de propagação de dados corrompidos para etapas posteriores da arquitetura Lakehouse. Precisamos assegurar que:
1. Violações de qualidade de dados (Quality Gate violations) interrompam a execução do pipeline de forma controlada.
2. Alterações de integridade de dados (como quebras de schema e desvios de tipos) sejam mapeadas e gerem eventos de alerta de drift.
3. Rastreabilidade de linhagem de dados (lineage mapping) seja capturada em cada processamento para auditoria.

## Decisão
Implementou-se as seguintes estruturas para assegurar o rigor nos pipelines:
1. **Quality Gates Interrompíveis:**
   - Pipelines que falham em validações de qualidade e regras de negócio de dados têm seu status alterado para `quality_failed`.
   - Bloqueio imediato da escrita no destino físico e interrupção do pipeline no Airflow por meio de exceções de qualidade.
2. **Tratamento de Drift no Schema:**
   - Lógica de diffing de esquemas embutida para detectar alterações estruturais indesejadas comparando metadados obtidos no Discovery contra a versão de referência do asset.
   - Geração automática de eventos do tipo `DriftEvent` com aprovação/rejeição manual no catálogo de dados.
3. **Mapeamento de Linhagem (Lineage Graph):**
   - Rastreamento implícito de caminhos de origem e destino na Platform API, organizando a cadeia de dependência de dados através de grafos direcionados acíclicos (DAGs) estruturados e expostos em endpoints de visualização de linhagem.

## Consequências
- **Positivas**:
  - Prevenção ativa de poluição e corrupção silenciosa de dados no Lakehouse.
  - Auditoria completa sobre alterações estruturais de dados por meio da gestão centralizada de drift.
- **Negativas**:
  - A interrupção abrupta e falha imediata do pipeline exige fluxos operacionais bem definidos para reprocessamento manual após a mitigação dos problemas de qualidade.
