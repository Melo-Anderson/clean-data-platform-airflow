# Ciclo de Vida do DataAsset (Asset Lifecycle)

Este documento descreve os estados de negócio do ciclo de vida do ativo e o fluxo das fases de pipeline de dados dentro da plataforma.

## 1. Estados de Negócio do Ciclo de Vida (Lifecycle States)
Diferente das etapas técnicas de pipeline, os estados de ciclo de vida representam a situação de governança e uso do DataAsset:

- **Draft (Rascunho)**: O ativo está sendo desenhado conceitualmente pelo PO, PM ou Analytics Engineer. O Endpoint é validado, mas nenhuma ingestão contínua ocorre.
- **Active (Ativo)**: O ativo está homologado, a autodescoberta foi concluída com sucesso e o pipeline de ingestão/transformação é executado sob agendamento contínuo.
- **Deprecated (Depreciado)**: O ativo continua ativo, mas foi marcado para obsolescência futura. Novos pipelines não devem usá-lo como origem. Notificações automáticas são enviadas aos consumidores cadastrados.
- **Archived (Arquivado)**: O processo de ingestão é paralisado. Apenas os dados históricos permanecem disponíveis para consulta regulatória em armazenamento de longo prazo (Cold Storage).

---

## 2. Fases do Pipeline Físico do Ativo

### Ingestão (Raw / Landing Zone)
- **Objetivo**: Extrair os dados da origem física definida no `Endpoint` e persistir na zona de pouso sem transformações estruturais.
- **Regras**: Preservação exata do formato de origem para auditoria completa do histórico.

### Validação (Trusted / Clean Zone)
- **Objetivo**: Filtrar e limpar os dados ingeridos com base nas especificações estruturais de colunas e regras de negócio.
- **Regras**:
  - Validação contra nulos e tipos primitivos de dados.
  - Verificação de herança de segurança (PolicyTags) para certificar o isolamento de dados restritos.
  - Rejeição de registros inválidos em área de quarentena.

### Transformação (Refined / Analytics Zone)
- **Objetivo**: Consolidar, cruzar e aplicar as regras de negócio finais para geração de data marts e tabelas analíticas otimizadas para consumo de negócio.
