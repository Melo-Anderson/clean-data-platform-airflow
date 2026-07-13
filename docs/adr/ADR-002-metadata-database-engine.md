# ADR 002: Motor de Banco de Dados de Metadados (SQLite/Dev vs PostgreSQL/Prod)

## Status
Aprovado

## Contexto
O catálogo de metadados da plataforma precisa armazenar informações estruturadas e relacionais sobre assets, schemas, pipelines, auditorias, logs de execução e regras de acesso granular.
Para simplificar a inicialização local em desenvolvimento e otimizar a confiabilidade e concorrência em produção, é necessária uma decisão de motor de banco de dados.

## Decisão
Adotou-se o uso de **PostgreSQL 16+ para produção** e **SQLite local para desenvolvimento rápido e testes automatizados**:
1. **Ambiente de Teste/Dev**: Utiliza arquivos SQLite locais (`dev.db`) ou banco em memória para velocidade máxima na execução de suítes de teste.
2. **Ambiente de Produção**: Utiliza PostgreSQL como motor relacional primário para suporte a concorrência forte, isolamento transacional e alta disponibilidade.
3. **Mapeamento e Evolução**: O SQLAlchemy ORM realiza o isolamento de dialetos, e o **Alembic** é utilizado para gerenciar migrações de schema de forma declarativa e versionada.

## Consequências
- **Positivas**:
  - Testes de integração extremamente rápidos com SQLite local.
  - Robustez transacional e conformidade ACID empresarial garantidas com PostgreSQL.
- **Negativas**:
  - Potencial divergência sutil de sintaxe e comportamento SQL entre dialetos (ex: SQLite não suporta nativamente certas sintaxes como `DISTINCT ON` ou constraints específicas, que precisam ser tratadas via código ou testes específicos de dialeto).
