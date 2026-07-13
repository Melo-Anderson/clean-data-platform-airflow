# ADR 001: Arquitetura Limpa (Clean Architecture / Ports & Adapters) e Domain-First Design

## Status
Aprovado

## Contexto
A plataforma de orquestração de dados interage com múltiplas ferramentas externas (bancos de dados relacionais, sistemas de armazenamento de credenciais, orquestradores de fluxo de trabalho como Apache Airflow).
Para evitar o acoplamento forte a essas ferramentas de terceiros (o que dificultaria os testes, a manutenção e futuras migrações de infraestrutura), é necessário estabelecer limites claros de arquitetura.

## Decisão
Adotou-se o padrão de **Arquitetura Limpa (Ports & Adapters / Hexagonal)** guiado por **Domain-First Design**:
1. **Camada de Domínio (`app/domain`)**: Contém as regras de negócio puras, entidades estruturadas (ex: `Pipeline`, `DataAsset`, `PipelineRun`) e Value Objects sem acoplamento a frameworks (FastAPI, SQLAlchemy, etc.).
2. **Camada de Aplicação (`app/application`)**: Implementa a lógica dos Casos de Uso (Use Cases) e define as interfaces/portas (`OrchestratorPort`, `SecretManagerPort`, `TelemetryPort`, `UnitOfWork`) como protocolos Python.
3. **Camada de Infraestrutura (`app/infrastructure`)**: Contém os adaptadores concretos (ex: `AirflowOrchestratorAdapter`, `BaoSecretManagerAdapter`, `SqlAlchemyUnitOfWork`), modelos SQLAlchemy, rotas FastAPI e configurações.

## Consequências
- **Positivas**:
  - Testabilidade excepcional: casos de uso são testados unitariamente sem necessidade de conexões com banco real ou Airflow.
  - Flexibilidade de tecnologia: adaptadores podem ser totalmente substituídos apenas implementando os respectivos protocolos da aplicação.
- **Negativas**:
  - Introduz código extra de boilerplate (mapeamento entre entidades de domínio e modelos SQLAlchemy ORM).
  - Requer rigor e disciplina na equipe para não vazar conceitos de infraestrutura (como sessões SQLAlchemy) para a camada de domínio.
