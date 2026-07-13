# Nível 3: Componentes de Discovery e Schema Drift

Este documento detalha os componentes do domínio de **Schema Discovery** e a inteligência de comparação e classificação de alterações estruturais (**Schema Drift**).

```mermaid
graph TD
    subgraph "HTTP Layer (app/infrastructure/http/)"
        ROUTER_DISC["DiscoveryRouter\n/discovery/"]
        AUTH["AuthMiddleware\nBearerToken ➔ Role"]
    end

    subgraph "Application Layer (app/application/)"
        UC_DISC["RunDiscoveryUseCase"]
        UC_APP_DRIFT["ApproveDriftUseCase"]
        UOW["UnitOfWork Protocol\n(porta de persistência)"]
    end

    subgraph "Domain Layer (app/domain/)"
        SCHEMA_SNAP["SchemaSnapshot\n(Value Object / Entity)"]
        SCHEMA_FIELD["SchemaField\n(Value Object)"]
        DRIFT_EVENT["DriftEvent\n(Value Object)"]
        DRIFT_CHANGE_TYPE["DriftChangeType\n(Enum)"]
        DRIFT_APP["DriftApproval\n(Aggregate Root)"]
        DRIFT_STATUS["DriftApprovalStatus\n(Enum)"]

        subgraph "Domain Services"
            DIFFER["SchemaDiffer\n(compara snapshots)"]
        end
    end

    subgraph "Infrastructure Layer (app/infrastructure/)"
        SQL_UOW["SqlUnitOfWork\nimplementa UnitOfWork"]
        SQL_REPO_DA["SqlDataObjectRepository"]
        SQL_REPO_DR["SqlDiscoveryRunRepository"]
        SQL_REPO_APP["SqlDriftApprovalRepository"]

        DISC_RUN["DatabaseDiscoveryRunner\n(DuckDB engine metadata)"]
        SECRET_FAC["get_secret_manager\n(Secret Manager Factory)"]
        VAULT_SECRET["BaoSecretManagerAdapter\n(leitura do OpenBao)"]

        DRIFT_CLASS["DriftClassifier\n(Adapter de Drift)"]
    end

    ROUTER_DISC --> AUTH
    AUTH -->|"Verifica permissão\ncatalog:sync"| UC_DISC
    AUTH -->|"Verifica permissão\ndrift:approve"| UC_APP_DRIFT

    UC_DISC --> UOW
    UC_DISC --> DISC_RUN
    UC_APP_DRIFT --> UOW

    UOW --> SQL_UOW
    SQL_UOW --> SQL_REPO_DA
    SQL_UOW --> SQL_REPO_DR
    SQL_UOW --> SQL_REPO_APP

    DISC_RUN --> SECRET_FAC
    SECRET_FAC --> VAULT_SECRET

    UC_DISC --> DIFFER
    UC_DISC --> DRIFT_CLASS

    DRIFT_CLASS --> DIFFER

    SCHEMA_SNAP --> SCHEMA_FIELD
    DIFFER -.->|"Gera"| DRIFT_EVENT
    DRIFT_EVENT --> DRIFT_CHANGE_TYPE
    DRIFT_APP --> DRIFT_STATUS
```

### Principais Componentes

1. **DiscoveryRouter (`app/infrastructure/http/routers/discovery_router.py`)**:
   - Expõe endpoints para iniciar o discovery de um schema, listar execuções de discovery, visualizar drifts pendentes de aprovação e submeter decisões de aprovação/rejeição (SRE).

2. **RunDiscoveryUseCase (`app/application/use_cases/run_discovery.py`)**:
   - Orquestra o ciclo de discovery: obtém credenciais de conexão, dispara o `DatabaseDiscoveryRunner` para inspecionar fisicamente as tabelas do banco de dados, gera um `SchemaSnapshot` atual e calcula o drift em relação ao snapshot anterior ativo utilizando o `SchemaDiffer`.

3. **DatabaseDiscoveryRunner (`app/infrastructure/discovery/database_discovery_runner.py`)**:
   - Conecta-se à base de dados de origem (ex: utilizando credenciais resolvidas pelo Secret Manager) e executa queries de inspeção para obter nomes de tabelas, tipos de dados, nulabilidade e chaves primárias.

4. **BaoSecretManagerAdapter (`app/infrastructure/adapters/secrets/bao_secret_manager_adapter.py`)**:
   - Implementa o mecanismo de comunicação segura com o OpenBao (Vault) via REST API para ler credenciais armazenadas na engine KV v2 de forma transparente para a aplicação de discovery.

5. **SchemaDiffer (`app/domain/discovery/services/schema_differ.py`)**:
   - Serviço de domínio puro. Recebe o snapshot anterior e o novo snapshot e calcula detalhadamente a lista de `DriftEvent` (campos adicionados, removidos, alterações de nulabilidade e incompatibilidade de tipos de dados).

6. **DriftClassifier (`app/infrastructure/drift_classifier.py`)**:
   - Adapter de infraestrutura que atua como gate para pipelines de ETL. Recebe dicionários serializados de schemas, converte-os em estruturas de domínio e avalia se o pipeline pode ou não seguir de acordo com a gravidade dos `DriftEvent` mapeados (campos removidos ou tipos incompatíveis bloqueiam o pipeline).

7. **ApproveDriftUseCase (`app/application/use_cases/approve_drift.py`)**:
   - Permite que um usuário com a role correta (SRE) aprove formalmente um drift de schema bloqueante, aplicando a alteração e atualizando o snapshot ativo no catálogo de assets da plataforma.
