# Nível 4: Fluxo - Schema Discovery Execution

Este diagrama de sequência descreve o processo de execução de um **Discovery Run** para extrair metadados e identificar alterações estruturais (drifts) em bancos de dados de origem de forma resiliente.

```mermaid
sequenceDiagram
    participant AE as 👤 Analytics Engineer
    participant Router as 🐍 DiscoveryRouter
    participant UseCase as ⚙️ RunDiscoveryUseCase
    participant Runner as 🛠️ DatabaseDiscoveryRunner
    participant Vault as 🔐 BaoSecretManagerAdapter
    participant SqlUow as 🐘 SqlUnitOfWork
    participant Differ as 🛠️ SchemaDiffer

    AE->>Router: POST /api/v1/discovery/runs<br>{endpoint_id: "..."}
    Router->>UseCase: execute(endpoint_id)

    UseCase->>SqlUow: find Endpoint by id
    SqlUow-->>UseCase: Endpoint Config

    UseCase->>Runner: run_discovery(endpoint_config)

    Runner->>Vault: get_credentials(secret_path)
    Vault-->>Runner: Conexão decodificada (user, password, host)

    Runner->>Runner: Abre conexão física (DuckDB/SQL) e<br>inspeciona tabelas e tipos de dados
    Runner-->>UseCase: Retorna lista de SchemaFields brutos

    UseCase->>SqlUow: Busca último SchemaSnapshot ativo para o asset
    SqlUow-->>UseCase: SchemaSnapshot (anterior) ou None

    UseCase->>UseCase: Instancia novo SchemaSnapshot (atual)

    UseCase->>Differ: diff(prev_snapshot, curr_snapshot)
    Differ-->>UseCase: Retorna lista de DriftEvents (ex: FIELD_ADDED, FIELD_REMOVED)

    alt Tem Drift Detectado
        UseCase->>SqlUow: Grava DriftApproval como PENDING
    end

    UseCase->>SqlUow: Grava novo SchemaSnapshot e marca DiscoveryRun como SUCCESS
    UseCase->>SqlUow: commit()

    UseCase-->>Router: DiscoveryRun Result (status, drifts_detected)
    Router-->>AE: 201 Created {run_id, status: "success", drifts: [...]}
```

### Detalhamento do Processo

1. **Trigger de Sincronização**: O engenheiro inicia o discovery para verificar se a estrutura do banco físico mudou em relação ao catálogo da plataforma.
2. **Resolução de Segredos**: O `DatabaseDiscoveryRunner` não guarda as senhas das conexões. Ele consome o `BaoSecretManagerAdapter` que recupera as credenciais criptografadas do OpenBao em tempo de execução.
3. **Inspeção Física**: Utilizando um adaptador SQL/DuckDB, o runner lê o catálogo do banco alvo e gera uma lista contendo os tipos nativos de cada coluna.
4. **Cálculo de Diferenças**: O `SchemaDiffer` de domínio compara a estrutura antiga com a atual. Eventos como inclusões, remoções ou mudanças de tipo são computados detalhadamente.
5. **Criação de Gate**: Se houver alguma mudança de schema, um registro do tipo `DriftApproval` é criado com status `PENDING`. As alterações só entrarão em vigor no catálogo corporativo após a validação do SRE.
