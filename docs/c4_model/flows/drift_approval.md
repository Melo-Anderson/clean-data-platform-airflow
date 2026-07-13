# Nível 4: Fluxo - Drift Approval Flow

Este diagrama de sequência descreve o fluxo de aprovação de drifts de schema, mostrando como um SRE resolve alterações estruturais bloqueadas nos pipelines.

```mermaid
sequenceDiagram
    participant SRE as 👤 SRE
    participant Router as 🐍 DiscoveryRouter
    participant UseCase as ⚙️ ApproveDriftUseCase
    participant SqlUow as 🐘 SqlUnitOfWork
    participant RepoApp as 🐘 SqlDriftApprovalRepository
    participant RepoDA as 🐘 SqlDataObjectRepository
    participant Catalog as ☁️ DataHubCatalogAdapter

    SRE->>Router: POST /api/v1/discovery/drifts/{id}/approve<br>{decision: "approved"}
    Router->>UseCase: execute(drift_id, decision)

    UseCase->>SqlUow: find DriftApproval by id
    SqlUow-->>UseCase: DriftApproval object (com a referência ao SchemaSnapshot proposto)

    alt Decisão == "approved"
        UseCase->>UseCase: Marca DriftApproval como APPROVED

        UseCase->>RepoDA: find_active_schema(object_id)
        RepoDA-->>UseCase: DataObject (com o Schema anterior)

        UseCase->>UseCase: Atualiza schema do DataObject com os novos campos
        UseCase->>RepoDA: save(DataObject)

        UseCase->>Catalog: publish_schema_version(DataObject)
        Catalog-->>UseCase: Publicado com sucesso no catálogo
    else Decisão == "rejected"
        UseCase->>UseCase: Marca DriftApproval como REJECTED (mantém o schema anterior ativo)
    end

    UseCase->>RepoApp: save(DriftApproval)
    UseCase->>SqlUow: commit()

    UseCase-->>Router: DriftApproval Finalizado
    Router-->>SRE: 200 OK {drift_id, status: "approved" / "rejected"}
```

### Detalhamento do Processo

1. **Ação do SRE**: Quando uma quebra de compatibilidade estrutural ocorre, o pipeline é impedido de rodar. O SRE inspeciona os detalhes do drift e toma uma ação corretiva.
2. **Atualização do Modelo**:
   - Se aprovado, a nova estrutura proposta no snapshot do discovery substitui o schema anterior cadastrado para o `DataObject`. O status do `DriftApproval` muda para `APPROVED`.
   - Se rejeitado, o `DriftApproval` muda para `REJECTED`, e a estrutura atual da plataforma continua ignorando a nova especificação, bloqueando execuções de dados subsequentes até que a fonte de origem seja normalizada ou uma nova decisão seja tomada.
3. **Publicação no Catálogo**: Em caso de aprovação, a plataforma publica a nova versão da tabela física e suas colunas para o DataHub, garantindo que o catálogo de dados corporativo esteja atualizado com o schema aprovado.
