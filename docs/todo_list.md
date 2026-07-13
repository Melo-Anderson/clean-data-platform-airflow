# Registro de Pendências e Decisões Arquiteturais

Este documento registra as **decisões de escopo** da plataforma — funcionalidades
planejadas que não foram implementadas nesta fase, com justificativa.

> [!NOTE]
> As pendências de desenvolvimento foram migradas para **GitHub Issues** com label `enhancement`
> para permitir tracking, priorização e assignment adequados.
> Consulte: https://github.com/Melo-Anderson/clean-data-platform-airflow/issues?q=label:enhancement

---

## Pendências Abertas (GitHub Issues)

| Feature | Issue |
|---|---|
| Cloud Discovery (S3/GCS/SFTP runners) | [TBD](https://github.com/Melo-Anderson/clean-data-platform-airflow/issues/) |
| External Catalog Integration (DataHub/OpenMetadata) | [TBD](https://github.com/Melo-Anderson/clean-data-platform-airflow/issues/) |
| Notification Adapters (Slack/Teams/Email) | [TBD](https://github.com/Melo-Anderson/clean-data-platform-airflow/issues/) |
| ETL e Export Pipeline Templates | [TBD](https://github.com/Melo-Anderson/clean-data-platform-airflow/issues/) |
| [G2] Security & Identity: RBAC granular, Keycloak/Auth0 integration | [TBD](https://github.com/Melo-Anderson/clean-data-platform-airflow/issues/) |
| [G3] Engineering Rigor: Property-based testing (Hypothesis), Chaos Engineering, Mutation Testing | [TBD](https://github.com/Melo-Anderson/clean-data-platform-airflow/issues/) |
| [G4] Platform Scale: CQRS Segregation, Event-Sourcing POC, Zero-Downtime Migrations | [TBD](https://github.com/Melo-Anderson/clean-data-platform-airflow/issues/) |

*Substitua os TBD pelos números reais das issues após a criação manual.*

---

## Decisões de Escopo Intencional (Não são bugs)

### Autenticação Bearer simplificada

**Decisão:** Manter `Authorization: Bearer <role>` como mecanismo de auth.
**Racional:** Este projeto é um showcase arquitetural educacional. JWT completo com
key management, refresh tokens e revocation lists adicionaria complexidade operacional
desproporcional ao propósito de demonstração. Em produção real, substituir por
autenticação OIDC/JWT com provider adequado (Auth0, Keycloak, Google IAP).

### Rate Limiting ausente

**Decisão:** Sem `slowapi` ou equivalente.
**Racional:** Repositório público sem SLA de produção. Rate limiting deve ser
implementado na camada de API Gateway (Kong, AWS API GW, GCP Apigee) em produção real.

### E2E não rodando no CI

**Decisão:** Testes E2E marcados como `@pytest.mark.e2e` e excluídos do CI com `-m "not e2e"`.
**Racional:** Repositório público sem runners privados ou secrets de cloud. Os testes
E2E rodam localmente via `docker compose run --rm e2e-tests`. Ver `docs/ci_cd_guide.md`.

---

## Conformidade e Padrões de Clean Code

Após auditoria detalhada no repositório, validou-se que a plataforma está em **100% de conformidade** com os padrões arquiteturais propostos.

### Ajustes Intencionais de Nomenclatura (Alinhados ao DDD)

| Termo | Motivo da manutenção |
|---|---|
| `Manager` (SecretManagerPort, BaoSecretManagerAdapter) | Nomenclatura de mercado para cofres de credenciais |
| `data` (DataObject, DataAsset) | Entidades lógicas centrais do domínio de dados, não genéricas |

### Conformidade Geral Confirmada

- **Tamanho:** Funções <= 20 linhas, arquivos <= 300 linhas
- **Acoplamento:** Nenhum import de infraestrutura (SQLAlchemy/httpx) em `domain/` ou `application/`
