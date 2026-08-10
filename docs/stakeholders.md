# Stakeholders e Papéis de Autorização

A plataforma utiliza autenticação por Bearer Token com papéis fixos que controlam o acesso a cada operação. Em produção, os tokens são trocados por JWT assinados.

## Papéis

| Role | Token (Dev Local) | Autenticação (Produção) | Responsabilidades |
|---|---|---|---|
| **PO / PM** | `Bearer po_pm` | JWT RS256 (claim: `po_pm`) | Donos de produto. Definem e registram DataAssets e Pipelines. |
| **Analytics Engineer** | `Bearer analytics_engineer` | JWT RS256 (claim: `analytics_engineer`) | Engenheiros de dados. Constroem transformações e supervisionam pipelines. |
| **SRE** | `Bearer sre` | JWT RS256 (claim: `sre`) | Responsáveis pela operação, infraestrutura e governança da plataforma. |
| **Leitura Pública** | `Bearer *` | JWT RS256 (qualquer role) | Leitura de metadados e observabilidade. |

---

## Matriz Granular de Permissões (RBAC)

O sistema de segurança mapeia roles em permissões granulares gerenciadas no banco e resolvidas pelo `DatabasePermissionResolver`:

| Permissão | Descrição | PO / PM | Analytics Engineer | SRE |
|---|---|:---:|:---:|:---:|
| `catalog:view` | Visualizar DataAssets, DataObjects e Endpoints | ✅ | ✅ | ✅ |
| `catalog:edit` | Criar e atualizar DataAssets (DRAFT) | ✅ | ❌ | ❌ |
| `catalog:sync` | Ativar DataAssets (`DRAFT → ACTIVE`) e vincular Endpoints | ❌ | ❌ | ✅ |
| `pipeline:trigger` | Criar, alterar e disparar execuções de Pipelines | ✅ | ✅ | ✅ |
| `drift:approve` | Aprovar alterações críticas de schema no Metadata Discovery | ❌ | ❌ | ✅ |

---

## Regras de Negócio de Autorização

- Um Asset só pode ser **ativado** (transição `DRAFT → ACTIVE`) por um SRE (`catalog:sync`).
- Um Pipeline só pode ser **criado** por PO/PM ou Analytics Engineer (`pipeline:trigger`).
- Um Run de Pipeline pode ser **disparado** por PO/PM, Analytics Engineer ou SRE (`pipeline:trigger`).
- O relatório de **Quality Gate** (`POST /quality-gate`) não requer role específico — é chamado pelo callback interno do Airflow via token de serviço.
- A **aprovação de drift crítico** é restrita ao SRE (`drift:approve`).

## Fluxo Típico de Onboarding

```
PO/PM            SRE                     Analytics Engineer
  │                │                              │
  ├── Registra Asset (DRAFT)                      │
  │                │                              │
  │         ├── Ativa Asset + Endpoint            │
  │         │   (DRAFT → ACTIVE + Discovery)      │
  │                │                              │
  │                │            ├── Registra Pipeline vinculado ao Asset
  │                │            │
  │                │            ├── Dispara Run
  │                │            │
  │                │            └── Monitora Quality Gate
```

## Fluxo de Autenticação em Produção (JWT RS256)

1. O cliente (Frontend / API) autentica-se em um Identity Provider (ex: Auth0, Keycloak) e recebe um JWT (Access Token).
2. O JWT é assinado usando o algoritmo assimétrico `RS256`.
3. O serviço `airflow-data-platform` valida a assinatura do JWT localmente usando a chave pública (definida via `PLATFORM_AUTH_JWT_PUBLIC_KEY_PEM_FILE` ou `keys/jwt_public.pem`).
4. O payload do token decodificado contém a role (`realm_access.roles` ou claim customizada configurada em `jwt_roles_claim`).
5. O `DatabasePermissionResolver` mapeia a role do token para permissões granulares (`catalog:edit`, `catalog:view`, `drift:approve`, etc.) consultando o banco de dados com cache de TTL.

**Mecanismo de Cache de Permissões:**
Para evitar consultas constantes ao banco a cada requisição, as permissões de cada role são armazenadas em cache em memória com TTL configurável. Uma invalidação de cache manual está disponível via classe `DatabasePermissionResolver.invalidate_cache()`.
