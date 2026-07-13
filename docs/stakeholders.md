# Stakeholders e Papéis de Autorização

A plataforma utiliza autenticação por Bearer Token com papéis fixos que controlam o acesso a cada operação. Em produção, os tokens são trocados por JWT assinados.

## Papéis

| Role | Token (Dev) | Responsabilidades | Operações Permitidas |
|---|---|---|---|
| **PO / PM** | `Bearer po_pm` | Donos de produto. Definem e registram DataAssets e Pipelines. | Criar Asset, Criar Pipeline, Disparar Discovery, Visualizar qualquer recurso |
| **Analytics Engineer** | `Bearer analytics_engineer` | Engenheiros de dados. Constroem transformações e supervisionam pipelines. | Criar Pipeline, Disparar Run, Ler Assets, Ler Runs |
| **SRE** | `Bearer sre` | Responsáveis pela operação da plataforma. | Ativar Asset (ligar Endpoint), Disparar Run, Aprovar Drift, Visualizar qualquer recurso |
| **Leitura Pública** | `Bearer *` | Qualquer usuário autenticado. | Visualizar Assets, DataObjects, Endpoints, PipelineRuns |

## Regras de Negócio de Autorização

- Um Asset só pode ser **ativado** (transição `DRAFT → ACTIVE`) por um SRE.
- Um Pipeline só pode ser **criado** por PO/PM ou Analytics Engineer.
- Um Run de Pipeline pode ser **disparado** por PO/PM, Analytics Engineer ou SRE.
- O relatório de **Quality Gate** (`POST /quality-gate`) não requer role específico — é chamado pelo callback interno do Airflow.
- A **aprovação de drift crítico** é restrita ao SRE.

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
