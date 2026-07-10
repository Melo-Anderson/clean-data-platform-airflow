# ADR-002: Gerenciamento de Secrets — OpenBao

**Status:** Accepted  
**Data:** 2026-07-10  

## Contexto

A plataforma de dados gerencia metadados de credenciais para conexão em bancos de dados, storages, APIs e buckets S3. Essas informações confidenciais não podem ser salvas em texto puro no banco de dados operacional. É necessário uma ferramenta externa segura para armazenamento de segredos.

## Alternativas Consideradas

| Alternativa | Prós | Contras |
|-------------|------|---------|
| **HashiCorp Vault** | Padrão da indústria, extremamente robusto, amplas APIs | Licenciamento Business Source License (BSL) restritivo |
| **AWS Secrets Manager** | Gerenciado nativamente | Custo mensal por segredo, lock-in na AWS |
| **OpenBao** | Open Source (Mozilla Public License 2.0), fork direto do Vault | Comunidade e ecossistema ainda em maturação |

## Decisão

Usar **OpenBao (fork 100% open source da comunidade Linux Foundation)**. A API permanece idêntica à do Vault KV v2, permitindo o uso imediato de clientes existentes (como o adapter `BaoSecretManagerAdapter` estendendo `SecretManagerPort`), sem o risco de licenciamento restritivo futuro.

## Consequências

- ✅ Licenciamento open source permissivo garantido a longo prazo
- ✅ Compatibilidade 1-para-1 com APIs existentes do Vault KV (v1 e v2)
- ⚠️ Necessidade de gerenciar a própria resiliência de rede do container em ambientes locais/Kubernetes (mitigada com retry no adapter)
