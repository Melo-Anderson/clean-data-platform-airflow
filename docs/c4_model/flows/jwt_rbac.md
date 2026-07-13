# Nível 4: Fluxo - JWT Validation e RBAC Check

Este diagrama de sequência detalha os passos que ocorrem a cada request HTTP nas rotas protegidas da plataforma para autenticar o usuário e validar suas permissões granulares de acesso.

```mermaid
sequenceDiagram
    participant Client as 👤 Cliente / Usuário
    participant Router as 🐍 Router (ex: pipeline_router)
    participant Dep as ⚙️ require_permission Dependency
    participant Validator as 🐍 JwtValidator
    participant Resolver as 🐍 DatabasePermissionResolver
    participant Cache as 📁 Memory Cache (dict)
    participant DB as 🐘 PostgreSQL (tabelas RBAC)

    Client->>Router: GET /api/v1/pipelines<br>Header: Authorization Bearer [JWT]
    Router->>Dep: Invoca dependência com permissão alvo (ex: "pipeline:view")

    Dep->>Validator: validate(token)

    alt Assinatura inválida, expirado ou claims incorretas
        Validator-->>Dep: Lança PlatformUnauthorizedError
        Dep-->>Router: HTTP 401 Unauthorized
    else Token Válido
        Validator-->>Dep: Retorna payload do JWT
    end

    Dep->>Validator: extract_roles(payload)
    Validator-->>Dep: Retorna lista de roles (ex: ["analytics_engineer"])

    Dep->>Resolver: get_permissions_for_roles(["analytics_engineer"])

    Resolver->>Cache: Procura permissões no cache local

    alt Hits no Cache e tempo < TTL
        Cache-->>Resolver: Retorna conjunto de permissões (ex: {"pipeline:view", "catalog:view"})
    else Miss no Cache ou expirou (tempo > TTL)
        Resolver->>DB: Query tabelas role_permissions / permissions
        DB-->>Resolver: Retorna registros físicos
        Resolver->>Cache: Salva conjunto de permissões no cache (com timestamp)
    end

    Resolver-->>Dep: Conjunto consolidado de permissões

    alt Permissão alvo ("pipeline:view") está no conjunto
        Dep->>Dep: Instancia CurrentUser(sub, roles, permissions)
        Dep-->>Router: Injeta CurrentUser no endpoint
        Router-->>Client: 200 OK (Executa lógica do endpoint e retorna resposta)
    else Permissão não encontrada
        Dep-->>Router: Lança PlatformForbiddenError
        Router-->>Client: 403 Forbidden
    end
```

### Detalhamento do Processo

1. **Recepção**: A cada chamada HTTP em rotas protegidas por permissão, a dependência do FastAPI `require_permission("permissao")` intercepta a chamada.
2. **Validação Criptográfica**: O `JwtValidator` faz a validação em memória do JWT usando a chave pública RSA. Erros de chave expirada ou assinatura adulterada geram erro HTTP 401 instantaneamente, sem encostar no banco de dados.
3. **Resolução de Permissões**: Como as roles vêm dentro do token JWT, a plataforma precisa converter as roles em permissões. O `DatabasePermissionResolver` cuida disso:
   - Para evitar gargalo de I/O a cada requisição, mantém um cache simples em memória estruturado por role.
   - O cache respeita o tempo máximo de vida (TTL) configurado globalmente nas variáveis de ambiente.
4. **Decisão de Acesso**: Se a permissão exigida pela rota estiver contida no conjunto mapeado, o request segue normalmente e o objeto `CurrentUser` fica disponível para a lógica de negócio do endpoint. Caso contrário, retorna HTTP 403.
