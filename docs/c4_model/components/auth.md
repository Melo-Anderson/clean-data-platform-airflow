# Nível 3: Componentes de Autenticação e RBAC

Este documento descreve os componentes internos responsáveis pela segurança da plataforma, incluindo validação criptográfica de tokens JWT (RS256) e resolução de permissões granulares via Role-Based Access Control (RBAC).

```mermaid
graph TD
    CLIENT["👤 Cliente HTTP / Swagger / Callback"]

    subgraph "HTTP Layer (app/infrastructure/http/)"
        AUTH_MID["AuthMiddleware\n(Validação de JWT e RBAC)"]
        DEP_PERM["require_permission\n(FastAPI Dependency Factory)"]
    end

    subgraph "Auth Core (app/auth/)"
        JWT_VAL["JwtValidator\n(Verificação RS256 e Claims)"]
        PERM_RES["DatabasePermissionResolver\n(Resolução e cache TTL de permissões)"]
    end

    subgraph "Domain Layer (app/domain/)"
        USER_MODEL["CurrentUser\n(Value Object: sub, roles, permissions)"]
        EXC_UNAUTH["PlatformUnauthorizedError\n(Exception)"]
        EXC_FORBID["PlatformForbiddenError\n(Exception)"]
    end

    subgraph "Infrastructure Layer (app/infrastructure/)"
        SQL_UOW["SqlUnitOfWork\n(Escopo de Transação)"]
        CONFIG["Settings\n(Configuração de PEM e claims)"]

        subgraph "RBAC Models"
            ROLE["RoleModel\n(roles)"]
            PERM["PermissionModel\n(permissions)"]
            RP["RolePermissionModel\n(associação M-N)"]
        end
    end

    CLIENT -->|"REST Request (Authorization Bearer)"| AUTH_MID
    AUTH_MID --> DEP_PERM
    DEP_PERM -.->|"Usa"| JWT_VAL
    DEP_PERM -.->|"Usa"| PERM_RES

    JWT_VAL -->|"Carrega PEM"| CONFIG
    PERM_RES -->|"Lê via SessionFactory"| ROLE
    PERM_RES -->|"Lê via SessionFactory"| PERM
    PERM_RES -->|"Lê via SessionFactory"| RP

    DEP_PERM -->|"Cria"| USER_MODEL

    DEP_PERM -.->|"Dispara se inválido"| EXC_UNAUTH
    DEP_PERM -.->|"Dispara se sem permissão"| EXC_FORBID
```

### Principais Componentes

1. **AuthMiddleware / require_permission (`app/infrastructure/http/routers/`)**:
   - Dependency factory do FastAPI que intercepta as chamadas de API, extrai o Bearer token do header, invoca a cadeia de validação e injeta a entidade `CurrentUser` no endpoint.

2. **JwtValidator (`app/auth/jwt_validator.py`)**:
   - Responsável por validar a assinatura criptográfica RS256 usando a chave pública PEM cadastrada em `Settings`.
   - Verifica data de expiração (`exp`), audience (`aud`), issuer (`iss`) e extrai as roles do usuário a partir da claim configurada (ex: claim `roles` ou um caminho aninhado).

3. **DatabasePermissionResolver (`app/auth/permission_resolver.py`)**:
   - Conecta-se às tabelas do banco de dados para buscar a lista de permissões associadas às roles extraídas do JWT.
   - Utiliza um dicionário de cache local com expiração baseada em tempo (TTL) para evitar chamadas excessivas ao banco de dados em requests sequenciais do mesmo usuário.

4. **RoleModel / PermissionModel / RolePermissionModel (`app/infrastructure/persistence/models/`)**:
   - Modelos ORM que representam o esquema físico de controle de acesso:
     - `RoleModel`: Nomes de roles como `sre`, `po_pm` e `analytics_engineer`.
     - `PermissionModel`: Permissões granulares como `pipeline:trigger`, `drift:approve`.
     - `RolePermissionModel`: Tabela associativa que liga N permissões a N roles.
