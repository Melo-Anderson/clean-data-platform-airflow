# ADR 005: Autenticação JWT Asimétrica (RS256) e RBAC Granular com Cache In-Memory

## Status
Aprovado

## Contexto
Anteriormente, a plataforma utilizava uma autenticação mockada (`Authorization: Bearer <role>`) mapeando requisições diretamente para papéis estáticos sem validação criptográfica. O modelo de autorização também era grosseiro e acoplado diretamente aos papéis (ex: SRE, PO_PM). Para tornar o sistema seguro em nível corporativo, é fundamental:
1. Validar a identidade de forma segura usando tokens JWT assinados por chaves públicas/privadas (RS256).
2. Substituir autorizações baseadas em papéis (Roles) por autorizações granulares baseadas em permissões específicas de domínio (ex: `pipeline:trigger`, `catalog:sync`).
3. Mitigar latência extra nas requisições HTTP introduzida pelas consultas frequentes ao banco de dados para checagem de permissões do usuário.

## Decisão
1. **Verificação de JWT Asimétrico (RS256):**
   - Utilização de uma chave pública RSA estática configurada (`AUTH_JWT_PUBLIC_KEY_PEM`) para validar a assinatura dos tokens JWT provenientes de provedores OIDC padrão (Keycloak/Auth0).
   - Extração dinâmica de papéis mapeados em claims parametrizáveis.
2. **Esquema de RBAC Granular no Banco:**
   - Criação de modelos relacionais estruturados (`permissions`, `roles`, `role_permissions`).
   - Mapeamento dinâmico na inicialização do banco (`scripts/init_db.py`) associando papéis às permissões granulares permitidas.
3. **Resolução de Permissões com Cache em Memória:**
   - Criação do serviço `PermissionResolver` equipado com cache em memória baseado em TTL (5 minutos por padrão) para resolver a união de permissões associadas às roles do usuário.
4. **Middleware de Autorização:**
   - Substituição de `require_role` por `require_permission` em todos os endpoints da API FastAPI, levantando exceções padronizadas conforme RFC 7807 (`PlatformUnauthorizedError` e `PlatformForbiddenError`).

## Consequências
- **Positivas**:
  - Segurança aprimorada contra falsificação de identidade.
  - Flexibilidade total para adicionar/remover permissões sem precisar refatorar código ou rotas.
  - Desempenho preservado graças ao cache in-memory do `PermissionResolver`.
- **Negativas**:
  - Dependência de inicialização e seeding do banco de dados para a resolução correta das permissões locais durante testes.
  - O cache em memória pode levar até o TTL expirar para propagar modificações manuais diretas efetuadas na tabela de permissões no banco.
