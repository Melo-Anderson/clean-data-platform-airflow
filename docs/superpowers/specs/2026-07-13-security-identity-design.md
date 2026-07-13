# Design Specification: Security & Identity (JWT & Granular RBAC)

**Date:** 2026-07-13
**Area:** Security / Authentication / Authorization
**Status:** Approved for Implementation
**Group:** 2 (World-Class Engineering)

## 1. Context & Motivation

Currently, the platform relies on a mock authentication mechanism (`Authorization: Bearer <role>`) that maps tokens directly to roles without any cryptographic validation. The authorization is also coarse-grained (based directly on roles like `SRE` or `PO_PM`).

To transition the platform to a secure, enterprise-grade state, we need:
- Standard cryptographically validated **JSON Web Tokens (JWT)** using asymmetric RSA cryptography with a local static public key.
- **Granular Role-Based Access Control (RBAC)** where roles are mapped to specific domain-level permissions in a normalized relational database schema.
- High-performance permission resolution leveraging **In-Memory Caching** to avoid hitting the database on every single API request.

## 2. Asymmetric JWT Authentication

The application will validate JWTs issued by any standard OIDC Identity Provider (Keycloak, Auth0) using a statically configured RSA public key.

### Configuration
The following environment variables will control JWT verification:
- `AUTH_JWT_PUBLIC_KEY_PEM`: Public RSA key in PEM format used for signature verification.
- `AUTH_JWT_ISSUER`: (Optional) Expected issuer claim (`iss`).
- `AUTH_JWT_AUDIENCE`: (Optional) Expected audience claim (`aud`).
- `JWT_ROLES_CLAIM`: (Optional) Path to the claim containing user roles (defaults to `"roles"`).

### Extraction & Verification Logic
1. Extraction: The existing `HTTPBearer` dependency intercepts the HTTP `Authorization` header and extracts the raw token.
2. Decoding & Validation: The token is decoded and verified using the static PEM public key (`algorithms=["RS256"]`). Claims like `exp` (expiration), `iss`, and `aud` are checked.
3. Role Resolution: The role(s) are extracted from the claim defined by `JWT_ROLES_CLAIM`. If the claim is nested (e.g. `realm_access.roles`), the parser will traverse the token dictionary. If the claim is a string, it is treated as a single role; if a list of strings, all are extracted.

## 3. Database Schema for Granular RBAC

We will replace role-level checks with fine-grained permission-level checks. The mapping of roles to permissions will be stored in a normalized relational schema.

### Database Models
Three new database entities will be introduced using SQLAlchemy:
- `PermissionModel` (table: `permissions`):
  - `id`: Primary key (Integer/UUID).
  - `name`: Unique string identifier (e.g., `"pipeline:create"`).
- `RoleModel` (table: `roles`):
  - `id`: Primary key (Integer/UUID).
  - `name`: Unique string identifier (e.g., `"sre"`).
- `RolePermissionModel` (table: `role_permissions`):
  - `role_id`: Foreign key referencing `roles.id`.
  - `permission_id`: Foreign key referencing `permissions.id`.
  - Primary key is the composite of `(role_id, permission_id)`.

### Seed Data
The database initialization script (`scripts/init_db.py`) will automatically seed the initial roles and their granular permissions:

| Role | Permissions |
|---|---|
| `sre` | `pipeline:create`, `pipeline:delete`, `pipeline:trigger`, `pipeline:view`, `drift:approve`, `drift:view`, `catalog:view`, `catalog:sync` |
| `analytics_engineer` | `pipeline:view`, `pipeline:trigger`, `catalog:view`, `catalog:sync` |
| `po_pm` | `pipeline:view`, `drift:approve`, `drift:view`, `catalog:view` |

## 4. Permission Resolver & Caching

Querying the database on every authenticated HTTP request introduces unacceptable latency. We will introduce a caching layer to store role-to-permission mappings.

### PermissionResolver API
We will implement a `PermissionResolver` service:
```python
class PermissionResolver:
    async def get_permissions_for_roles(self, roles: list[str]) -> set[str]:
        """Resolves the union of permissions assigned to the given roles, using cache."""
        ...
```

### Caching Strategy
- The resolver will use a simple, thread-safe in-memory cache dictionary.
- Cache keys: String representation of sorted roles (e.g., `"analytics_engineer,sre"`).
- Cache value: Set of resolved permission names (e.g., `{"pipeline:trigger", "pipeline:view"}`).
- Caching TTL: A configurable time-to-live (e.g., 5 minutes) to ensure that manual database modifications are eventually propagated without requiring a server reboot.

## 5. FastAPI Integration & Enforcement

The existing `require_role` dependency factory will be deprecated in favor of a permission-based check.

### Authorization Flow
1. API Endpoint uses `Depends(require_permission("pipeline:trigger"))`.
2. `get_current_user` extracts the JWT, verifies its signature, extracts user roles, and returns a `CurrentUser` object.
3. `require_permission` calls the `PermissionResolver` using the roles in `CurrentUser`.
4. If the required permission string is present in the resolved permissions, request proceeds.
5. If the permission is missing, or the JWT is invalid, it raises the standard `DomainException` subclass (e.g., `PlatformForbiddenError` or `PlatformUnauthorizedError`), mapping to RFC 7807 problem responses (handled by exception handlers from Group 1).

## 6. Verification Plan

### Automated Tests
- **JWT Verification Tests**: Test validation of valid, expired, and invalidly signed RS256 JWTs using an in-memory generated RSA key pair.
- **RBAC DB Tests**: Verify that `Role`, `Permission`, and association tables are correctly mapped and queried.
- **Cache Tests**: Verify that `PermissionResolver` cache works, hits/misses behave correctly, and expires according to TTL.
- **Endpoint Tests**: Verify that endpoints return 403 Forbidden (RFC 7807 format) when the user does not have the required permission.

### Manual Verification
- Execute `init_db` script to seed tables.
- Query API endpoints using standard curl/HTTP client with generated JWTs representing different roles.

## 7. Spec Amendments (Post-Brainstorming Audit)

The following gaps were identified by code inspection and corrected before implementation:

### A. CurrentUser Expansion
`app/auth/current_user.py` currently holds `role: Role` (a single enum). JWT tokens from real IDPs typically carry one or more roles. The field must be changed to `roles: list[str]` (plain strings, not enum, to avoid hard coupling on every future role addition). All call sites are updated to use `roles` instead of `role`.

### B. New Security Exceptions
Two new `DomainException` subclasses must be added to `app/domain/shared/exceptions.py`:
- `PlatformUnauthorizedError`: maps to HTTP **401** — raised when a token is missing, expired, or has an invalid signature.
- `PlatformForbiddenError`: maps to HTTP **403** — raised when a valid token exists but the user lacks the required permission.

Both are registered in `app/infrastructure/http/exception_handlers.py` with RFC 7807 responses.

### C. Router Migration Scope
The following 4 router files will have their `require_role(...)` calls replaced with `require_permission(...)` calls using the permissions defined in Section 3 seed data:

| Router | Old Guard | New Permission |
|---|---|---|
| `pipeline_router.py` POST `/` | `require_role(PO_PM, ANALYTICS_ENGINEER)` | `require_permission("pipeline:create")` |
| `pipeline_router.py` POST `/{id}/trigger` | `require_role(PO_PM, ANALYTICS_ENGINEER, SRE)` | `require_permission("pipeline:trigger")` |
| `discovery_router.py` GET `/` | `require_role(PO_PM, ANALYTICS_ENGINEER, SRE)` | `require_permission("catalog:view")` |
| `discovery_router.py` POST `/approve` | `require_role(PO_PM)` | `require_permission("drift:approve")` |
| `endpoint_router.py` (all) | `require_role(SRE, PO_PM)` | `require_permission("catalog:sync")` |
| `asset_router.py` GET `/` | `require_role(PO_PM, ANALYTICS_ENGINEER)` | `require_permission("catalog:view")` |
| `asset_router.py` POST `/sync` | `require_role(SRE)` | `require_permission("catalog:sync")` |
| `asset_router.py` POST `/approve` | `require_role(PO_PM)` | `require_permission("drift:approve")` |

### D. Settings Expansion
`app/config.py` must gain three new optional settings with `PLATFORM_` prefix:
- `auth_jwt_public_key_pem: str = ""` — PEM-encoded RSA public key.
- `auth_jwt_issuer: str = ""` — expected `iss` claim (empty = skip validation).
- `auth_jwt_audience: str = ""` — expected `aud` claim (empty = skip validation).
- `jwt_roles_claim: str = "roles"` — JWT claim path for role extraction.
- `permission_cache_ttl_seconds: int = 300` — TTL for the permission resolver cache.
