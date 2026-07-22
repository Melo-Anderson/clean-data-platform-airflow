# Mock Store API Microservice Design Spec

**Date:** 2026-07-22
**Status:** Draft / Approved by User
**Target Path:** `services/mock_store_api/`
**Goal:** Implement a standalone, isolated E-Commerce Mock API microservice running in its own container, sharing the platform PostgreSQL instance under a dedicated database schema (`mock_store`). This service acts as a target business source and sink API for validating OpenAPI discovery, data ingestion, and export workflows.

---

## 1. System Architecture & Boundaries

The `mock_store_api` is a separate FastAPI application completely decoupled from `app/` (the Data Platform). It does not import any code or domain modules from `app/`.

### Directory Structure

```text
airflow-data-platform-sdd/
├── services/
│   └── mock_store_api/
│       ├── __init__.py
│       ├── main.py               # FastAPI instance, routes, lifespan schema initialization
│       ├── config.py             # App configuration via pydantic-settings (DATABASE_URL, PORT)
│       ├── database.py           # Async SQLAlchemy engine & session maker
│       ├── models.py             # ORM models (schema="mock_store")
│       ├── schemas.py            # Pydantic schemas (Request/Response & pagination)
│       ├── seed.py               # Automatic seeding for test data
│       ├── Dockerfile            # Python 3.12-slim container build
│       └── requirements.txt      # Isolated dependencies (fastapi, uvicorn, sqlalchemy, asyncpg)
├── tests/
│   └── e2e/
│       └── test_mock_api_e2e.py  # E2E integration tests for Mock Store API
└── docker-compose.yml            # Service definition under --profile mock-api / e2e-api
```

### Key Technical Specs

| Spec | Value |
|---|---|
| Runtime | Python 3.12 (FastAPI + Uvicorn) |
| Port | `8081` (host & container) |
| Database Engine | PostgreSQL (same container `postgres:5432` as `platform_db`) |
| Database Schema | `mock_store` (isolated from `public`) |
| OpenAPI Endpoint | `GET /openapi.json` and Swagger `GET /docs` |
| Authentication | Open / Unauthenticated for E2E testing convenience |

---

## 2. Database Models (`mock_store` Schema)

All tables inherit from a base configured with `__table_args__ = {"schema": "mock_store"}`.

### 2.1 `mock_store.customers`
- `id`: `UUID` (Primary Key, auto-generated)
- `full_name`: `VARCHAR(255)`, NOT NULL
- `email`: `VARCHAR(255)`, UNIQUE INDEX, NOT NULL
- `document_id`: `VARCHAR(20)`, PII field for governance testing
- `status`: `VARCHAR(20)`, NOT NULL (default: `'ACTIVE'`)
- `created_at`: `TIMESTAMP WITH TIME ZONE`, default: `now()`

### 2.2 `mock_store.products`
- `id`: `UUID` (Primary Key, auto-generated)
- `sku`: `VARCHAR(50)`, UNIQUE INDEX, NOT NULL
- `name`: `VARCHAR(255)`, NOT NULL
- `category`: `VARCHAR(100)`, INDEX, NOT NULL
- `price`: `NUMERIC(10, 2)`, NOT NULL
- `stock_quantity`: `INTEGER`, NOT NULL, default: `0`
- `created_at`: `TIMESTAMP WITH TIME ZONE`, default: `now()`

### 2.3 `mock_store.orders`
- `id`: `UUID` (Primary Key, auto-generated)
- `customer_id`: `UUID`, Foreign Key ➔ `mock_store.customers.id`, INDEX
- `status`: `VARCHAR(30)`, NOT NULL (default: `'PENDING'`)
- `total_amount`: `NUMERIC(10, 2)`, NOT NULL
- `shipping_address`: `TEXT`
- `created_at`: `TIMESTAMP WITH TIME ZONE`, INDEX, default: `now()`

### 2.4 `mock_store.order_items`
- `id`: `UUID` (Primary Key, auto-generated)
- `order_id`: `UUID`, Foreign Key ➔ `mock_store.orders.id`, INDEX
- `product_id`: `UUID`, Foreign Key ➔ `mock_store.products.id`, INDEX
- `quantity`: `INTEGER`, NOT NULL
- `unit_price`: `NUMERIC(10, 2)`, NOT NULL

---

## 3. Data Seeding Strategy (`seed.py`)

On application startup, the service verifies if `mock_store.customers` has records. If empty, `seed.py` executes automatically to insert:
- **20 Customers** (synthetic names, emails, CPF/CNPJ documents)
- **15 Products** (categories: Electronics, Books, Clothing)
- **50 Orders** with associated **Order Items**

---

## 4. API Specification & Endpoints

### 4.1 System & Health
- `GET /health`: Returns `{"status": "ok", "service": "mock_store_api"}`.
- `GET /openapi.json`: Auto-generated OpenAPI 3.0 specification.

### 4.2 Reading / Ingestion Endpoints (GET)
Standard paginated responses for ingestion testing:

```json
{
  "data": [ ... ],
  "pagination": {
    "page": 1,
    "limit": 50,
    "total_records": 50,
    "total_pages": 1,
    "has_next": false
  }
}
```

- `GET /api/v1/customers`
  - Query parameters: `page: int = 1`, `limit: int = 50`, `status: str | None = None`
- `GET /api/v1/products`
  - Query parameters: `page: int = 1`, `limit: int = 50`, `category: str | None = None`
- `GET /api/v1/orders`
  - Query parameters: `page: int = 1`, `limit: int = 50`, `status: str | None = None`, `start_date: datetime | None = None`

### 4.3 Writing / Export Sink Endpoints (POST)
For testing data export pipelines from the Data Platform into external APIs:

- `POST /api/v1/customers`: Single customer creation. Returns `201 Created`.
- `POST /api/v1/customers/batch`: Batch customer creation. Accepts `list[CustomerCreate]`. Returns `{"inserted": N, "ids": [...]}`.
- `POST /api/v1/orders`: Single order creation. Returns `201 Created`.
- `POST /api/v1/orders/batch`: Batch order creation. Accepts `list[OrderCreate]`. Returns `{"inserted": N, "ids": [...]}`.

---

## 5. Docker Compose & Containerization

### Dockerfile (`services/mock_store_api/Dockerfile`)
```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY services/mock_store_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/mock_store_api/ /app/services/mock_store_api/
ENV PYTHONPATH=/app
CMD ["uvicorn", "services.mock_store_api.main:app", "--host", "0.0.0.0", "--port", "8081"]
```

### Docker Compose Service Profile
Added to `docker-compose.yml`:
```yaml
  mock-api:
    build:
      context: .
      dockerfile: services/mock_store_api/Dockerfile
    profiles:
      - mock-api
      - e2e-api
    ports:
      - "8081:8081"
    environment:
      DATABASE_URL: postgresql+asyncpg://airflow:airflow@postgres:5432/platform_db
      PORT: "8081"
    depends_on:
      postgres:
        condition: service_healthy
```

---

## 6. Verification & E2E Testing Plan

A new E2E test file `tests/e2e/test_mock_api_e2e.py` will verify:
1. `GET /health` returns status `ok`.
2. `GET /openapi.json` contains valid schemas for `customers`, `products`, `orders`.
3. `GET /api/v1/customers`, `GET /api/v1/products`, and `GET /api/v1/orders` return seeded data with proper pagination headers.
4. `POST /api/v1/orders/batch` successfully inserts new records and updates database state.
