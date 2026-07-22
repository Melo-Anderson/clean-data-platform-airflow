# Mock Store API Microservice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone FastAPI mock e-commerce microservice within the monorepo to act as a target for API discovery and data ingestion/export testing.

**Architecture:** A lightweight FastAPI application located in `services/mock_store_api/`, running on port 8081. It connects to the shared Postgres database but uses a dedicated schema (`mock_store`). It includes automatic startup seeding and fully mocked unit tests (no real DB connection needed).

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2.0 (Async), asyncpg, Pydantic, pytest, httpx.

## Global Constraints

- Must use asynchronous SQLAlchemy sessions.
- No imports from the main platform code (`app/`). The service is completely isolated.
- The database schema for all models must be explicitly set to `mock_store`.
- Unit tests must NOT connect to a real database. Use `dependency_override` or `unittest.mock.patch`.
- `get_settings()` must NOT use `@lru_cache` to allow `monkeypatch` to work correctly in tests.

---

### Task 1: Package Scaffolding & Configuration

**Files:**
- Create: `services/__init__.py`
- Create: `services/mock_store_api/__init__.py`
- Create: `services/mock_store_api/requirements.txt`
- Create: `services/mock_store_api/config.py`
- Create: `tests/unit/services/__init__.py`
- Create: `tests/unit/services/mock_store_api/__init__.py`
- Create: `tests/unit/services/mock_store_api/test_config.py`

**Interfaces:**
- Consumes: N/A
- Produces: `services.mock_store_api.config.Settings` with `database_url: str` and `port: int`. `get_settings() -> Settings` (no cache).

- [ ] **Step 1: Create all empty `__init__.py` files**

```bash
# Create the files with empty content
echo "" > services/__init__.py
echo "" > services/mock_store_api/__init__.py
echo "" > tests/unit/services/__init__.py
echo "" > tests/unit/services/mock_store_api/__init__.py
```

- [ ] **Step 2: Write `requirements.txt`**

Create `services/mock_store_api/requirements.txt`:

```text
fastapi>=0.111.0
uvicorn>=0.29.0
sqlalchemy>=2.0.30
asyncpg>=0.29.0
pydantic>=2.7.1
pydantic-settings>=2.2.1
httpx>=0.27.0
```

- [ ] **Step 3: Write the failing test**

`tests/unit/services/mock_store_api/test_config.py`:

```python
from services.mock_store_api.config import get_settings


def test_settings_default_values():
    settings = get_settings()
    assert settings.database_url.startswith("postgresql+asyncpg://")
    assert settings.port == 8081


def test_settings_load_from_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/testdb")
    monkeypatch.setenv("PORT", "9999")

    settings = get_settings()
    assert settings.database_url == "postgresql+asyncpg://test:test@localhost:5432/testdb"
    assert settings.port == 9999
```

- [ ] **Step 4: Run test to verify it fails**

```bash
uv run pytest tests/unit/services/mock_store_api/test_config.py -v
```
Expected: FAIL with `ModuleNotFoundError: No module named 'services.mock_store_api.config'`

- [ ] **Step 5: Write minimal implementation**

`services/mock_store_api/config.py`:

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://airflow:airflow@localhost:5432/platform_db"
    port: int = 8081


def get_settings() -> Settings:
    # No @lru_cache: allows monkeypatch.setenv to work correctly in tests
    return Settings()
```

- [ ] **Step 6: Run test to verify it passes**

```bash
uv run pytest tests/unit/services/mock_store_api/test_config.py -v
```
Expected: 2 PASSED

- [ ] **Step 7: Commit**

```bash
git add services/__init__.py services/mock_store_api/__init__.py services/mock_store_api/requirements.txt services/mock_store_api/config.py tests/unit/services/__init__.py tests/unit/services/mock_store_api/__init__.py tests/unit/services/mock_store_api/test_config.py
git commit -m "feat(mock-api): scaffold package structure and add configuration"
```

---

### Task 2: Database Connection Setup

**Files:**
- Create: `services/mock_store_api/database.py`
- Create: `tests/unit/services/mock_store_api/test_database.py`

**Interfaces:**
- Consumes: `services.mock_store_api.config.get_settings()`
- Produces:
  - `Base`: SQLAlchemy `DeclarativeBase` for ORM models
  - `engine`: `AsyncEngine` instance
  - `AsyncSessionLocal`: `async_sessionmaker[AsyncSession]`
  - `get_db() -> AsyncGenerator[AsyncSession, None]`: FastAPI dependency

- [ ] **Step 1: Write the failing test**

`tests/unit/services/mock_store_api/test_database.py`:

```python
import inspect
from sqlalchemy.ext.asyncio import AsyncSession
from services.mock_store_api.database import get_db, Base, engine


def test_get_db_is_async_generator():
    # Verifies the function is an async generator without making a real DB connection
    assert inspect.isasyncgenfunction(get_db)


def test_base_is_declarative():
    # Verifies Base is the SQLAlchemy ORM declarative base
    from sqlalchemy.orm import DeclarativeBase
    assert issubclass(Base, DeclarativeBase)


def test_engine_is_not_none():
    assert engine is not None
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/unit/services/mock_store_api/test_database.py -v
```
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

`services/mock_store_api/database.py`:

```python
from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from services.mock_store_api.config import get_settings

_settings = get_settings()

engine = create_async_engine(_settings.database_url, echo=False)
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/unit/services/mock_store_api/test_database.py -v
```
Expected: 3 PASSED

- [ ] **Step 5: Commit**

```bash
git add services/mock_store_api/database.py tests/unit/services/mock_store_api/test_database.py
git commit -m "feat(mock-api): add async database connection and session factory"
```

---

### Task 3: Database Models

**Files:**
- Create: `services/mock_store_api/models.py`
- Create: `tests/unit/services/mock_store_api/test_models.py`

**Interfaces:**
- Consumes: `services.mock_store_api.database.Base`
- Produces: ORM classes `Customer`, `Product`, `Order`, `OrderItem` — all in schema `mock_store`.

- [ ] **Step 1: Write the failing test**

`tests/unit/services/mock_store_api/test_models.py`:

```python
from services.mock_store_api.models import Customer, Product, Order, OrderItem


def _get_schema(model) -> str:
    args = model.__table_args__
    if isinstance(args, dict):
        return args.get("schema", "")
    # tuple form: last element is a dict
    for item in args:
        if isinstance(item, dict):
            return item.get("schema", "")
    return ""


def test_all_models_use_mock_store_schema():
    assert _get_schema(Customer) == "mock_store"
    assert _get_schema(Product) == "mock_store"
    assert _get_schema(Order) == "mock_store"
    assert _get_schema(OrderItem) == "mock_store"


def test_customer_has_required_columns():
    cols = {c.name for c in Customer.__table__.columns}
    assert {"id", "full_name", "email", "document_id", "status", "created_at"} <= cols


def test_order_has_fk_to_customer():
    fks = {fk.target_fullname for fk in Order.__table__.foreign_keys}
    assert "mock_store.customers.id" in fks


def test_order_item_has_fk_to_order_and_product():
    fks = {fk.target_fullname for fk in OrderItem.__table__.foreign_keys}
    assert "mock_store.orders.id" in fks
    assert "mock_store.products.id" in fks
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/unit/services/mock_store_api/test_models.py -v
```
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

`services/mock_store_api/models.py`:

```python
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Integer, Numeric, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from services.mock_store_api.database import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Customer(Base):
    __tablename__ = "customers"
    __table_args__ = {"schema": "mock_store"}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    document_id: Mapped[str | None] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    orders: Mapped[list[Order]] = relationship("Order", back_populates="customer")


class Product(Base):
    __tablename__ = "products"
    __table_args__ = {"schema": "mock_store"}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sku: Mapped[str] = mapped_column(String(50), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    price: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    stock_quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = {"schema": "mock_store"}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("mock_store.customers.id"), index=True)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="PENDING")
    total_amount: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    shipping_address: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)

    customer: Mapped[Customer] = relationship("Customer", back_populates="orders")
    items: Mapped[list[OrderItem]] = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")


class OrderItem(Base):
    __tablename__ = "order_items"
    __table_args__ = {"schema": "mock_store"}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("mock_store.orders.id"), index=True)
    product_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("mock_store.products.id"), index=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)

    order: Mapped[Order] = relationship("Order", back_populates="items")
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/unit/services/mock_store_api/test_models.py -v
```
Expected: 4 PASSED

- [ ] **Step 5: Commit**

```bash
git add services/mock_store_api/models.py tests/unit/services/mock_store_api/test_models.py
git commit -m "feat(mock-api): add SQLAlchemy ORM models under mock_store schema"
```

---

### Task 4: Pydantic Schemas

**Files:**
- Create: `services/mock_store_api/schemas.py`
- Create: `tests/unit/services/mock_store_api/test_schemas.py`

**Interfaces:**
- Consumes: N/A
- Produces:
  - `PaginationMeta`, `PaginatedResponse[T]`
  - `CustomerCreate`, `CustomerResponse`
  - `ProductResponse`
  - `OrderItemCreate`, `OrderCreate`, `OrderResponse`
  - `BatchInsertResult`

- [ ] **Step 1: Write the failing test**

`tests/unit/services/mock_store_api/test_schemas.py`:

```python
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from services.mock_store_api.schemas import (
    BatchInsertResult,
    CustomerCreate,
    CustomerResponse,
    OrderCreate,
    OrderItemCreate,
    OrderResponse,
    PaginatedResponse,
    PaginationMeta,
    ProductResponse,
)


def test_customer_create_schema():
    customer = CustomerCreate(full_name="John Doe", email="john@example.com", document_id="12345678901", status="ACTIVE")
    assert customer.full_name == "John Doe"
    assert customer.status == "ACTIVE"


def test_paginated_response_with_customers():
    now = datetime.now(timezone.utc)
    cid = uuid.uuid4()
    item = CustomerResponse(id=cid, full_name="A", email="a@b.com", created_at=now)
    meta = PaginationMeta(page=1, limit=10, total_records=1, total_pages=1, has_next=False)
    resp = PaginatedResponse[CustomerResponse](data=[item], pagination=meta)
    assert resp.pagination.page == 1
    assert len(resp.data) == 1
    assert resp.data[0].id == cid


def test_order_create_with_items():
    order = OrderCreate(
        customer_id=uuid.uuid4(),
        total_amount=Decimal("99.90"),
        items=[OrderItemCreate(product_id=uuid.uuid4(), quantity=2, unit_price=Decimal("49.95"))],
    )
    assert len(order.items) == 1


def test_batch_insert_result():
    ids = [uuid.uuid4(), uuid.uuid4()]
    result = BatchInsertResult(inserted=2, ids=ids)
    assert result.inserted == 2
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/unit/services/mock_store_api/test_schemas.py -v
```
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

`services/mock_store_api/schemas.py`:

```python
from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


class PaginationMeta(BaseModel):
    page: int
    limit: int
    total_records: int
    total_pages: int
    has_next: bool


class PaginatedResponse(BaseModel, Generic[T]):
    data: list[T]
    pagination: PaginationMeta


class CustomerBase(BaseModel):
    full_name: str
    email: str
    document_id: str | None = None
    status: str = "ACTIVE"


class CustomerCreate(CustomerBase):
    pass


class CustomerResponse(CustomerBase):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    created_at: datetime


class ProductResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    sku: str
    name: str
    category: str
    price: Decimal
    stock_quantity: int
    created_at: datetime


class OrderItemCreate(BaseModel):
    product_id: uuid.UUID
    quantity: int
    unit_price: Decimal


class OrderCreate(BaseModel):
    customer_id: uuid.UUID
    status: str = "PENDING"
    total_amount: Decimal
    shipping_address: str | None = None
    items: list[OrderItemCreate] = Field(default_factory=list)


class OrderResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    customer_id: uuid.UUID
    status: str
    total_amount: Decimal
    shipping_address: str | None = None
    created_at: datetime


class BatchInsertResult(BaseModel):
    inserted: int
    ids: list[uuid.UUID]
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/unit/services/mock_store_api/test_schemas.py -v
```
Expected: 4 PASSED

- [ ] **Step 5: Commit**

```bash
git add services/mock_store_api/schemas.py tests/unit/services/mock_store_api/test_schemas.py
git commit -m "feat(mock-api): add pydantic request and response schemas"
```

---

### Task 5: Data Seeding Logic

**Files:**
- Create: `services/mock_store_api/seed.py`
- Create: `tests/unit/services/mock_store_api/test_seed.py`

**Interfaces:**
- Consumes: `services.mock_store_api.database.AsyncSessionLocal`, `Customer`, `Product`, `Order` models.
- Produces: `async def seed_data_if_empty() -> None`

- [ ] **Step 1: Write the failing test**

`tests/unit/services/mock_store_api/test_seed.py`:

```python
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
@patch("services.mock_store_api.seed.AsyncSessionLocal")
async def test_seed_skips_if_already_has_data(mock_session_maker):
    mock_session = AsyncMock()
    mock_session_maker.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_maker.return_value.__aexit__ = AsyncMock(return_value=False)

    mock_result = MagicMock()
    mock_result.scalar_one.return_value = 5  # 5 customers already exist
    mock_session.execute = AsyncMock(return_value=mock_result)

    from services.mock_store_api.seed import seed_data_if_empty

    await seed_data_if_empty()

    mock_session.add_all.assert_not_called()
    mock_session.commit.assert_not_called()


@pytest.mark.asyncio
@patch("services.mock_store_api.seed.AsyncSessionLocal")
async def test_seed_inserts_data_when_empty(mock_session_maker):
    mock_session = AsyncMock()
    mock_session_maker.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_maker.return_value.__aexit__ = AsyncMock(return_value=False)

    mock_result = MagicMock()
    mock_result.scalar_one.return_value = 0  # no customers yet
    mock_session.execute = AsyncMock(return_value=mock_result)

    from services.mock_store_api.seed import seed_data_if_empty

    await seed_data_if_empty()

    assert mock_session.add_all.call_count == 3  # customers, products, orders
    mock_session.commit.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/unit/services/mock_store_api/test_seed.py -v
```
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

`services/mock_store_api/seed.py`:

```python
from __future__ import annotations

import logging
import uuid

from sqlalchemy import func, select

from services.mock_store_api.database import AsyncSessionLocal
from services.mock_store_api.models import Customer, Order, Product

logger = logging.getLogger(__name__)


async def seed_data_if_empty() -> None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(func.count(Customer.id)))
        count = result.scalar_one()

        if count > 0:
            logger.info("Database already seeded with %d customers. Skipping.", count)
            return

        logger.info("Seeding database with initial data...")

        customers = [
            Customer(
                id=uuid.uuid4(),
                full_name=f"Customer {i}",
                email=f"customer{i}@example.com",
                document_id=f"{10000000000 + i}",
                status="ACTIVE",
            )
            for i in range(20)
        ]
        session.add_all(customers)

        products = [
            Product(
                id=uuid.uuid4(),
                sku=f"SKU-{i:04d}",
                name=f"Product {i}",
                category="Electronics" if i % 2 == 0 else "Books",
                price=float(10 + i),
                stock_quantity=100,
            )
            for i in range(15)
        ]
        session.add_all(products)

        orders = [
            Order(
                id=uuid.uuid4(),
                customer_id=customers[i % 20].id,
                total_amount=float(50 + i * 2),
                status="PAID",
                shipping_address=f"Rua Exemplo, {i + 1}",
            )
            for i in range(50)
        ]
        session.add_all(orders)

        await session.commit()
        logger.info("Database seeded: 20 customers, 15 products, 50 orders.")
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/unit/services/mock_store_api/test_seed.py -v
```
Expected: 2 PASSED

- [ ] **Step 5: Commit**

```bash
git add services/mock_store_api/seed.py tests/unit/services/mock_store_api/test_seed.py
git commit -m "feat(mock-api): add auto-seeding logic with idempotency check"
```

---

### Task 6: FastAPI Application & Endpoints

**Files:**
- Create: `services/mock_store_api/main.py`
- Create: `tests/unit/services/mock_store_api/test_main.py`

**Interfaces:**
- Consumes: `Customer`, `Product`, `Order`, `OrderItem` models; all schemas from Task 4; `get_db` from Task 2; `seed_data_if_empty` from Task 5.
- Produces: `app: FastAPI` instance with all routes registered.

- [ ] **Step 1: Write the failing test**

The tests use FastAPI's `app.dependency_overrides` to swap `get_db` for a mock session, so no real database connection is needed.

`tests/unit/services/mock_store_api/test_main.py`:

```python
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def mock_db_session():
    session = AsyncMock()
    return session


@pytest.fixture()
def client(mock_db_session):
    from services.mock_store_api.database import get_db
    from services.mock_store_api.main import app

    async def override_get_db():
        yield mock_db_session

    app.dependency_overrides[get_db] = override_get_db
    with patch("services.mock_store_api.main.seed_data_if_empty", new_callable=AsyncMock):
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c
    app.dependency_overrides.clear()


def test_health_returns_ok(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "mock_store_api"}


def test_customers_endpoint_returns_paginated_structure(client, mock_db_session):
    # Mock the two DB calls: SELECT customers + COUNT
    mock_result_items = MagicMock()
    mock_result_items.scalars.return_value.all.return_value = []
    count_result = MagicMock()
    count_result.__await__ = AsyncMock(return_value=0).__await__

    mock_db_session.execute = AsyncMock(return_value=mock_result_items)
    mock_db_session.scalar = AsyncMock(return_value=0)

    response = client.get("/api/v1/customers")
    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert "pagination" in body
    assert body["pagination"]["page"] == 1


def test_products_endpoint_returns_paginated_structure(client, mock_db_session):
    mock_result_items = MagicMock()
    mock_result_items.scalars.return_value.all.return_value = []
    mock_db_session.execute = AsyncMock(return_value=mock_result_items)
    mock_db_session.scalar = AsyncMock(return_value=0)

    response = client.get("/api/v1/products")
    assert response.status_code == 200
    assert "data" in response.json()


def test_orders_endpoint_returns_paginated_structure(client, mock_db_session):
    mock_result_items = MagicMock()
    mock_result_items.scalars.return_value.all.return_value = []
    mock_db_session.execute = AsyncMock(return_value=mock_result_items)
    mock_db_session.scalar = AsyncMock(return_value=0)

    response = client.get("/api/v1/orders")
    assert response.status_code == 200
    assert "data" in response.json()


def test_customers_batch_endpoint_exists(client, mock_db_session):
    mock_db_session.commit = AsyncMock()
    response = client.post("/api/v1/customers/batch", json=[
        {"full_name": "Jane", "email": "jane@test.com"}
    ])
    # 200 or 201 means the route exists and was reached
    assert response.status_code in (200, 201, 422)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/unit/services/mock_store_api/test_main.py -v
```
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

`services/mock_store_api/main.py`:

```python
from __future__ import annotations

import math
import uuid
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import Depends, FastAPI
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from services.mock_store_api.database import Base, engine, get_db
from services.mock_store_api.models import Customer, Order, OrderItem, Product
from services.mock_store_api.schemas import (
    BatchInsertResult,
    CustomerCreate,
    CustomerResponse,
    OrderCreate,
    OrderResponse,
    PaginatedResponse,
    PaginationMeta,
    ProductResponse,
)
from services.mock_store_api.seed import seed_data_if_empty


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    async with engine.begin() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS mock_store"))
        await conn.run_sync(Base.metadata.create_all)
    await seed_data_if_empty()
    yield


app = FastAPI(title="Mock Store API", version="1.0.0", lifespan=lifespan)


def _build_pagination(page: int, limit: int, total: int) -> PaginationMeta:
    total_pages = math.ceil(total / limit) if limit > 0 else 1
    return PaginationMeta(
        page=page, limit=limit, total_records=total, total_pages=total_pages, has_next=page < total_pages
    )


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "mock_store_api"}


@app.get("/api/v1/customers", response_model=PaginatedResponse[CustomerResponse])
async def list_customers(
    page: int = 1, limit: int = 50, status: str | None = None, db: AsyncSession = Depends(get_db)
) -> dict:
    offset = (page - 1) * limit
    query = select(Customer)
    if status:
        query = query.where(Customer.status == status)
    result = await db.execute(query.offset(offset).limit(limit))
    items = result.scalars().all()
    total = await db.scalar(select(func.count(Customer.id))) or 0
    return {"data": items, "pagination": _build_pagination(page, limit, total)}


@app.get("/api/v1/products", response_model=PaginatedResponse[ProductResponse])
async def list_products(
    page: int = 1, limit: int = 50, category: str | None = None, db: AsyncSession = Depends(get_db)
) -> dict:
    offset = (page - 1) * limit
    query = select(Product)
    if category:
        query = query.where(Product.category == category)
    result = await db.execute(query.offset(offset).limit(limit))
    items = result.scalars().all()
    total = await db.scalar(select(func.count(Product.id))) or 0
    return {"data": items, "pagination": _build_pagination(page, limit, total)}


@app.get("/api/v1/orders", response_model=PaginatedResponse[OrderResponse])
async def list_orders(
    page: int = 1, limit: int = 50, status: str | None = None, db: AsyncSession = Depends(get_db)
) -> dict:
    offset = (page - 1) * limit
    query = select(Order)
    if status:
        query = query.where(Order.status == status)
    result = await db.execute(query.offset(offset).limit(limit))
    items = result.scalars().all()
    total = await db.scalar(select(func.count(Order.id))) or 0
    return {"data": items, "pagination": _build_pagination(page, limit, total)}


@app.post("/api/v1/customers", status_code=201)
async def create_customer(customer: CustomerCreate, db: AsyncSession = Depends(get_db)) -> dict:
    new_customer = Customer(id=uuid.uuid4(), **customer.model_dump())
    db.add(new_customer)
    await db.commit()
    return {"message": "Customer created", "id": str(new_customer.id)}


@app.post("/api/v1/customers/batch", response_model=BatchInsertResult)
async def create_customers_batch(
    customers: list[CustomerCreate], db: AsyncSession = Depends(get_db)
) -> dict:
    db_customers = [Customer(id=uuid.uuid4(), **c.model_dump()) for c in customers]
    db.add_all(db_customers)
    await db.commit()
    return {"inserted": len(db_customers), "ids": [c.id for c in db_customers]}


@app.post("/api/v1/orders", status_code=201)
async def create_order(order: OrderCreate, db: AsyncSession = Depends(get_db)) -> dict:
    data = order.model_dump()
    items_data = data.pop("items", [])
    new_order = Order(id=uuid.uuid4(), **data)
    for item in items_data:
        new_order.items.append(OrderItem(id=uuid.uuid4(), **item))
    db.add(new_order)
    await db.commit()
    return {"message": "Order created", "id": str(new_order.id)}


@app.post("/api/v1/orders/batch", response_model=BatchInsertResult)
async def create_orders_batch(orders: list[OrderCreate], db: AsyncSession = Depends(get_db)) -> dict:
    db_orders = []
    for o in orders:
        data = o.model_dump()
        items_data = data.pop("items", [])
        order_obj = Order(id=uuid.uuid4(), **data)
        for item in items_data:
            order_obj.items.append(OrderItem(id=uuid.uuid4(), **item))
        db_orders.append(order_obj)
    db.add_all(db_orders)
    await db.commit()
    return {"inserted": len(db_orders), "ids": [o.id for o in db_orders]}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/unit/services/mock_store_api/test_main.py -v
```
Expected: 5 PASSED

- [ ] **Step 5: Run full unit test suite to ensure no regressions**

```bash
uv run pytest tests/unit/ -v
```
Expected: All PASSED

- [ ] **Step 6: Commit**

```bash
git add services/mock_store_api/main.py tests/unit/services/mock_store_api/test_main.py
git commit -m "feat(mock-api): add FastAPI application with all endpoints and mocked unit tests"
```

---

### Task 7: Dockerization, Docker Compose & E2E Test

**Files:**
- Create: `services/mock_store_api/Dockerfile`
- Modify: `docker-compose.yml` — add `mock-api` service
- Create: `tests/e2e/test_mock_api_e2e.py`

**Interfaces:**
- Consumes: Completed `services/mock_store_api/` package.
- Produces: `mock-api` container reachable at `http://localhost:8081`; E2E tests validating seeded data and write paths.

- [ ] **Step 1: Write `Dockerfile`**

`services/mock_store_api/Dockerfile`:

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY services/mock_store_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/ /app/services/
ENV PYTHONPATH=/app
CMD ["uvicorn", "services.mock_store_api.main:app", "--host", "0.0.0.0", "--port", "8081"]
```

- [ ] **Step 2: Add `mock-api` service to `docker-compose.yml`**

Under the `services:` block in `docker-compose.yml`, add:

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

- [ ] **Step 3: Write E2E test file**

`tests/e2e/test_mock_api_e2e.py`:

```python
import os

import httpx
import pytest


def _base_url() -> str:
    host = os.getenv("MOCK_API_HOST", "localhost")
    return f"http://{host}:8081"


@pytest.mark.asyncio
async def test_health_returns_ok():
    async with httpx.AsyncClient(base_url=_base_url()) as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_openapi_schema_has_expected_paths():
    async with httpx.AsyncClient(base_url=_base_url()) as client:
        response = await client.get("/openapi.json")
    assert response.status_code == 200
    paths = response.json()["paths"]
    assert "/api/v1/customers" in paths
    assert "/api/v1/products" in paths
    assert "/api/v1/orders" in paths


@pytest.mark.asyncio
async def test_customers_returns_seeded_data_with_pagination():
    async with httpx.AsyncClient(base_url=_base_url()) as client:
        response = await client.get("/api/v1/customers?page=1&limit=10")
    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert "pagination" in body
    assert body["pagination"]["total_records"] >= 20  # at least seeded 20
    assert body["pagination"]["page"] == 1
    assert len(body["data"]) <= 10


@pytest.mark.asyncio
async def test_products_returns_seeded_data():
    async with httpx.AsyncClient(base_url=_base_url()) as client:
        response = await client.get("/api/v1/products")
    assert response.status_code == 200
    body = response.json()
    assert body["pagination"]["total_records"] >= 15  # at least seeded 15


@pytest.mark.asyncio
async def test_orders_returns_seeded_data():
    async with httpx.AsyncClient(base_url=_base_url()) as client:
        response = await client.get("/api/v1/orders")
    assert response.status_code == 200
    body = response.json()
    assert body["pagination"]["total_records"] >= 50  # at least seeded 50


@pytest.mark.asyncio
async def test_batch_create_customers():
    payload = [
        {"full_name": "E2E Customer 1", "email": "e2ecust1@test.com"},
        {"full_name": "E2E Customer 2", "email": "e2ecust2@test.com"},
    ]
    async with httpx.AsyncClient(base_url=_base_url()) as client:
        response = await client.post("/api/v1/customers/batch", json=payload)
    assert response.status_code == 200
    body = response.json()
    assert body["inserted"] == 2
    assert len(body["ids"]) == 2


@pytest.mark.asyncio
async def test_batch_create_orders():
    # First get a valid customer_id from the seeded data
    async with httpx.AsyncClient(base_url=_base_url()) as client:
        customers_resp = await client.get("/api/v1/customers?limit=1")
        customer_id = customers_resp.json()["data"][0]["id"]

        payload = [
            {"customer_id": customer_id, "total_amount": 199.99, "status": "PENDING"},
        ]
        response = await client.post("/api/v1/orders/batch", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["inserted"] == 1
    assert len(body["ids"]) == 1
```

- [ ] **Step 4: Build and run mock-api container**

```bash
docker compose --profile core --profile mock-api up -d --build
```

- [ ] **Step 5: Run E2E tests**

```bash
uv run pytest tests/e2e/test_mock_api_e2e.py -v
```
Expected: 7 PASSED

- [ ] **Step 6: Commit**

```bash
git add services/mock_store_api/Dockerfile docker-compose.yml tests/e2e/test_mock_api_e2e.py
git commit -m "feat(mock-api): add Dockerfile, docker-compose profile, and full E2E test suite"
```
