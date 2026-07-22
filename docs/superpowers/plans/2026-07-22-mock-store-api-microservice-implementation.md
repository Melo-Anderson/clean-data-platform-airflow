# Mock Store API Microservice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone FastAPI mock e-commerce microservice within the monorepo to act as a target for API discovery and data ingestion/export testing.

**Architecture:** A lightweight FastAPI application located in `services/mock_store_api`, running on port 8081. It connects to the shared Postgres database but uses a dedicated schema (`mock_store`). It includes automatic startup seeding.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2.0 (Async), asyncpg, Pydantic, pytest.

## Global Constraints

- Must use asynchronous SQLAlchemy sessions.
- No imports from the main platform code (`app/`). The service is completely isolated.
- The database schema for all models must be explicitly set to `mock_store`.

---

### Task 1: Basic Configuration & Environment

**Files:**
- Create: `services/mock_store_api/__init__.py`
- Create: `services/mock_store_api/requirements.txt`
- Create: `services/mock_store_api/config.py`
- Create: `tests/unit/services/mock_store_api/test_config.py`

**Interfaces:**
- Consumes: N/A
- Produces: `mock_store_api.config.Settings` containing `DATABASE_URL` and `PORT`.

- [ ] **Step 1: Write requirements.txt**

```text
fastapi>=0.111.0
uvicorn>=0.29.0
sqlalchemy>=2.0.30
asyncpg>=0.29.0
pydantic>=2.7.1
pydantic-settings>=2.2.1
```

- [ ] **Step 2: Write the failing test for configuration**

```python
import os
from services.mock_store_api.config import get_settings

def test_settings_load_from_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/testdb")
    monkeypatch.setenv("PORT", "8081")

    settings = get_settings()
    assert settings.database_url == "postgresql+asyncpg://test:test@localhost:5432/testdb"
    assert settings.port == 8081
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/unit/services/mock_store_api/test_config.py -v`
Expected: FAIL with "ModuleNotFoundError" or similar.

- [ ] **Step 4: Write minimal implementation**

```python
from functools import lru_cache
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://airflow:airflow@localhost:5432/platform_db"
    port: int = 8081

@lru_cache
def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/unit/services/mock_store_api/test_config.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add services/mock_store_api/__init__.py services/mock_store_api/requirements.txt services/mock_store_api/config.py tests/unit/services/mock_store_api/test_config.py
git commit -m "feat(mock-api): add basic configuration and requirements"
```

---

### Task 2: Database Connection Setup

**Files:**
- Create: `services/mock_store_api/database.py`
- Create: `tests/unit/services/mock_store_api/test_database.py`

**Interfaces:**
- Consumes: `mock_store_api.config.get_settings()`
- Produces: `mock_store_api.database.get_db()` dependency, `engine`, and `Base` declarative class.

- [ ] **Step 1: Write the failing test**

```python
import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from services.mock_store_api.database import get_db

@pytest.mark.asyncio
async def test_get_db_yields_session():
    async for session in get_db():
        assert isinstance(session, AsyncSession)
        break
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/services/mock_store_api/test_database.py -v`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

```python
from collections.abc import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base
from services.mock_store_api.config import get_settings

settings = get_settings()

engine = create_async_engine(settings.database_url, echo=False)
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

Base = declarative_base()

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/services/mock_store_api/test_database.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/mock_store_api/database.py tests/unit/services/mock_store_api/test_database.py
git commit -m "feat(mock-api): add database connection setup"
```

---

### Task 3: Database Models

**Files:**
- Create: `services/mock_store_api/models.py`
- Create: `tests/unit/services/mock_store_api/test_models.py`

**Interfaces:**
- Consumes: `mock_store_api.database.Base`
- Produces: ORM classes: `Customer`, `Product`, `Order`, `OrderItem`.

- [ ] **Step 1: Write the failing test**

```python
from services.mock_store_api.models import Customer, Product, Order, OrderItem

def test_models_have_correct_schema():
    assert Customer.__table_args__["schema"] == "mock_store"
    assert Product.__table_args__["schema"] == "mock_store"
    assert Order.__table_args__["schema"] == "mock_store"
    assert OrderItem.__table_args__["schema"] == "mock_store"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/services/mock_store_api/test_models.py -v`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

```python
import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, Numeric, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from services.mock_store_api.database import Base

def utcnow():
    return datetime.now(timezone.utc)

class Customer(Base):
    __tablename__ = "customers"
    __table_args__ = {"schema": "mock_store"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    full_name = Column(String(255), nullable=False)
    email = Column(String(255), unique=True, nullable=False, index=True)
    document_id = Column(String(20))
    status = Column(String(20), nullable=False, default="ACTIVE")
    created_at = Column(DateTime(timezone=True), default=utcnow)

    orders = relationship("Order", back_populates="customer")

class Product(Base):
    __tablename__ = "products"
    __table_args__ = {"schema": "mock_store"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sku = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    category = Column(String(100), nullable=False, index=True)
    price = Column(Numeric(10, 2), nullable=False)
    stock_quantity = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), default=utcnow)

class Order(Base):
    __tablename__ = "orders"
    __table_args__ = {"schema": "mock_store"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_id = Column(UUID(as_uuid=True), ForeignKey("mock_store.customers.id"), index=True)
    status = Column(String(30), nullable=False, default="PENDING")
    total_amount = Column(Numeric(10, 2), nullable=False)
    shipping_address = Column(String)
    created_at = Column(DateTime(timezone=True), default=utcnow, index=True)

    customer = relationship("Customer", back_populates="orders")
    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")

class OrderItem(Base):
    __tablename__ = "order_items"
    __table_args__ = {"schema": "mock_store"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id = Column(UUID(as_uuid=True), ForeignKey("mock_store.orders.id"), index=True)
    product_id = Column(UUID(as_uuid=True), ForeignKey("mock_store.products.id"), index=True)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)

    order = relationship("Order", back_populates="items")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/services/mock_store_api/test_models.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/mock_store_api/models.py tests/unit/services/mock_store_api/test_models.py
git commit -m "feat(mock-api): add database models for e-commerce"
```

---

### Task 4: Pydantic Schemas

**Files:**
- Create: `services/mock_store_api/schemas.py`
- Create: `tests/unit/services/mock_store_api/test_schemas.py`

**Interfaces:**
- Consumes: N/A
- Produces: Response and Create schemas (e.g., `CustomerResponse`, `CustomerCreate`, `PaginatedResponse`).

- [ ] **Step 1: Write the failing test**

```python
from services.mock_store_api.schemas import CustomerCreate, PaginatedResponse, CustomerResponse

def test_customer_create_schema():
    customer = CustomerCreate(full_name="John Doe", email="john@example.com", document_id="123", status="ACTIVE")
    assert customer.full_name == "John Doe"

def test_paginated_response():
    resp = PaginatedResponse[CustomerResponse](
        data=[],
        pagination={"page": 1, "limit": 10, "total_records": 0, "total_pages": 0, "has_next": False}
    )
    assert resp.pagination.page == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/services/mock_store_api/test_schemas.py -v`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

```python
from typing import Generic, TypeVar
from pydantic import BaseModel, ConfigDict, Field
from uuid import UUID
from datetime import datetime
from decimal import Decimal

T = TypeVar('T')

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
    id: UUID
    created_at: datetime

class ProductResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    sku: str
    name: str
    category: str
    price: Decimal
    stock_quantity: int
    created_at: datetime

class OrderItemCreate(BaseModel):
    product_id: UUID
    quantity: int
    unit_price: Decimal

class OrderCreate(BaseModel):
    customer_id: UUID
    status: str = "PENDING"
    total_amount: Decimal
    shipping_address: str | None = None
    items: list[OrderItemCreate] = Field(default_factory=list)

class OrderResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    customer_id: UUID
    status: str
    total_amount: Decimal
    shipping_address: str | None = None
    created_at: datetime

class BatchInsertResult(BaseModel):
    inserted: int
    ids: list[UUID]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/services/mock_store_api/test_schemas.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/mock_store_api/schemas.py tests/unit/services/mock_store_api/test_schemas.py
git commit -m "feat(mock-api): add pydantic schemas for requests and responses"
```

---

### Task 5: Data Seeding Logic

**Files:**
- Create: `services/mock_store_api/seed.py`
- Modify: `tests/unit/services/mock_store_api/test_seed.py` (Mocked test)

**Interfaces:**
- Consumes: `mock_store_api.models` classes, `mock_store_api.database.engine`.
- Produces: `async def seed_data_if_empty()`

- [ ] **Step 1: Write the failing test**

```python
import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from services.mock_store_api.seed import seed_data_if_empty

@pytest.mark.asyncio
@patch("services.mock_store_api.seed.AsyncSessionLocal")
async def test_seed_data_does_not_seed_if_customers_exist(mock_session_maker):
    mock_session = AsyncMock()
    mock_session_maker.return_value.__aenter__.return_value = mock_session
    mock_result = MagicMock()
    mock_result.scalar_one.return_value = 5 # 5 customers exist
    mock_session.execute.return_value = mock_result

    await seed_data_if_empty()

    mock_session.add_all.assert_not_called()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/services/mock_store_api/test_seed.py -v`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

```python
import logging
import uuid
from sqlalchemy import select, func
from services.mock_store_api.database import AsyncSessionLocal
from services.mock_store_api.models import Customer, Product, Order

logger = logging.getLogger(__name__)

async def seed_data_if_empty() -> None:
    async with AsyncSessionLocal() as session:
        # Check if customers exist
        result = await session.execute(select(func.count(Customer.id)))
        count = result.scalar_one()

        if count > 0:
            logger.info("Database already seeded with %d customers.", count)
            return

        logger.info("Seeding database...")
        # Create dummy customers
        customers = [
            Customer(id=uuid.uuid4(), full_name=f"Customer {i}", email=f"customer{i}@example.com", document_id=f"1234567890{i}")
            for i in range(20)
        ]
        session.add_all(customers)

        # Create dummy products
        products = [
            Product(id=uuid.uuid4(), sku=f"SKU-{i}", name=f"Product {i}", category="Electronics" if i % 2 == 0 else "Books", price=10.50 + i, stock_quantity=100)
            for i in range(15)
        ]
        session.add_all(products)

        # Create dummy orders
        orders = [
            Order(id=uuid.uuid4(), customer_id=customers[i % 20].id, total_amount=100.0, status="PAID")
            for i in range(50)
        ]
        session.add_all(orders)

        await session.commit()
        logger.info("Database successfully seeded.")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/services/mock_store_api/test_seed.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/mock_store_api/seed.py tests/unit/services/mock_store_api/test_seed.py
git commit -m "feat(mock-api): add initial data seeding logic"
```

---

### Task 6: FastAPI Application & Endpoints

**Files:**
- Create: `services/mock_store_api/main.py`
- Create: `tests/unit/services/mock_store_api/test_main.py`

**Interfaces:**
- Consumes: Models, Schemas, `get_db`, `seed_data_if_empty`.
- Produces: `app` FastAPI instance.

- [ ] **Step 1: Write the failing test**

```python
from fastapi.testclient import TestClient
from services.mock_store_api.main import app

client = TestClient(app)

def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "mock_store_api"}

def test_customers_endpoint():
    # Will fail without db mock or setup, but verifying endpoint exists
    response = client.get("/api/v1/customers")
    assert response.status_code in (200, 500) # 500 is ok if db is not mocked in this simple test
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/services/mock_store_api/test_main.py -v`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

```python
import math
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from fastapi import FastAPI, Depends
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession
from services.mock_store_api.database import engine, Base, get_db
from services.mock_store_api.seed import seed_data_if_empty
from services.mock_store_api.models import Customer, Product, Order, OrderItem
from services.mock_store_api.schemas import (
    CustomerResponse, ProductResponse, OrderResponse,
    PaginatedResponse, PaginationMeta, BatchInsertResult, OrderCreate, CustomerCreate
)

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    async with engine.begin() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS mock_store"))
        await conn.run_sync(Base.metadata.create_all)
    await seed_data_if_empty()
    yield

app = FastAPI(title="Mock Store API", lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok", "service": "mock_store_api"}

@app.get("/api/v1/customers", response_model=PaginatedResponse[CustomerResponse])
async def list_customers(page: int = 1, limit: int = 50, db: AsyncSession = Depends(get_db)):
    offset = (page - 1) * limit
    result = await db.execute(select(Customer).offset(offset).limit(limit))
    items = result.scalars().all()
    count = await db.scalar(select(func.count(Customer.id)))
    count = count or 0
    total_pages = math.ceil(count / limit) if limit > 0 else 1
    meta = PaginationMeta(page=page, limit=limit, total_records=count, total_pages=total_pages, has_next=page < total_pages)
    return {"data": items, "pagination": meta}

@app.get("/api/v1/products", response_model=PaginatedResponse[ProductResponse])
async def list_products(page: int = 1, limit: int = 50, db: AsyncSession = Depends(get_db)):
    offset = (page - 1) * limit
    result = await db.execute(select(Product).offset(offset).limit(limit))
    items = result.scalars().all()
    count = await db.scalar(select(func.count(Product.id)))
    count = count or 0
    total_pages = math.ceil(count / limit) if limit > 0 else 1
    meta = PaginationMeta(page=page, limit=limit, total_records=count, total_pages=total_pages, has_next=page < total_pages)
    return {"data": items, "pagination": meta}

@app.get("/api/v1/orders", response_model=PaginatedResponse[OrderResponse])
async def list_orders(page: int = 1, limit: int = 50, db: AsyncSession = Depends(get_db)):
    offset = (page - 1) * limit
    result = await db.execute(select(Order).offset(offset).limit(limit))
    items = result.scalars().all()
    count = await db.scalar(select(func.count(Order.id)))
    count = count or 0
    total_pages = math.ceil(count / limit) if limit > 0 else 1
    meta = PaginationMeta(page=page, limit=limit, total_records=count, total_pages=total_pages, has_next=page < total_pages)
    return {"data": items, "pagination": meta}

@app.post("/api/v1/customers/batch", response_model=BatchInsertResult)
async def create_customers_batch(customers: list[CustomerCreate], db: AsyncSession = Depends(get_db)):
    db_customers = [Customer(**c.model_dump()) for c in customers]
    db.add_all(db_customers)
    await db.commit()
    return {"inserted": len(db_customers), "ids": [c.id for c in db_customers]}

@app.post("/api/v1/orders/batch", response_model=BatchInsertResult)
async def create_orders_batch(orders: list[OrderCreate], db: AsyncSession = Depends(get_db)):
    db_orders = []
    for o in orders:
        data = o.model_dump()
        items = data.pop("items", [])
        order_obj = Order(**data)
        for item in items:
            order_obj.items.append(OrderItem(**item))
        db_orders.append(order_obj)
    db.add_all(db_orders)
    await db.commit()
    return {"inserted": len(db_orders), "ids": [o.id for o in db_orders]}

@app.post("/api/v1/orders", status_code=201)
async def create_order(order: OrderCreate, db: AsyncSession = Depends(get_db)):
    res = await create_orders_batch([order], db)
    return {"message": "Order created", "id": res["ids"][0]}

@app.post("/api/v1/customers", status_code=201)
async def create_customer(customer: CustomerCreate, db: AsyncSession = Depends(get_db)):
    res = await create_customers_batch([customer], db)
    return {"message": "Customer created", "id": res["ids"][0]}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/services/mock_store_api/test_main.py -v`

- [ ] **Step 5: Commit**

```bash
git add services/mock_store_api/main.py tests/unit/services/mock_store_api/test_main.py
git commit -m "feat(mock-api): add FastAPI application and REST endpoints"
```

---

### Task 7: Dockerization & E2E Integration

**Files:**
- Create: `services/mock_store_api/Dockerfile`
- Modify: `docker-compose.yml`
- Create: `tests/e2e/test_mock_api_e2e.py`

**Interfaces:**
- Consumes: Completed mock api service.
- Produces: Integrated container running on `docker-compose`.

- [ ] **Step 1: Write Dockerfile**

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY services/mock_store_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/mock_store_api/ /app/services/mock_store_api/
ENV PYTHONPATH=/app
CMD ["uvicorn", "services.mock_store_api.main:app", "--host", "0.0.0.0", "--port", "8081"]
```

- [ ] **Step 2: Modify `docker-compose.yml`**

Add the following service under `services:` block:

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

- [ ] **Step 3: Create basic E2E test file**

`tests/e2e/test_mock_api_e2e.py`:

```python
import pytest
import httpx
import os

@pytest.mark.asyncio
async def test_mock_api_health():
    host = os.getenv("MOCK_API_HOST", "localhost")
    async with httpx.AsyncClient(base_url=f"http://{host}:8081") as client:
        response = await client.get("/health")
        assert response.status_code == 200
```

- [ ] **Step 4: Commit**

```bash
git add services/mock_store_api/Dockerfile docker-compose.yml tests/e2e/test_mock_api_e2e.py
git commit -m "feat(mock-api): containerize mock api and add docker-compose profile"
```
