from __future__ import annotations

import math
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from services.mock_store_api.config import get_settings
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
    if get_settings().testing:
        yield
        return
    try:
        async with engine.begin() as conn:
            await conn.execute(text("CREATE SCHEMA IF NOT EXISTS mock_store"))
            await conn.run_sync(Base.metadata.create_all)
        await seed_data_if_empty()
    except Exception as exc:
        import logging

        logging.getLogger(__name__).warning("Skipping lifespan DB initialization: %s", exc)
    yield


app = FastAPI(title="Mock Store API", version="1.0.0", lifespan=lifespan)


def _build_pagination(page: int, limit: int, total: int) -> PaginationMeta:
    total_pages = math.ceil(total / limit) if limit > 0 else 1
    return PaginationMeta(
        page=page,
        limit=limit,
        total_records=total,
        total_pages=total_pages,
        has_next=page < total_pages,
    )


@app.get("/health", tags=["System"])
async def health() -> dict:
    return {"status": "ok", "service": "mock_store_api"}


@app.get(
    "/api/v1/customers", response_model=PaginatedResponse[CustomerResponse], tags=["Customers"]
)
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


@app.get("/api/v1/products", response_model=PaginatedResponse[ProductResponse], tags=["Products"])
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


@app.get("/api/v1/orders", response_model=PaginatedResponse[OrderResponse], tags=["Orders"])
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


@app.post("/api/v1/customers", status_code=201, tags=["Customers"])
async def create_customer(customer: CustomerCreate, db: AsyncSession = Depends(get_db)) -> dict:
    new_customer = Customer(id=uuid.uuid4(), **customer.model_dump())
    db.add(new_customer)
    await db.commit()
    return {"message": "Customer created", "id": str(new_customer.id)}


@app.post("/api/v1/customers/batch", response_model=BatchInsertResult, tags=["Customers"])
async def create_customers_batch(
    customers: list[CustomerCreate], db: AsyncSession = Depends(get_db)
) -> dict:
    db_customers = [Customer(id=uuid.uuid4(), **c.model_dump()) for c in customers]
    db.add_all(db_customers)
    await db.commit()
    return {"inserted": len(db_customers), "ids": [c.id for c in db_customers]}


@app.post("/api/v1/orders", status_code=201, tags=["Orders"])
async def create_order(order: OrderCreate, db: AsyncSession = Depends(get_db)) -> dict:
    data = order.model_dump()
    items_data = data.pop("items", [])
    new_order = Order(id=uuid.uuid4(), **data)
    for item in items_data:
        new_order.items.append(OrderItem(id=uuid.uuid4(), **item))
    db.add(new_order)
    await db.commit()
    return {"message": "Order created", "id": str(new_order.id)}


@app.post("/api/v1/orders/batch", response_model=BatchInsertResult, tags=["Orders"])
async def create_orders_batch(
    orders: list[OrderCreate], db: AsyncSession = Depends(get_db)
) -> dict:
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
