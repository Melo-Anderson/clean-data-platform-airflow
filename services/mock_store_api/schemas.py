from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class PaginationMeta(BaseModel):
    page: int
    limit: int
    total_records: int
    total_pages: int
    has_next: bool


class PaginatedResponse[T](BaseModel):
    data: list[T]
    pagination: PaginationMeta


class CustomerBase(BaseModel):
    full_name: str = Field(..., min_length=1)
    email: str = Field(..., min_length=3)
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
    quantity: int = Field(..., gt=0)
    unit_price: Decimal = Field(..., ge=0)


class OrderCreate(BaseModel):
    customer_id: uuid.UUID
    status: str = "PENDING"
    total_amount: Decimal = Field(..., ge=0)
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
