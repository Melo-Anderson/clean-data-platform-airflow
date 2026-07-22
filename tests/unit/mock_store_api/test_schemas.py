import uuid
from datetime import UTC, datetime
from decimal import Decimal

from services.mock_store_api.schemas import (
    BatchInsertResult,
    CustomerCreate,
    CustomerResponse,
    OrderCreate,
    OrderItemCreate,
    PaginatedResponse,
    PaginationMeta,
)


def test_customer_create_schema():
    customer = CustomerCreate(
        full_name="John Doe", email="john@example.com", document_id="12345678901", status="ACTIVE"
    )
    assert customer.full_name == "John Doe"
    assert customer.status == "ACTIVE"


def test_paginated_response_with_customers():
    now = datetime.now(UTC)
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
