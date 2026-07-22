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
