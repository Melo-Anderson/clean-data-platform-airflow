from services.mock_store_api.models import Customer, Order, OrderItem, Product


def _get_schema(model) -> str:
    args = model.__table_args__
    if isinstance(args, dict):
        return args.get("schema", "")
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
