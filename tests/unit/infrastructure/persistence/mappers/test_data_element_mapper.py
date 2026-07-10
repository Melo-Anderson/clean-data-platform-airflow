from app.domain.objects.data_element import DataElement
from app.domain.objects.element_type import ElementType
from app.infrastructure.persistence.models.data_element_model import DataElementModel
from app.infrastructure.persistence.repositories.sql_data_object_repository import (
    _element_to_domain,
)


def test_element_to_domain_maps_all_fields() -> None:
    m = DataElementModel(
        id="e1",
        object_id="o1",
        name="customer_id",
        source_type="integer",
        destination_type="bigint",
        required=True,
        nullable=False,
        description="PK",
        policy_tag=None,
        auto_generated=False,
        is_computed=False,
    )
    el = _element_to_domain(m)
    assert el.id == "e1"
    assert el.source_type == ElementType.INTEGER
    assert el.destination_type == ElementType.BIGINT
    assert el.required is True
    assert el.nullable is False


def test_element_to_domain_handles_null_source_type() -> None:
    m = DataElementModel(
        id="e2",
        object_id="o1",
        name="computed_col",
        source_type=None,
        destination_type="string",
        required=False,
        nullable=True,
        description="",
        policy_tag=None,
        auto_generated=False,
        is_computed=True,
    )
    el = _element_to_domain(m)
    assert el.source_type is None
    assert el.is_computed is True
