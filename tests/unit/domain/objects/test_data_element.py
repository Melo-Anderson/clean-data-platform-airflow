from __future__ import annotations

from app.domain.objects.data_element import DataElement
from app.domain.objects.element_type import ElementType


def test_data_element_is_primary_key_default():
    el = DataElement(
        id="el-1",
        object_id="obj-1",
        name="email",
        source_type=ElementType.STRING,
        destination_type=ElementType.STRING,
    )
    assert el.is_primary_key is False


def test_data_element_is_primary_key_true():
    el = DataElement(
        id="el-2",
        object_id="obj-1",
        name="id",
        source_type=ElementType.INTEGER,
        destination_type=ElementType.INTEGER,
        is_primary_key=True,
    )
    assert el.is_primary_key is True
