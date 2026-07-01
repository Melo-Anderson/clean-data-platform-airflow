from __future__ import annotations

import uuid

from app.domain.lineage.lineage_mapping import LineageMapping


def _mapping() -> LineageMapping:
    return LineageMapping(
        id=str(uuid.uuid4()),
        pipeline_id="pipe-1",
        source_object_id="src-obj-1",
        destination_object_id="dst-obj-1",
    )


def test_add_direct_mapping() -> None:
    m = _mapping()
    col = m.add_mapping(source_column="email", destination_column="email")
    assert col.transformation_expression is None
    assert len(m.direct_mappings()) == 1


def test_add_transformed_mapping() -> None:
    m = _mapping()
    m.add_mapping("cpf", "document_hash", transformation_expression="SHA256(cpf)")
    assert len(m.transformed_mappings()) == 1
    assert m.transformed_mappings()[0].source_column == "cpf"


def test_lineage_mapping_separates_direct_from_transformed() -> None:
    m = _mapping()
    m.add_mapping("name", "name")  # direct
    m.add_mapping("cpf", "doc_hash", "SHA256(cpf)")  # transformed
    m.add_mapping("birth_date", "age", "DATEDIFF(NOW(), birth_date) / 365")  # transformed
    assert len(m.direct_mappings()) == 1
    assert len(m.transformed_mappings()) == 2
