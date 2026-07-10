from __future__ import annotations

from app.domain.lineage.lineage_graph import LineageGraph
from app.domain.lineage.lineage_mapping import LineageMapping


def test_lineage_graph_trace_upstream() -> None:
    # src_table.id -> dw_table.id_hash -> final_table.id_hash
    m1 = LineageMapping(
        id="m1", pipeline_id="p1", source_object_id="src_table", destination_object_id="dw_table"
    )
    m1.add_mapping(
        source_column="id", destination_column="id_hash", transformation_expression="SHA256(id)"
    )

    m2 = LineageMapping(
        id="m2", pipeline_id="p2", source_object_id="dw_table", destination_object_id="final_table"
    )
    m2.add_mapping(source_column="id_hash", destination_column="id_hash")

    graph = LineageGraph()
    graph.build_from_mappings([m1, m2])

    upstream = graph.trace_upstream("final_table", "id_hash")
    assert len(upstream) == 2
    assert upstream[0].object_id == "dw_table"
    assert upstream[0].transformation == "SHA256(id)"
    assert upstream[1].object_id == "src_table"


def test_lineage_graph_trace_downstream() -> None:
    m1 = LineageMapping(
        id="m1", pipeline_id="p1", source_object_id="src_table", destination_object_id="dw_table"
    )
    m1.add_mapping("id", "id_hash", "SHA256(id)")

    graph = LineageGraph()
    graph.build_from_mappings([m1])

    downstream = graph.trace_downstream("src_table", "id")
    assert len(downstream) == 1
    assert downstream[0].object_id == "dw_table"
    assert downstream[0].column_name == "id_hash"
