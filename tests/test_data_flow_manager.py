import pytest
from pipelines.data_flow_manager import (
    TransformationType, JoinType, SinkType, SourceType,
    DataFlowTransformation, DataFlowPipeline, DataPlatformOrchestrator
)


# ── DataFlowTransformation tests ─────────────────────────────────────────────

def test_transform_is_filter():
    t = DataFlowTransformation("t1", "filter-nulls", TransformationType.FILTER)
    assert t.is_filter()
    assert not t.is_join()

def test_transform_is_join():
    t = DataFlowTransformation("t2", "customer-join", TransformationType.JOIN, JoinType.INNER)
    assert t.is_join()
    assert not t.is_filter()

def test_transform_is_aggregate():
    t = DataFlowTransformation("t3", "sum-revenue", TransformationType.AGGREGATE)
    assert t.is_aggregate()

def test_transform_is_window():
    t = DataFlowTransformation("t4", "row-number", TransformationType.WINDOW)
    assert t.is_window()

def test_transform_is_complex_join():
    t = DataFlowTransformation("t5", "join", TransformationType.JOIN)
    assert t.is_complex()

def test_transform_is_complex_window():
    t = DataFlowTransformation("t6", "window", TransformationType.WINDOW)
    assert t.is_complex()

def test_transform_is_complex_rank():
    t = DataFlowTransformation("t7", "rank", TransformationType.RANK)
    assert t.is_complex()

def test_transform_not_complex_filter():
    t = DataFlowTransformation("t8", "filter", TransformationType.FILTER)
    assert not t.is_complex()

def test_transform_not_complex_select():
    t = DataFlowTransformation("t9", "select", TransformationType.SELECT)
    assert not t.is_complex()

def test_transform_not_complex_sort():
    t = DataFlowTransformation("t10", "sort", TransformationType.SORT)
    assert not t.is_complex()

def test_transform_join_type_set():
    t = DataFlowTransformation("t11", "left-join", TransformationType.JOIN, JoinType.LEFT_OUTER)
    assert t.join_type == JoinType.LEFT_OUTER

def test_transform_join_type_none_by_default():
    t = DataFlowTransformation("t12", "filter", TransformationType.FILTER)
    assert t.join_type is None


# ── DataFlowPipeline tests ───────────────────────────────────────────────────

def make_pipeline(pid="pl1"):
    return DataFlowPipeline(pid, "ingestion-pipeline", SourceType.ADLS, SinkType.SYNAPSE_POOL)

def test_pipeline_add_and_get_transform():
    pl = make_pipeline()
    t = DataFlowTransformation("t1", "filter", TransformationType.FILTER)
    pl.add_transformation(t)
    assert pl.get_transformation("t1") is t

def test_pipeline_duplicate_transform_raises():
    pl = make_pipeline()
    t = DataFlowTransformation("t1", "filter", TransformationType.FILTER)
    pl.add_transformation(t)
    with pytest.raises(ValueError):
        pl.add_transformation(t)

def test_pipeline_get_nonexistent_transform():
    pl = make_pipeline()
    assert pl.get_transformation("nope") is None

def test_pipeline_transform_count():
    pl = make_pipeline()
    pl.add_transformation(DataFlowTransformation("t1", "f", TransformationType.FILTER))
    pl.add_transformation(DataFlowTransformation("t2", "j", TransformationType.JOIN))
    assert pl.transform_count == 2

def test_pipeline_complex_transforms():
    pl = make_pipeline()
    pl.add_transformation(DataFlowTransformation("t1", "join", TransformationType.JOIN))
    pl.add_transformation(DataFlowTransformation("t2", "filter", TransformationType.FILTER))
    pl.add_transformation(DataFlowTransformation("t3", "window", TransformationType.WINDOW))
    assert len(pl.complex_transforms) == 2

def test_pipeline_join_transforms():
    pl = make_pipeline()
    pl.add_transformation(DataFlowTransformation("t1", "j1", TransformationType.JOIN, JoinType.INNER))
    pl.add_transformation(DataFlowTransformation("t2", "j2", TransformationType.JOIN, JoinType.LEFT_OUTER))
    pl.add_transformation(DataFlowTransformation("t3", "f", TransformationType.FILTER))
    assert len(pl.join_transforms) == 2

def test_pipeline_has_window_functions_true():
    pl = make_pipeline()
    pl.add_transformation(DataFlowTransformation("t1", "win", TransformationType.WINDOW))
    assert pl.has_window_functions()

def test_pipeline_has_window_functions_false():
    pl = make_pipeline()
    pl.add_transformation(DataFlowTransformation("t1", "filter", TransformationType.FILTER))
    assert not pl.has_window_functions()

def test_pipeline_is_adls_to_synapse():
    pl = DataFlowPipeline("pl1", "pipe", SourceType.ADLS, SinkType.SYNAPSE_POOL)
    assert pl.is_adls_to_synapse()

def test_pipeline_not_adls_to_synapse():
    pl = DataFlowPipeline("pl1", "pipe", SourceType.AZURE_SQL, SinkType.SYNAPSE_POOL)
    assert not pl.is_adls_to_synapse()

def test_pipeline_is_sql_to_adls():
    pl = DataFlowPipeline("pl1", "pipe", SourceType.AZURE_SQL, SinkType.ADLS)
    assert pl.is_sql_to_adls()

def test_pipeline_not_sql_to_adls():
    pl = DataFlowPipeline("pl1", "pipe", SourceType.ADLS, SinkType.ADLS)
    assert not pl.is_sql_to_adls()

def test_pipeline_transformations_property():
    pl = make_pipeline()
    pl.add_transformation(DataFlowTransformation("t1", "f", TransformationType.FILTER))
    assert len(pl.transformations) == 1


# ── DataPlatformOrchestrator tests ──────────────────────────────────────────

def make_orchestrator():
    return DataPlatformOrchestrator("orch1")

def test_orchestrator_register_and_get_pipeline():
    orch = make_orchestrator()
    pl = DataFlowPipeline("pl1", "pipe", SourceType.ADLS, SinkType.SYNAPSE_POOL)
    orch.register_pipeline(pl)
    assert orch.get_pipeline("pl1") is pl

def test_orchestrator_duplicate_pipeline_raises():
    orch = make_orchestrator()
    pl = DataFlowPipeline("pl1", "pipe", SourceType.ADLS, SinkType.SYNAPSE_POOL)
    orch.register_pipeline(pl)
    with pytest.raises(ValueError):
        orch.register_pipeline(pl)

def test_orchestrator_get_nonexistent_pipeline():
    orch = make_orchestrator()
    assert orch.get_pipeline("nope") is None

def test_orchestrator_adls_to_synapse_pipelines():
    orch = make_orchestrator()
    pl1 = DataFlowPipeline("pl1", "p1", SourceType.ADLS, SinkType.SYNAPSE_POOL)
    pl2 = DataFlowPipeline("pl2", "p2", SourceType.AZURE_SQL, SinkType.ADLS)
    orch.register_pipeline(pl1)
    orch.register_pipeline(pl2)
    assert len(orch.adls_to_synapse_pipelines) == 1

def test_orchestrator_get_by_sink():
    orch = make_orchestrator()
    pl1 = DataFlowPipeline("pl1", "p1", SourceType.ADLS, SinkType.BLOB)
    pl2 = DataFlowPipeline("pl2", "p2", SourceType.ADLS, SinkType.BLOB)
    pl3 = DataFlowPipeline("pl3", "p3", SourceType.ADLS, SinkType.SYNAPSE_POOL)
    orch.register_pipeline(pl1)
    orch.register_pipeline(pl2)
    orch.register_pipeline(pl3)
    assert len(orch.get_by_sink(SinkType.BLOB)) == 2

def test_orchestrator_summary_keys():
    orch = make_orchestrator()
    s = orch.summary()
    assert "platform_id" in s
    assert "total_pipelines" in s
    assert "adls_to_synapse" in s

def test_orchestrator_summary_counts():
    orch = make_orchestrator()
    pl = DataFlowPipeline("pl1", "p", SourceType.ADLS, SinkType.SYNAPSE_POOL)
    orch.register_pipeline(pl)
    s = orch.summary()
    assert s["total_pipelines"] == 1
    assert s["adls_to_synapse"] == 1

def test_orchestrator_pipelines_property():
    orch = make_orchestrator()
    pl1 = DataFlowPipeline("pl1", "p1", SourceType.ADLS, SinkType.BLOB)
    pl2 = DataFlowPipeline("pl2", "p2", SourceType.BLOB, SinkType.COSMOS)
    orch.register_pipeline(pl1)
    orch.register_pipeline(pl2)
    assert len(orch.pipelines) == 2
