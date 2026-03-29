from enum import Enum
from dataclasses import dataclass, field
from typing import List, Dict, Optional


class TransformationType(Enum):
    SELECT = "select"
    FILTER = "filter"
    JOIN = "join"
    AGGREGATE = "aggregate"
    LOOKUP = "lookup"
    SORT = "sort"
    WINDOW = "window"
    RANK = "rank"
    CONDITIONAL_SPLIT = "conditional_split"
    UNION = "union"
    FLATTEN = "flatten"


class JoinType(Enum):
    INNER = "inner"
    LEFT_OUTER = "left_outer"
    RIGHT_OUTER = "right_outer"
    FULL_OUTER = "full_outer"
    CROSS = "cross"


class SinkType(Enum):
    ADLS = "adls"
    AZURE_SQL = "azure_sql"
    SYNAPSE_POOL = "synapse_pool"
    BLOB = "blob"
    COSMOS = "cosmos"
    EVENTHUB = "eventhub"


class SourceType(Enum):
    ADLS = "adls"
    AZURE_SQL = "azure_sql"
    BLOB = "blob"
    REST_API = "rest_api"
    COSMOS = "cosmos"


@dataclass
class DataFlowTransformation:
    transform_id: str
    transform_name: str
    transform_type: TransformationType
    join_type: Optional[JoinType] = None

    def is_filter(self) -> bool:
        return self.transform_type == TransformationType.FILTER

    def is_join(self) -> bool:
        return self.transform_type == TransformationType.JOIN

    def is_aggregate(self) -> bool:
        return self.transform_type == TransformationType.AGGREGATE

    def is_window(self) -> bool:
        return self.transform_type == TransformationType.WINDOW

    def is_complex(self) -> bool:
        return self.transform_type in (
            TransformationType.JOIN,
            TransformationType.WINDOW,
            TransformationType.RANK,
        )


@dataclass
class DataFlowPipeline:
    pipeline_id: str
    pipeline_name: str
    source_type: SourceType
    sink_type: SinkType
    _transformations: Dict[str, DataFlowTransformation] = field(default_factory=dict)

    def add_transformation(self, t: DataFlowTransformation) -> None:
        if t.transform_id in self._transformations:
            raise ValueError(f"Transformation {t.transform_id} already exists")
        self._transformations[t.transform_id] = t

    def get_transformation(self, transform_id: str) -> Optional[DataFlowTransformation]:
        return self._transformations.get(transform_id)

    @property
    def transformations(self) -> List[DataFlowTransformation]:
        return list(self._transformations.values())

    @property
    def complex_transforms(self) -> List[DataFlowTransformation]:
        return [t for t in self._transformations.values() if t.is_complex()]

    @property
    def join_transforms(self) -> List[DataFlowTransformation]:
        return [t for t in self._transformations.values() if t.is_join()]

    def has_window_functions(self) -> bool:
        return any(t.is_window() for t in self._transformations.values())

    @property
    def transform_count(self) -> int:
        return len(self._transformations)

    def is_adls_to_synapse(self) -> bool:
        return self.source_type == SourceType.ADLS and self.sink_type == SinkType.SYNAPSE_POOL

    def is_sql_to_adls(self) -> bool:
        return self.source_type == SourceType.AZURE_SQL and self.sink_type == SinkType.ADLS


@dataclass
class DataPlatformOrchestrator:
    platform_id: str
    _pipelines: Dict[str, DataFlowPipeline] = field(default_factory=dict)

    def register_pipeline(self, pipeline: DataFlowPipeline) -> None:
        if pipeline.pipeline_id in self._pipelines:
            raise ValueError(f"Pipeline {pipeline.pipeline_id} already registered")
        self._pipelines[pipeline.pipeline_id] = pipeline

    def get_pipeline(self, pipeline_id: str) -> Optional[DataFlowPipeline]:
        return self._pipelines.get(pipeline_id)

    @property
    def pipelines(self) -> List[DataFlowPipeline]:
        return list(self._pipelines.values())

    @property
    def adls_to_synapse_pipelines(self) -> List[DataFlowPipeline]:
        return [p for p in self._pipelines.values() if p.is_adls_to_synapse()]

    def get_by_sink(self, sink_type: SinkType) -> List[DataFlowPipeline]:
        return [p for p in self._pipelines.values() if p.sink_type == sink_type]

    def summary(self) -> dict:
        return {
            "platform_id": self.platform_id,
            "total_pipelines": len(self._pipelines),
            "adls_to_synapse": len(self.adls_to_synapse_pipelines),
            "pipelines_with_window": sum(
                1 for p in self._pipelines.values() if p.has_window_functions()
            ),
        }
