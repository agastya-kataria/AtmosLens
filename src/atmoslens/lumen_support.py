from __future__ import annotations

import pandas as pd
from lumen.pipeline import Pipeline
from lumen.sources import InMemorySource

from atmoslens.models import AnalysisResult


def _clean_frame(frame: pd.DataFrame) -> pd.DataFrame:
    cleaned = frame.copy()
    for column in cleaned.columns:
        if pd.api.types.is_datetime64_any_dtype(cleaned[column]):
            cleaned[column] = cleaned[column].dt.strftime("%Y-%m-%d %H:%M")
    return cleaned


def build_activity_pipeline(result: AnalysisResult) -> Pipeline:
    timeline = _clean_frame(pd.DataFrame(result.timeline_records))
    windows = _clean_frame(pd.DataFrame(result.window_records))
    source = InMemorySource(
        tables={
            "timeline": timeline,
            "windows": windows,
        }
    )
    return Pipeline(source=source, table="windows")


def build_route_pipeline(result: AnalysisResult) -> Pipeline:
    departures = _clean_frame(pd.DataFrame(result.route_records))
    source = InMemorySource(tables={"departures": departures})
    return Pipeline(source=source, table="departures")


def pipeline_summary_spec(pipeline: Pipeline) -> dict[str, object]:
    source = pipeline.source
    return {
        "source_type": type(source).__name__,
        "tables": list(source.tables),
        "table": pipeline.table,
    }
