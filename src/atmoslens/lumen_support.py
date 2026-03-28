"""Lumen pipeline builders for AtmosLens.

Two pipeline strategies are provided:

1. **AtmosXarraySource pipeline** (``build_xarray_pipeline``)
   Uses the new ``AtmosXarraySource`` — a real ``lumen.sources.Source``
   subclass that queries the raw xarray Dataset with coordinate-based
   operations.  This is the GSoC prototype approach: the xarray cube is
   the *primary* Lumen source, not a flattened intermediate.

2. **InMemorySource pipeline** (``build_activity_pipeline``, ``build_route_pipeline``)
   The original approach: pre-processed DataFrames loaded into
   ``InMemorySource``.  Kept for comparison and backwards compatibility.

The contrast between these two strategies is made visible in the Lumen
Bridge tab, making the upstream design gap explicit.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from lumen.pipeline import Pipeline
from lumen.sources import InMemorySource

from atmoslens.models import AnalysisRequest, AnalysisResult
from atmoslens.xarray_source import AtmosXarraySource

if TYPE_CHECKING:
    import xarray as xr


def _clean_frame(frame: pd.DataFrame) -> pd.DataFrame:
    cleaned = frame.copy()
    for column in cleaned.columns:
        if pd.api.types.is_datetime64_any_dtype(cleaned[column]):
            cleaned[column] = cleaned[column].dt.strftime("%Y-%m-%d %H:%M")
    return cleaned


# ------------------------------------------------------------------
# AtmosXarraySource pipeline (GSoC prototype)
# ------------------------------------------------------------------

def build_xarray_pipeline(dataset: xr.Dataset, request: AnalysisRequest) -> Pipeline:
    """
    Build a Lumen Pipeline backed by ``AtmosXarraySource``.

    This is the GSoC prototype pipeline: the xarray Dataset is the
    *primary* Lumen source.  The Pipeline queries it with coordinate-based
    parameters (lat, lon, time_start, time_end) rather than operating on
    pre-processed DataFrames.

    The ``"table"`` name is the pollutant variable (e.g. ``"pm2_5"``), and
    calling ``pipeline.data`` triggers an xarray ``.sel()`` / ``.interp()``
    chain, not a SQL row filter.

    Parameters
    ----------
    dataset : xr.Dataset
        The live xarray forecast cube.
    request : AnalysisRequest
        Current analysis parameters (location, time horizon, pollutant).
    """
    source = AtmosXarraySource(dataset=dataset)
    # Return the pipeline for the requested pollutant variable
    table = request.pollutant if request.pollutant in source.get_tables() else source.get_tables()[0]
    return Pipeline(source=source, table=table)


def xarray_pipeline_summary(dataset: xr.Dataset, request: AnalysisRequest) -> dict:
    """
    Return a summary dict for the XarraySource-backed pipeline.

    Includes source class, Lumen inheritance chain, coord ranges, and the
    query that would be issued for the current request.
    """
    source = AtmosXarraySource(dataset=dataset)
    table = request.pollutant if request.pollutant in source.get_tables() else source.get_tables()[0]
    times = pd.DatetimeIndex(pd.to_datetime(dataset.time.values))
    return {
        "source_class": "AtmosXarraySource",
        "lumen_base": "lumen.sources.Source",
        "table": table,
        "query": {
            "lat": request.location_lat,
            "lon": request.location_lon,
            "time_start": times[0].isoformat(),
            "time_end": times[min(request.time_horizon_hours, len(times) - 1)].isoformat(),
        },
        "coord_ranges": source.coord_ranges(),
        "tables_available": source.get_tables(),
    }


# ------------------------------------------------------------------
# InMemorySource pipelines (original approach — kept for comparison)
# ------------------------------------------------------------------

def build_activity_pipeline(result: AnalysisResult) -> Pipeline:
    """Build an InMemorySource-backed Lumen Pipeline from pre-processed records."""
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
    """Build an InMemorySource-backed Lumen Pipeline from route departure records."""
    departures = _clean_frame(pd.DataFrame(result.route_records))
    source = InMemorySource(tables={"departures": departures})
    return Pipeline(source=source, table="departures")


def pipeline_summary_spec(pipeline: Pipeline) -> dict:
    """Return a summary dict for any Lumen Pipeline (used for Bridge tab display)."""
    source = pipeline.source
    return {
        "source_type": type(source).__name__,
        "tables": list(source.tables) if hasattr(source, "tables") else [],
        "table": pipeline.table,
    }
