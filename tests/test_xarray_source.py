"""Tests for AtmosXarraySource and the DuckDB sql_bridge.

Verifies:
- AtmosXarraySource is a real lumen.sources.Source subclass
- get_tables(), get_schema(), get() return correct types and shapes
- Coordinate-based query filtering works (lat bbox, time slice, point interp)
- summary_dict() contains expected keys
- DuckDB SQL bridge executes and returns a DataFrame
- lumen_support.build_xarray_pipeline() returns a Lumen Pipeline
"""

from __future__ import annotations

import pandas as pd
import pytest

import lumen.sources

from atmoslens.lumen_support import build_xarray_pipeline, xarray_pipeline_summary
from atmoslens.models import AnalysisRequest
from atmoslens.sql_bridge import example_sql_query, query_xarray_sql, run_example_query
from atmoslens.xarray_source import AtmosXarraySource


# ---------------------------------------------------------------------------
# AtmosXarraySource — inheritance and registration
# ---------------------------------------------------------------------------

def test_is_lumen_source_subclass():
    assert issubclass(AtmosXarraySource, lumen.sources.Source)


def test_source_type():
    assert AtmosXarraySource.source_type == "atmos_xarray"


# ---------------------------------------------------------------------------
# AtmosXarraySource — get_tables
# ---------------------------------------------------------------------------

def test_get_tables_returns_variable_names(sample_dataset):
    src = AtmosXarraySource(dataset=sample_dataset)
    tables = src.get_tables()
    assert set(tables) == {"pm2_5", "nitrogen_dioxide", "ozone", "european_aqi"}


def test_get_tables_empty_on_none():
    src = AtmosXarraySource(dataset=None)
    assert src.get_tables() == []


# ---------------------------------------------------------------------------
# AtmosXarraySource — get_schema
# ---------------------------------------------------------------------------

def test_get_schema_single_variable(sample_dataset):
    src = AtmosXarraySource(dataset=sample_dataset)
    schema = src.get_schema("pm2_5")
    assert "dims" in schema
    assert "coords" in schema
    assert "lat" in schema["coords"]
    assert "lon" in schema["coords"]
    assert "time" in schema["coords"]


def test_get_schema_time_coord_has_start_end(sample_dataset):
    src = AtmosXarraySource(dataset=sample_dataset)
    schema = src.get_schema("pm2_5")
    time_schema = schema["coords"]["time"]
    assert "start" in time_schema
    assert "end" in time_schema
    assert "n" in time_schema
    assert time_schema["n"] == 12  # 12 time steps in fixture


def test_get_schema_spatial_coord_has_min_max(sample_dataset):
    src = AtmosXarraySource(dataset=sample_dataset)
    schema = src.get_schema("pm2_5")
    lat_schema = schema["coords"]["lat"]
    assert "min" in lat_schema
    assert "max" in lat_schema
    assert lat_schema["min"] == pytest.approx(53.25)
    assert lat_schema["max"] == pytest.approx(53.45)


def test_get_schema_all_tables(sample_dataset):
    src = AtmosXarraySource(dataset=sample_dataset)
    all_schemas = src.get_schema()
    assert set(all_schemas.keys()) == set(src.get_tables())


# ---------------------------------------------------------------------------
# AtmosXarraySource — get (coordinate-based queries)
# ---------------------------------------------------------------------------

def test_get_returns_dataframe(sample_dataset):
    src = AtmosXarraySource(dataset=sample_dataset)
    df = src.get("pm2_5")
    assert isinstance(df, pd.DataFrame)
    assert "pm2_5" in df.columns
    assert "lat" in df.columns
    assert "lon" in df.columns
    assert "time" in df.columns


def test_get_lat_bbox_filters_rows(sample_dataset):
    src = AtmosXarraySource(dataset=sample_dataset)
    full = src.get("pm2_5")
    filtered = src.get("pm2_5", lat_min=53.30, lat_max=53.50)
    # Only lat >= 53.30 should survive (53.35, 53.45)
    assert filtered["lat"].nunique() < full["lat"].nunique()
    assert all(filtered["lat"] >= 53.30)


def test_get_time_slice_filters_rows(sample_dataset):
    src = AtmosXarraySource(dataset=sample_dataset)
    full = src.get("pm2_5")
    filtered = src.get("pm2_5", time_start="2026-03-25 06:00", time_end="2026-03-25 11:00")
    assert len(filtered) < len(full)
    assert filtered["time"].nunique() <= 6


def test_get_limit_caps_rows(sample_dataset):
    src = AtmosXarraySource(dataset=sample_dataset)
    df = src.get("pm2_5", limit=5)
    assert len(df) == 5


def test_get_point_interpolation(sample_dataset):
    src = AtmosXarraySource(dataset=sample_dataset)
    df = src.get("pm2_5", lat=53.30, lon=-6.25)
    # Point interp removes lat/lon dims — result has one row per time step
    assert len(df) == 12
    assert "pm2_5" in df.columns


def test_get_unknown_table_returns_empty(sample_dataset):
    src = AtmosXarraySource(dataset=sample_dataset)
    df = src.get("nonexistent_variable")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_get_consumes_dask_kwarg(sample_dataset):
    """__dask kwarg must be silently consumed (Lumen internal)."""
    src = AtmosXarraySource(dataset=sample_dataset)
    df = src.get("pm2_5", __dask=False)
    assert isinstance(df, pd.DataFrame)


# ---------------------------------------------------------------------------
# AtmosXarraySource — summary_dict
# ---------------------------------------------------------------------------

def test_summary_dict_keys(sample_dataset):
    src = AtmosXarraySource(dataset=sample_dataset)
    summary = src.summary_dict()
    for key in ("source_class", "lumen_base", "source_type", "tables", "dims", "coord_ranges", "design_gap"):
        assert key in summary, f"Missing key: {key}"


def test_summary_dict_source_class(sample_dataset):
    src = AtmosXarraySource(dataset=sample_dataset)
    assert src.summary_dict()["source_class"] == "AtmosXarraySource"
    assert src.summary_dict()["lumen_base"] == "lumen.sources.Source"


# ---------------------------------------------------------------------------
# sql_bridge — DuckDB queries
# ---------------------------------------------------------------------------

def test_query_xarray_sql_returns_dataframe(sample_dataset):
    sql = "SELECT time, lat, lon, pm2_5 FROM forecast LIMIT 5"
    result = query_xarray_sql(sample_dataset, sql, variable="pm2_5")
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 5
    assert "pm2_5" in result.columns


def test_query_xarray_sql_where_clause(sample_dataset):
    sql = "SELECT time, lat, lon, pm2_5 FROM forecast WHERE pm2_5 > 20 ORDER BY pm2_5 DESC"
    result = query_xarray_sql(sample_dataset, sql, variable="pm2_5")
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert all(result["pm2_5"] > 20)


def test_example_sql_query_is_string(sample_dataset):
    sql = example_sql_query("pm2_5", sample_dataset)
    assert isinstance(sql, str)
    assert "forecast" in sql
    assert "pm2_5" in sql
    assert "WHERE" in sql


def test_run_example_query_returns_tuple(sample_dataset):
    sql, result = run_example_query(sample_dataset, "pm2_5")
    assert isinstance(sql, str)
    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# lumen_support — XarraySource pipeline integration
# ---------------------------------------------------------------------------

def _make_request():
    return AnalysisRequest(
        location_name="Dublin",
        location_lat=53.35,
        location_lon=-6.25,
        profile_name="general",
        activity_name="run",
        pollutant="pm2_5",
        advisor_mode="activity",
        time_horizon_hours=12,
    )


def test_build_xarray_pipeline_source_type(sample_dataset):
    from lumen.pipeline import Pipeline
    pipeline = build_xarray_pipeline(sample_dataset, _make_request())
    assert isinstance(pipeline, Pipeline)
    assert isinstance(pipeline.source, AtmosXarraySource)


def test_build_xarray_pipeline_table_is_pollutant(sample_dataset):
    pipeline = build_xarray_pipeline(sample_dataset, _make_request())
    assert pipeline.table == "pm2_5"


def test_build_xarray_pipeline_data_is_dataframe(sample_dataset):
    pipeline = build_xarray_pipeline(sample_dataset, _make_request())
    df = pipeline.data
    assert isinstance(df, pd.DataFrame)
    assert "pm2_5" in df.columns


def test_xarray_pipeline_summary_keys(sample_dataset):
    summary = xarray_pipeline_summary(sample_dataset, _make_request())
    for key in ("source_class", "lumen_base", "table", "query", "coord_ranges", "tables_available"):
        assert key in summary


def test_xarray_pipeline_summary_source_class(sample_dataset):
    summary = xarray_pipeline_summary(sample_dataset, _make_request())
    assert summary["source_class"] == "AtmosXarraySource"
