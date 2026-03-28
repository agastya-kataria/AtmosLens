"""SQL bridge for xarray data via DuckDB.

This module demonstrates SQL-like querying on xarray datasets, bridging
N-dimensional labeled arrays with the SQL query language that Lumen's AI
planner already understands.

**GSoC 2026 context**
The HoloViz GSoC 2026 "Lumen + Xarray Integration" spec calls for:

    "Integration with xarray-sql or similar mechanisms will allow SQL-like
    filtering and aggregation while preserving xarray semantics."

``query_xarray_sql`` uses DuckDB as that "similar mechanism": it registers
a coordinate-preserving DataFrame derived from an xarray slice as a DuckDB
in-memory relation and executes arbitrary SQL against it.  The coordinate
columns (time, lat, lon) are always present, so spatial and temporal
predicates in SQL map naturally back to xarray coordinate operations.

Usage example
-------------
>>> from atmoslens.sql_bridge import query_xarray_sql, example_sql_query
>>> import xarray as xr
>>> ds = xr.open_dataset("data/sample_forecast.nc")
>>> sql = example_sql_query("pm2_5", ds)
>>> df = query_xarray_sql(ds, sql, variable="pm2_5")
>>> print(df.head())
"""

from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING

import duckdb
import pandas as pd

if TYPE_CHECKING:
    import xarray as xr


def query_xarray_sql(
    dataset: xr.Dataset,
    sql: str,
    variable: str | None = None,
    pre_slice: dict | None = None,
) -> pd.DataFrame:
    """
    Execute SQL against an xarray Dataset via DuckDB.

    The dataset (or a single variable DataArray) is converted to a tidy
    DataFrame and registered as a DuckDB in-memory table named ``"forecast"``.
    All coordinate columns (``time``, ``lat``, ``lon``) are preserved, so SQL
    temporal and spatial predicates compose cleanly.

    Parameters
    ----------
    dataset : xr.Dataset
        Source dataset.  Must contain the requested ``variable``.
    sql : str
        SQL query.  The table name is always ``"forecast"``.
    variable : str | None
        Which data variable to expose.  If ``None``, uses the first variable.
    pre_slice : dict | None
        Optional coordinate pre-slice applied before registration (reduces
        memory for large datasets).  Keys: ``lat_min``, ``lat_max``,
        ``lon_min``, ``lon_max``, ``time_start``, ``time_end``.

    Returns
    -------
    pd.DataFrame
        Query result with coordinate columns preserved.

    Notes
    -----
    A fresh DuckDB connection is created per call (thread-safe, no shared
    state).  For large datasets consider using ``pre_slice`` to limit the
    registered DataFrame size before the SQL filter runs.
    """
    # --- Resolve variable ---
    if variable is None:
        variable = next(iter(dataset.data_vars))
    da = dataset[variable]

    # --- Optional pre-slice to keep the registered table small ---
    if pre_slice:
        lat_min = pre_slice.get("lat_min")
        lat_max = pre_slice.get("lat_max")
        if lat_min is not None and "lat" in da.dims:
            da = da.sel(lat=slice(float(lat_min), float(lat_max)))
        lon_min = pre_slice.get("lon_min")
        lon_max = pre_slice.get("lon_max")
        if lon_min is not None and "lon" in da.dims:
            da = da.sel(lon=slice(float(lon_min), float(lon_max)))
        time_start = pre_slice.get("time_start")
        time_end = pre_slice.get("time_end")
        if time_start is not None and "time" in da.dims:
            da = da.sel(time=slice(pd.Timestamp(time_start), pd.Timestamp(time_end) if time_end else None))

    # --- Flatten to tidy DataFrame (coordinate columns preserved) ---
    df = da.to_dataframe(name=variable).reset_index()

    # --- DuckDB in-memory query ---
    con = duckdb.connect()
    try:
        con.register("forecast", df)
        result = con.execute(sql).df()
    finally:
        con.close()

    return result


def example_sql_query(variable: str, dataset: xr.Dataset) -> str:
    """
    Build a representative SQL query for the Bridge UI display.

    Selects rows where the pollutant exceeds a meaningful threshold inside
    the dataset's lat/lon bounds, for the first 24 hours of the forecast.

    Parameters
    ----------
    variable : str
        Data variable name (e.g. ``"pm2_5"``).
    dataset : xr.Dataset
        Used to derive realistic bounds and a time anchor.
    """
    times = pd.DatetimeIndex(pd.to_datetime(dataset.time.values))
    t0 = times[0].isoformat()
    t24 = (times[0] + pd.Timedelta(hours=24)).isoformat()

    lat_vals = dataset.lat.values
    lon_vals = dataset.lon.values
    lat_mid = float(lat_vals.mean())
    lon_mid = float(lon_vals.mean())
    lat_half = float((lat_vals.max() - lat_vals.min()) / 4)
    lon_half = float((lon_vals.max() - lon_vals.min()) / 4)

    # Use ~60th-percentile of actual data as threshold (adaptive, always produces rows)
    import numpy as np
    vals = dataset[variable].values
    threshold = float(np.nanpercentile(vals, 60))

    return textwrap.dedent(f"""\
        SELECT time, lat, lon, {variable}
        FROM forecast
        WHERE time BETWEEN '{t0}' AND '{t24}'
          AND lat  BETWEEN {lat_mid - lat_half:.3f} AND {lat_mid + lat_half:.3f}
          AND lon  BETWEEN {lon_mid - lon_half:.3f} AND {lon_mid + lon_half:.3f}
          AND {variable} > {threshold:.1f}
        ORDER BY {variable} DESC
        LIMIT 20""")


def run_example_query(dataset: xr.Dataset, variable: str) -> tuple[str, pd.DataFrame]:
    """
    Build and execute the example SQL query for a given variable.

    Returns both the SQL string (for display) and the result DataFrame.
    Silently returns empty results if the query finds no matching rows.

    Parameters
    ----------
    dataset : xr.Dataset
        Source dataset.
    variable : str
        Data variable to query.

    Returns
    -------
    sql : str
        The SQL query string.
    result : pd.DataFrame
        Query result (may be empty).
    """
    times = pd.DatetimeIndex(pd.to_datetime(dataset.time.values))
    sql = example_sql_query(variable, dataset)
    pre_slice = {
        "time_start": times[0].isoformat(),
        "time_end": (times[0] + pd.Timedelta(hours=24)).isoformat(),
    }
    try:
        result = query_xarray_sql(dataset, sql, variable=variable, pre_slice=pre_slice)
    except Exception:
        result = pd.DataFrame()
    return sql, result
