"""AtmosXarraySource — prototype XarraySource for Lumen.

This module provides ``AtmosXarraySource``, a concrete ``lumen.sources.Source``
subclass that wraps an ``xarray.Dataset`` and answers queries using native
xarray coordinate operations instead of flattening the data upfront.

**GSoC 2026 context**
The HoloViz GSoC 2026 "Lumen + Xarray Integration" project (HIGH priority)
calls for a first-class ``XarraySource`` in Lumen that preserves xarray
semantics while fitting into the Lumen pipeline abstraction.  AtmosXarraySource
is that prototype: it extends the real ``lumen.sources.Source`` ABC, answers
coordinate-based queries, and makes the design gap explicit — showing both
what works today and where upstream Lumen needs to change to support
N-dimensional labeled data natively.

Design gap made visible
-----------------------
Lumen's ``Source.get()`` is expected to return a ``pd.DataFrame``.  For
truly N-dimensional data (time × lat × lon × variable) this means we must
either:

1. Flatten the xarray array to a tidy DataFrame after slicing (current
   approach — works, but loses xarray semantics for downstream consumers).
2. Change Lumen's pipeline abstraction to accept xarray objects natively
   (the upstream GSoC contribution).

``AtmosXarraySource`` demonstrates path 1 as a working prototype and
documents path 2 in its ``get_schema()`` return shape, which exposes
*coordinate ranges* rather than column-value ranges — a structural difference
that makes the need for upstream change obvious.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import param
import xarray as xr

import lumen.sources  # noqa: F401 — ensure Source is registered


class AtmosXarraySource(lumen.sources.Source):
    """
    Lumen ``Source`` that queries an ``xarray.Dataset`` via coordinate
    operations rather than tabular row filtering.

    **Tables** correspond to ``xarray`` data variables (e.g. ``"pm2_5"``,
    ``"nitrogen_dioxide"``).  Each call to :meth:`get` slices the underlying
    DataArray along its labeled coordinates (lat, lon, time) and returns a
    tidy ``pd.DataFrame`` with one row per grid cell in the result.

    Supported query keys
    --------------------
    lat_min, lat_max : float
        Bounding-box latitude slice (inclusive on both ends).
    lon_min, lon_max : float
        Bounding-box longitude slice.
    time_start, time_end : str | pd.Timestamp
        ISO-8601 strings or Timestamps for the time slice.
    lat, lon : float
        Single-point bilinear interpolation (overrides bbox if provided).
    limit : int
        Maximum rows returned (applied after coordinate slicing).

    Notes
    -----
    The ``__dask`` key (Lumen internal) is silently consumed and ignored;
    this prototype always returns a computed ``pd.DataFrame``.
    """

    source_type: str = "atmos_xarray"

    dataset: xr.Dataset = param.Parameter(
        doc="The xarray.Dataset to query.  Must have 'time', 'lat', 'lon' coords.",
    )

    # ------------------------------------------------------------------
    # Required Source interface
    # ------------------------------------------------------------------

    def get_tables(self) -> list[str]:
        """Return the list of queryable tables (xarray data variable names)."""
        if self.dataset is None:
            return []
        return list(self.dataset.data_vars)

    def get_schema(
        self,
        table: str | None = None,
        limit: int | None = None,
        shuffle: bool = False,
    ) -> dict[str, Any]:
        """
        Return an *xarray-aware* schema.

        Unlike tabular sources, the schema describes **coordinate ranges**
        (start/end/n for time; min/max/n for lat/lon) rather than column
        value distributions.  This structural difference is the core design
        point: a native Lumen XarraySource would expose this richer schema to
        the AI planner so it can generate coordinate-based queries, not
        row-predicate SQL.

        Parameters
        ----------
        table : str | None
            Variable name.  ``None`` returns schemas for all variables.
        """
        if self.dataset is None:
            return {} if table is None else {}
        if table is not None:
            return self._variable_schema(table)
        return {t: self._variable_schema(t) for t in self.get_tables()}

    def get(self, table: str, **query) -> pd.DataFrame:
        """
        Query the xarray Dataset using coordinate-based operations.

        This is the fundamental difference from a tabular Source: instead of
        filtering rows by column predicates, we slice the N-dimensional array
        along its labeled coordinate axes.  The result is then unwound into a
        tidy DataFrame for Lumen compatibility.

        Parameters
        ----------
        table : str
            The data variable to query (e.g. ``"pm2_5"``).
        **query
            Coordinate-based query parameters (see class docstring).
        """
        if self.dataset is None or table not in self.dataset.data_vars:
            return pd.DataFrame()

        # Lumen internals — consume silently
        query.pop("__dask", None)
        limit = query.pop("limit", None)

        da: xr.DataArray = self.dataset[table]

        # --- Spatial bounding box ---
        lat_min = query.get("lat_min")
        lat_max = query.get("lat_max")
        lon_min = query.get("lon_min")
        lon_max = query.get("lon_max")

        if lat_min is not None and "lat" in da.dims:
            da = da.sel(lat=slice(float(lat_min), float(lat_max)))
        if lon_min is not None and "lon" in da.dims:
            da = da.sel(lon=slice(float(lon_min), float(lon_max)))

        # --- Time slice ---
        time_start = query.get("time_start")
        time_end = query.get("time_end")
        if time_start is not None and "time" in da.dims:
            t0 = pd.Timestamp(time_start)
            t1 = pd.Timestamp(time_end) if time_end else None
            da = da.sel(time=slice(t0, t1))

        # --- Single-point interpolation (overrides bbox) ---
        lat_pt = query.get("lat")
        lon_pt = query.get("lon")
        if lat_pt is not None and lon_pt is not None and "lat" in da.dims and "lon" in da.dims:
            da = da.interp(lat=float(lat_pt), lon=float(lon_pt), method="linear")

        # Convert to tidy DataFrame preserving all coordinate columns
        df = da.to_dataframe(name=table).reset_index()

        if limit is not None:
            df = df.head(int(limit))

        return df

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _variable_schema(self, variable: str) -> dict[str, Any]:
        """Build xarray-aware schema for one data variable."""
        da = self.dataset[variable]
        coord_schemas: dict[str, Any] = {}
        for coord_name in da.dims:
            if coord_name not in self.dataset.coords:
                continue
            coord = self.dataset.coords[coord_name]
            vals = coord.values
            if coord_name == "time":
                coord_schemas[coord_name] = {
                    "type": "datetime",
                    "start": pd.Timestamp(vals[0]).isoformat(),
                    "end": pd.Timestamp(vals[-1]).isoformat(),
                    "n": int(len(vals)),
                }
            else:
                coord_schemas[coord_name] = {
                    "type": str(coord.dtype),
                    "min": float(vals.min()),
                    "max": float(vals.max()),
                    "n": int(len(vals)),
                }
        return {
            "dims": list(da.dims),
            "dtype": str(da.dtype),
            "attrs": dict(da.attrs),
            "coords": coord_schemas,
        }

    # ------------------------------------------------------------------
    # Convenience helpers used by the UI and lumen_support
    # ------------------------------------------------------------------

    def coord_ranges(self) -> dict[str, Any]:
        """Return coordinate ranges for the full dataset (all variables)."""
        if self.dataset is None:
            return {}
        ranges: dict[str, Any] = {}
        for coord_name, coord in self.dataset.coords.items():
            vals = coord.values
            if coord_name == "time":
                ranges[coord_name] = {
                    "start": pd.Timestamp(vals[0]).isoformat(),
                    "end": pd.Timestamp(vals[-1]).isoformat(),
                    "n": int(len(vals)),
                }
            elif vals.ndim == 1:
                ranges[coord_name] = {
                    "min": float(vals.min()),
                    "max": float(vals.max()),
                    "n": int(len(vals)),
                }
        return ranges

    def summary_dict(self) -> dict[str, Any]:
        """
        Return a summary suitable for display in the Lumen Bridge tab.

        Includes source class, Lumen inheritance, dataset dimensions,
        coordinate ranges, and available variables — the information a Lumen
        AI planner would need to generate xarray-native queries.
        """
        if self.dataset is None:
            return {"source_type": self.source_type, "dataset": None}
        return {
            "source_class": type(self).__name__,
            "lumen_base": "lumen.sources.Source",
            "source_type": self.source_type,
            "tables": self.get_tables(),
            "dims": dict(self.dataset.sizes),
            "coord_ranges": self.coord_ranges(),
            "design_gap": (
                "get_schema() exposes coordinate ranges (not column types). "
                "get() operates on labeled axes (not row predicates). "
                "Upstream Lumen needs to accept xarray objects natively to "
                "preserve N-dimensional structure across pipeline stages."
            ),
        }
