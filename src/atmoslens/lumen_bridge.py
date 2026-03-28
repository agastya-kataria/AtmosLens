"""Lumen–xarray bridge utilities.

This module provides two complementary bridge layers:

1. ``XarrayPipelineBridge`` — introspects an xarray Dataset and serialises
   the analysis as a query-spec-like structure.  This documents the *shape*
   of an upstream XarraySource query without requiring Lumen changes.

2. ``AtmosXarraySource`` (imported from ``atmoslens.xarray_source``) — a
   concrete ``lumen.sources.Source`` subclass that actually executes
   coordinate-based queries on the xarray Dataset.  This is the GSoC
   prototype: a real Lumen Source that operates natively on N-dimensional
   labeled data.

**GSoC 2026 alignment**
The HoloViz GSoC 2026 "Lumen + Xarray Integration" project first steps are:

    "Explore Lumen Source abstractions, prototype a minimal XarraySource,
    evaluate xarray-sql query translation approaches."

``AtmosXarraySource`` is that minimal prototype.  ``XarrayPipelineBridge``
documents the serialised query shape.  Together they form a vertical slice
that makes the upstream contribution concrete.
"""

from __future__ import annotations

from dataclasses import asdict

import pandas as pd
import xarray as xr

from atmoslens.models import AnalysisRequest, TransformStep
# Re-export so callers can import the Source prototype from this module too
from atmoslens.xarray_source import AtmosXarraySource  # noqa: F401


class XarrayPipelineBridge:
    """
    Introspect an xarray Dataset and serialise analysis state as a
    query-spec-like structure.

    This documents the *shape* of what a native Lumen ``XarraySource``
    would expose to an AI planner: dimension ranges, coordinate metadata,
    and the explicit transform steps that produced the result.

    For the actual Lumen ``Source`` subclass see ``AtmosXarraySource``.
    """

    def __init__(self, dataset: xr.Dataset) -> None:
        self.dataset = dataset

    def schema(self) -> dict[str, object]:
        """
        Return an xarray-aware schema for the dataset.

        Exposes coordinate ranges (start/end/n for time; min/max/n for
        spatial dims) rather than column-value distributions.  This
        structural difference is what motivates upstream Lumen changes:
        the AI planner needs to understand N-dimensional coordinate ranges
        to generate xarray-native queries.
        """
        coords: dict[str, dict[str, object]] = {}
        for name, coord in self.dataset.coords.items():
            values = coord.values
            if values.ndim != 1 or values.size == 0:
                continue
            first = values[0]
            last = values[-1]
            if name == "time":
                coords[name] = {
                    "dtype": str(coord.dtype),
                    "n": int(coord.size),
                    "start": pd.Timestamp(first).isoformat(),
                    "end": pd.Timestamp(last).isoformat(),
                }
            else:
                coords[name] = {
                    "dtype": str(coord.dtype),
                    "n": int(coord.size),
                    "min": _scalar(values.min()),
                    "max": _scalar(values.max()),
                }

        variables = {
            name: {
                "dims": list(variable.dims),
                "dtype": str(variable.dtype),
                "attrs": dict(variable.attrs),
            }
            for name, variable in self.dataset.data_vars.items()
        }
        return {
            "source_type": "xarray.Dataset",
            "lumen_prototype": "AtmosXarraySource(lumen.sources.Source)",
            "dims": {name: int(length) for name, length in self.dataset.sizes.items()},
            "coords": coords,
            "variables": variables,
            "attrs": dict(self.dataset.attrs),
        }

    def build_query_spec(self, request: AnalysisRequest, steps: tuple[TransformStep, ...]) -> dict[str, object]:
        """
        Serialise an analysis request as a query-spec-like structure.

        This is the shape ``AtmosXarraySource.get()`` would receive if Lumen
        generated coordinate-based queries natively.
        """
        return {
            "source": {
                "kind": "AtmosXarraySource",
                "lumen_base": "lumen.sources.Source",
                "variables": list(self.dataset.data_vars),
            },
            "request": asdict(request),
            "select": {
                "variable": request.pollutant,
                "coords": ["time", "lat", "lon"],
                "horizon_hours": request.time_horizon_hours,
            },
            "transforms": self.serialize_steps(steps),
        }

    @staticmethod
    def serialize_steps(steps: tuple[TransformStep, ...]) -> list[dict[str, object]]:
        return [step.to_dict() for step in steps]


def _scalar(value):
    if hasattr(value, "item"):
        try:
            return value.item()
        except ValueError:
            return value
    return value
