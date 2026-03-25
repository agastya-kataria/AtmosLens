from __future__ import annotations

from dataclasses import asdict

import pandas as pd
import xarray as xr

from atmoslens.models import AnalysisRequest, TransformStep


class XarrayPipelineBridge:
    """Small bridge that makes the xarray workflow explicit in a Lumen-like shape."""

    def __init__(self, dataset: xr.Dataset) -> None:
        self.dataset = dataset

    def schema(self) -> dict[str, object]:
        coords: dict[str, dict[str, object]] = {}
        for name, coord in self.dataset.coords.items():
            values = coord.values
            if values.size:
                first = values[0]
                last = values[-1]
            else:
                first = last = None
            coords[name] = {
                "dtype": str(coord.dtype),
                "size": int(coord.size),
                "first": pd.Timestamp(first).isoformat() if name == "time" and first is not None else _scalar(first),
                "last": pd.Timestamp(last).isoformat() if name == "time" and last is not None else _scalar(last),
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
            "dims": {name: int(length) for name, length in self.dataset.sizes.items()},
            "coords": coords,
            "variables": variables,
            "attrs": dict(self.dataset.attrs),
        }

    def build_query_spec(self, request: AnalysisRequest, steps: tuple[TransformStep, ...]) -> dict[str, object]:
        return {
            "source": {"kind": "xarray", "variables": list(self.dataset.data_vars)},
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
