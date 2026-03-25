from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr


@pytest.fixture()
def sample_dataset() -> xr.Dataset:
    times = pd.date_range("2026-03-25 00:00", periods=12, freq="h")
    latitudes = np.array([53.25, 53.35, 53.45])
    longitudes = np.array([-6.35, -6.25, -6.15])

    hour_effect = np.array([18, 12, 6, 4, 5, 8, 12, 18, 24, 28, 30, 26], dtype=float)[:, None, None]
    lat_effect = np.array([-1.5, 0.0, 2.0], dtype=float)[None, :, None]
    lon_effect = np.array([2.0, 0.0, -1.0], dtype=float)[None, None, :]

    pm25 = hour_effect + lat_effect + lon_effect
    no2 = 38 + hour_effect * 1.4 + lat_effect * 2.5 - lon_effect
    ozone = 62 + hour_effect * 0.8 - lat_effect * 1.2 + lon_effect * 0.3
    aqi = np.clip(pm25 * 2.2, 0, None)

    return xr.Dataset(
        data_vars={
            "pm2_5": (("time", "lat", "lon"), pm25, {"units": "µg/m³"}),
            "nitrogen_dioxide": (("time", "lat", "lon"), no2, {"units": "µg/m³"}),
            "ozone": (("time", "lat", "lon"), ozone, {"units": "µg/m³"}),
            "european_aqi": (("time", "lat", "lon"), aqi, {"units": "index"}),
        },
        coords={"time": times, "lat": latitudes, "lon": longitudes},
        attrs={
            "title": "Synthetic test cube shaped like the AtmosLens sample dataset",
            "source": "tests",
            "region_name": "Dublin commuter belt",
        },
    )

