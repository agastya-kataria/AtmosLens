from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode
from urllib.request import urlopen

import numpy as np
import pandas as pd
import xarray as xr

from atmoslens.models import RouteDefinition

OPEN_METEO_AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
SUPPORTED_POLLUTANTS = ("pm2_5", "nitrogen_dioxide", "ozone", "european_aqi")
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_PATH = REPO_ROOT / "data" / "sample_forecast.nc"


@dataclass(frozen=True)
class RegionConfig:
    name: str
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float
    n_lat: int = 9
    n_lon: int = 11
    forecast_hours: int = 48
    timezone: str = "Europe/Dublin"
    domains: str = "cams_europe"


DEFAULT_REGION = RegionConfig(
    name="Dublin commuter belt",
    lat_min=53.18,
    lat_max=53.43,
    lon_min=-6.42,
    lon_max=-6.03,
)

DEFAULT_LOCATIONS: dict[str, tuple[float, float]] = {
    "Docklands": (53.3478, -6.2374),
    "Phoenix Park": (53.3561, -6.3298),
    "Sandyford": (53.2749, -6.2256),
    "Tallaght": (53.2858, -6.3730),
    "Howth": (53.3871, -6.0653),
    "Bray": (53.2028, -6.1093),
}

DEFAULT_ROUTES: dict[str, RouteDefinition] = {
    "Sandyford to Docklands": RouteDefinition(
        name="Sandyford to Docklands",
        points=(
            (53.2749, -6.2256),
            (53.2892, -6.2384),
            (53.3152, -6.2522),
            (53.3478, -6.2374),
        ),
        duration_minutes=40,
        description="South suburbs into the city core; useful for commute exposure testing.",
    ),
    "Tallaght to City Centre": RouteDefinition(
        name="Tallaght to City Centre",
        points=(
            (53.2858, -6.3730),
            (53.2999, -6.3322),
            (53.3208, -6.2919),
            (53.3498, -6.2603),
        ),
        duration_minutes=45,
        description="Western approach into the city with a broad transect across the grid.",
    ),
    "Howth to Docklands": RouteDefinition(
        name="Howth to Docklands",
        points=(
            (53.3871, -6.0653),
            (53.3762, -6.1128),
            (53.3655, -6.1715),
            (53.3478, -6.2374),
        ),
        duration_minutes=35,
        description="Coastal inbound route that often benefits from cleaner early-hour flow.",
    ),
}


def build_grid(config: RegionConfig = DEFAULT_REGION) -> tuple[np.ndarray, np.ndarray, list[tuple[float, float]]]:
    latitudes = np.linspace(config.lat_min, config.lat_max, config.n_lat).round(4)
    longitudes = np.linspace(config.lon_min, config.lon_max, config.n_lon).round(4)
    points = [(float(lat), float(lon)) for lat in latitudes for lon in longitudes]
    return latitudes, longitudes, points


def _dataset_path(path: str | Path | None = None) -> Path:
    return Path(path).expanduser().resolve() if path else DEFAULT_DATA_PATH


def _normalise_dataset(ds: xr.Dataset) -> xr.Dataset:
    rename_map = {}
    if "latitude" in ds.coords and "lat" not in ds.coords:
        rename_map["latitude"] = "lat"
    if "longitude" in ds.coords and "lon" not in ds.coords:
        rename_map["longitude"] = "lon"
    if rename_map:
        ds = ds.rename(rename_map)
    ds = ds.sortby("time")
    return ds


def validate_dataset(ds: xr.Dataset) -> xr.Dataset:
    ds = _normalise_dataset(ds)
    required_dims = {"time", "lat", "lon"}
    missing_dims = required_dims.difference(ds.dims)
    if missing_dims:
        raise ValueError(f"Dataset missing expected dimensions: {sorted(missing_dims)}")

    pollutants = available_pollutants(ds)
    if not pollutants:
        raise ValueError("Dataset does not expose any supported pollutant variables.")
    return ds


def available_pollutants(ds: xr.Dataset) -> list[str]:
    return [name for name in SUPPORTED_POLLUTANTS if name in ds.data_vars]


def fetch_open_meteo_grid(
    config: RegionConfig = DEFAULT_REGION,
    output_path: str | Path | None = None,
    pollutants: Iterable[str] = SUPPORTED_POLLUTANTS,
    timeout: int = 90,
) -> xr.Dataset:
    latitudes, longitudes, points = build_grid(config)
    pollutant_list = [pollutant for pollutant in pollutants if pollutant in SUPPORTED_POLLUTANTS]
    if not pollutant_list:
        raise ValueError("At least one supported pollutant must be requested.")

    query = urlencode(
        {
            "latitude": ",".join(f"{lat:.4f}" for lat, _ in points),
            "longitude": ",".join(f"{lon:.4f}" for _, lon in points),
            "hourly": ",".join(pollutant_list),
            "forecast_hours": config.forecast_hours,
            "timezone": config.timezone,
            "domains": config.domains,
        }
    )

    with urlopen(f"{OPEN_METEO_AIR_QUALITY_URL}?{query}", timeout=timeout) as response:
        payload = json.load(response)

    records = payload if isinstance(payload, list) else [payload]
    if len(records) != len(points):
        raise ValueError(f"Expected {len(points)} point records but received {len(records)}")

    hourly = records[0]["hourly"]
    times = pd.to_datetime(hourly["time"])
    hourly_units = records[0].get("hourly_units", {})
    n_time = len(times)
    n_lat = len(latitudes)
    n_lon = len(longitudes)

    data_vars: dict[str, tuple[tuple[str, ...], np.ndarray, dict[str, str]]] = {}
    for pollutant in pollutant_list:
        cube = np.empty((n_time, n_lat, n_lon), dtype=float)
        for point_index, point_response in enumerate(records):
            values = point_response["hourly"].get(pollutant)
            if values is None:
                raise ValueError(f"Open-Meteo response missing hourly field for '{pollutant}'.")
            lat_index = point_index // n_lon
            lon_index = point_index % n_lon
            cube[:, lat_index, lon_index] = np.asarray(
                [np.nan if value is None else float(value) for value in values],
                dtype=float,
            )

        data_vars[pollutant] = (
            ("time", "lat", "lon"),
            cube,
            {"units": hourly_units.get(pollutant, "")},
        )

    ds = xr.Dataset(
        data_vars=data_vars,
        coords={"time": times, "lat": latitudes, "lon": longitudes},
        attrs={
            "title": "AtmosLens sample air-quality forecast",
            "source": "Open-Meteo Air Quality API",
            "domain": config.domains,
            "region_name": config.name,
            "timezone": config.timezone,
            "forecast_hours": str(config.forecast_hours),
            "bbox": json.dumps(
                {
                    "lat_min": config.lat_min,
                    "lat_max": config.lat_max,
                    "lon_min": config.lon_min,
                    "lon_max": config.lon_max,
                }
            ),
        },
    )

    ds = validate_dataset(ds)
    if output_path:
        target = _dataset_path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        ds.to_netcdf(target)
    return ds


def load_dataset(
    path: str | Path | None = None,
    *,
    allow_download: bool = False,
    config: RegionConfig = DEFAULT_REGION,
) -> xr.Dataset:
    dataset_path = _dataset_path(path)
    if dataset_path.exists():
        return validate_dataset(xr.load_dataset(dataset_path))
    if allow_download:
        return fetch_open_meteo_grid(config=config, output_path=dataset_path)
    raise FileNotFoundError(
        f"Could not find {dataset_path}. Run `atmoslens-fetch --output {dataset_path}` first."
    )


def location_series(ds: xr.Dataset, location_name: str, pollutant: str) -> pd.Series:
    lat, lon = DEFAULT_LOCATIONS[location_name]
    da = ds[pollutant].interp(lat=lat, lon=lon)
    series = pd.Series(da.values, index=pd.to_datetime(da["time"].values), name=pollutant)
    return series.sort_index()


def map_frame(ds: xr.Dataset, pollutant: str, timestamp: pd.Timestamp) -> xr.DataArray:
    return ds[pollutant].sel(time=timestamp, method="nearest")


def dataset_summary(ds: xr.Dataset) -> dict[str, object]:
    valid = validate_dataset(ds)
    return {
        "dims": {name: int(length) for name, length in valid.sizes.items()},
        "pollutants": available_pollutants(valid),
        "time_start": pd.Timestamp(valid.time.min().item()).isoformat(),
        "time_end": pd.Timestamp(valid.time.max().item()).isoformat(),
        "region_name": valid.attrs.get("region_name", ""),
        "source": valid.attrs.get("source", ""),
    }


def build_region_from_args(args: argparse.Namespace) -> RegionConfig:
    return replace(
        DEFAULT_REGION,
        lat_min=args.lat_min if args.lat_min is not None else DEFAULT_REGION.lat_min,
        lat_max=args.lat_max if args.lat_max is not None else DEFAULT_REGION.lat_max,
        lon_min=args.lon_min if args.lon_min is not None else DEFAULT_REGION.lon_min,
        lon_max=args.lon_max if args.lon_max is not None else DEFAULT_REGION.lon_max,
        n_lat=args.n_lat if args.n_lat is not None else DEFAULT_REGION.n_lat,
        n_lon=args.n_lon if args.n_lon is not None else DEFAULT_REGION.n_lon,
        forecast_hours=args.forecast_hours,
        timezone=args.timezone,
        domains=args.domains,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch a gridded Open-Meteo air-quality dataset for AtmosLens.")
    parser.add_argument("--output", default=str(DEFAULT_DATA_PATH))
    parser.add_argument("--forecast-hours", type=int, default=DEFAULT_REGION.forecast_hours)
    parser.add_argument("--timezone", default=DEFAULT_REGION.timezone)
    parser.add_argument("--domains", default=DEFAULT_REGION.domains)
    parser.add_argument("--lat-min", type=float)
    parser.add_argument("--lat-max", type=float)
    parser.add_argument("--lon-min", type=float)
    parser.add_argument("--lon-max", type=float)
    parser.add_argument("--n-lat", type=int)
    parser.add_argument("--n-lon", type=int)
    args = parser.parse_args(argv)

    config = build_region_from_args(args)
    ds = fetch_open_meteo_grid(config=config, output_path=args.output)
    summary = dataset_summary(ds)
    print(json.dumps(summary, indent=2))
    print(f"Wrote sample forecast to {Path(args.output).expanduser().resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

