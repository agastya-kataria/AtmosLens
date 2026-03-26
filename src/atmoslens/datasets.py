from __future__ import annotations

import argparse
import json
import math
import re
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd
import xarray as xr

from atmoslens.models import LocationDefinition, RouteDefinition

OPEN_METEO_AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
OPEN_METEO_GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
SUPPORTED_POLLUTANTS = ("pm2_5", "nitrogen_dioxide", "ozone", "european_aqi")
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_PATH = REPO_ROOT / "data" / "sample_forecast.nc"
LIVE_DATA_PATH = REPO_ROOT / "data" / "live_forecast.nc"
MAX_POINTS_PER_REQUEST = 100


@dataclass(frozen=True)
class RegionConfig:
    name: str
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float
    n_lat: int = 15
    n_lon: int = 17
    forecast_hours: int = 48
    timezone: str = "auto"
    domains: str = "auto"


def region_from_center(
    name: str,
    center_lat: float,
    center_lon: float,
    *,
    lat_span: float,
    lon_span: float,
    n_lat: int = 15,
    n_lon: int = 17,
    forecast_hours: int = 48,
    timezone: str = "auto",
    domains: str = "auto",
) -> RegionConfig:
    half_lat = lat_span / 2.0
    half_lon = lon_span / 2.0
    lat_min = max(-89.5, center_lat - half_lat)
    lat_max = min(89.5, center_lat + half_lat)
    lon_min = max(-179.5, center_lon - half_lon)
    lon_max = min(179.5, center_lon + half_lon)
    return RegionConfig(
        name=name,
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max,
        n_lat=n_lat,
        n_lon=n_lon,
        forecast_hours=forecast_hours,
        timezone=timezone,
        domains=domains,
    )


def _line_route(
    name: str,
    start_label: str,
    start: tuple[float, float],
    end_label: str,
    end: tuple[float, float],
    *,
    duration_minutes: int,
    region_name: str,
    description: str,
) -> RouteDefinition:
    lat1, lon1 = start
    lat2, lon2 = end
    mid1 = (lat1 + (lat2 - lat1) * 0.33, lon1 + (lon2 - lon1) * 0.33)
    mid2 = (lat1 + (lat2 - lat1) * 0.66, lon1 + (lon2 - lon1) * 0.66)
    return RouteDefinition(
        name=name,
        points=(start, mid1, mid2, end),
        duration_minutes=duration_minutes,
        description=description,
        start_label=start_label,
        end_label=end_label,
        region_name=region_name,
    )


REGION_PRESETS: dict[str, RegionConfig] = {
    "Dublin Metro": region_from_center(
        "Dublin Metro", 53.3498, -6.2603, lat_span=0.42, lon_span=0.55, timezone="Europe/Dublin", domains="cams_europe"
    ),
    "London Metro": region_from_center(
        "London Metro", 51.5072, -0.1276, lat_span=0.55, lon_span=0.85, timezone="Europe/London"
    ),
    "New York Metro": region_from_center(
        "New York Metro", 40.7128, -74.0060, lat_span=0.65, lon_span=0.9, timezone="America/New_York"
    ),
    "Los Angeles Basin": region_from_center(
        "Los Angeles Basin", 34.0522, -118.2437, lat_span=0.7, lon_span=1.1, timezone="America/Los_Angeles"
    ),
    "Delhi NCR": region_from_center(
        "Delhi NCR", 28.6139, 77.2090, lat_span=0.7, lon_span=1.0, timezone="Asia/Kolkata"
    ),
    "Tokyo Metro": region_from_center(
        "Tokyo Metro", 35.6762, 139.6503, lat_span=0.65, lon_span=0.9, timezone="Asia/Tokyo"
    ),
    "Sao Paulo Metro": region_from_center(
        "Sao Paulo Metro", -23.5505, -46.6333, lat_span=0.7, lon_span=0.9, timezone="America/Sao_Paulo"
    ),
    "Lagos Metro": region_from_center(
        "Lagos Metro", 6.5244, 3.3792, lat_span=0.55, lon_span=0.8, timezone="Africa/Lagos"
    ),
    "Singapore": region_from_center(
        "Singapore", 1.3521, 103.8198, lat_span=0.35, lon_span=0.5, timezone="Asia/Singapore"
    ),
    "Sydney Metro": region_from_center(
        "Sydney Metro", -33.8688, 151.2093, lat_span=0.8, lon_span=1.0, timezone="Australia/Sydney"
    ),
    "Cape Town Metro": region_from_center(
        "Cape Town Metro", -33.9249, 18.4241, lat_span=0.8, lon_span=1.0, timezone="Africa/Johannesburg"
    ),
}

LOCATION_PRESETS: dict[str, LocationDefinition] = {
    "Dublin Docklands": LocationDefinition("Docklands", 53.3478, -6.2374, "Europe/Dublin", "Dublin Metro"),
    "London Soho": LocationDefinition("Soho", 51.5136, -0.1365, "Europe/London", "London Metro"),
    "New York Midtown": LocationDefinition("Midtown", 40.7549, -73.9840, "America/New_York", "New York Metro"),
    "Los Angeles Downtown": LocationDefinition("Downtown LA", 34.0407, -118.2468, "America/Los_Angeles", "Los Angeles Basin"),
    "Delhi Connaught Place": LocationDefinition("Connaught Place", 28.6315, 77.2167, "Asia/Kolkata", "Delhi NCR"),
    "Tokyo Marunouchi": LocationDefinition("Marunouchi", 35.6812, 139.7671, "Asia/Tokyo", "Tokyo Metro"),
    "Sao Paulo Paulista": LocationDefinition("Paulista", -23.5614, -46.6566, "America/Sao_Paulo", "Sao Paulo Metro"),
    "Lagos Marina": LocationDefinition("Marina", 6.4541, 3.3947, "Africa/Lagos", "Lagos Metro"),
    "Singapore Marina Bay": LocationDefinition("Marina Bay", 1.2823, 103.8588, "Asia/Singapore", "Singapore"),
    "Sydney CBD": LocationDefinition("Sydney CBD", -33.8688, 151.2093, "Australia/Sydney", "Sydney Metro"),
    "Cape Town Foreshore": LocationDefinition("Foreshore", -33.9157, 18.4291, "Africa/Johannesburg", "Cape Town Metro"),
}

ROUTE_PRESETS: dict[str, RouteDefinition] = {
    "Sandyford to Docklands": _line_route(
        "Sandyford to Docklands",
        "Sandyford",
        (53.2749, -6.2256),
        "Docklands",
        (53.3478, -6.2374),
        duration_minutes=40,
        region_name="Dublin Metro",
        description="South suburbs into the city core; useful for commute exposure testing.",
    ),
    "Soho to Canary Wharf": _line_route(
        "Soho to Canary Wharf",
        "Soho",
        (51.5136, -0.1365),
        "Canary Wharf",
        (51.5054, -0.0235),
        duration_minutes=42,
        region_name="London Metro",
        description="West End to east-side business district commute through central London.",
    ),
    "Prospect Park to Midtown": _line_route(
        "Prospect Park to Midtown",
        "Prospect Park",
        (40.6602, -73.9690),
        "Midtown",
        (40.7549, -73.9840),
        duration_minutes=46,
        region_name="New York Metro",
        description="Brooklyn-to-Manhattan style commute crossing a dense urban core.",
    ),
    "Santa Monica to Downtown LA": _line_route(
        "Santa Monica to Downtown LA",
        "Santa Monica",
        (34.0195, -118.4912),
        "Downtown LA",
        (34.0407, -118.2468),
        duration_minutes=55,
        region_name="Los Angeles Basin",
        description="Coast-to-core route across a broad basin where timing materially changes exposure.",
    ),
    "Hauz Khas to Connaught Place": _line_route(
        "Hauz Khas to Connaught Place",
        "Hauz Khas",
        (28.5494, 77.2001),
        "Connaught Place",
        (28.6315, 77.2167),
        duration_minutes=38,
        region_name="Delhi NCR",
        description="Dense urban Delhi route with a strong need for hour-by-hour exposure guidance.",
    ),
    "Shibuya to Marunouchi": _line_route(
        "Shibuya to Marunouchi",
        "Shibuya",
        (35.6595, 139.7005),
        "Marunouchi",
        (35.6812, 139.7671),
        duration_minutes=30,
        region_name="Tokyo Metro",
        description="Central Tokyo trip where a short commute still benefits from route-time optimization.",
    ),
    "Vila Mariana to Paulista": _line_route(
        "Vila Mariana to Paulista",
        "Vila Mariana",
        (-23.5891, -46.6346),
        "Paulista",
        (-23.5614, -46.6566),
        duration_minutes=26,
        region_name="Sao Paulo Metro",
        description="Compact but meaningful corridor in Sao Paulo's dense urban core.",
    ),
    "Yaba to Marina": _line_route(
        "Yaba to Marina",
        "Yaba",
        (6.5095, 3.3796),
        "Marina",
        (6.4541, 3.3947),
        duration_minutes=33,
        region_name="Lagos Metro",
        description="Inland-to-waterfront Lagos commute with meaningful hour-to-hour changes.",
    ),
    "Queenstown to Marina Bay": _line_route(
        "Queenstown to Marina Bay",
        "Queenstown",
        (1.2942, 103.7864),
        "Marina Bay",
        (1.2823, 103.8588),
        duration_minutes=28,
        region_name="Singapore",
        description="Short Singapore commute that still benefits from route exposure timing.",
    ),
    "Newtown to Sydney CBD": _line_route(
        "Newtown to Sydney CBD",
        "Newtown",
        (-33.8981, 151.1746),
        "Sydney CBD",
        (-33.8688, 151.2093),
        duration_minutes=24,
        region_name="Sydney Metro",
        description="Inner-city Sydney route that shows the model works outside Europe and North America.",
    ),
    "Observatory to Foreshore": _line_route(
        "Observatory to Foreshore",
        "Observatory",
        (-33.9370, 18.4655),
        "Foreshore",
        (-33.9157, 18.4291),
        duration_minutes=22,
        region_name="Cape Town Metro",
        description="Cape Town commuter corridor useful for demonstrating a globally configurable route workflow.",
    ),
}

DEFAULT_REGION = REGION_PRESETS["Dublin Metro"]
DEFAULT_LOCATIONS = {name: (location.lat, location.lon) for name, location in LOCATION_PRESETS.items()}
DEFAULT_ROUTES = ROUTE_PRESETS


def build_grid(config: RegionConfig = DEFAULT_REGION) -> tuple[np.ndarray, np.ndarray, list[tuple[float, float]]]:
    latitudes = np.linspace(config.lat_min, config.lat_max, config.n_lat).round(4)
    longitudes = np.linspace(config.lon_min, config.lon_max, config.n_lon).round(4)
    points = [(float(lat), float(lon)) for lat in latitudes for lon in longitudes]
    return latitudes, longitudes, points


def _dataset_path(path: str | Path | None = None) -> Path:
    return Path(path).expanduser().resolve() if path else DEFAULT_DATA_PATH


def _fetch_json(url: str, timeout: int = 90, retries: int = 3) -> dict | list:
    request = Request(url, headers={"User-Agent": "AtmosLens/0.1"})
    for attempt in range(retries):
        try:
            with urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except HTTPError as exc:
            retryable = exc.code in {429, 500, 502, 503, 504}
            if retryable and attempt < retries - 1:
                time.sleep(1.2 * (attempt + 1))
                continue
            if exc.code == 429:
                raise ValueError(
                    "The upstream forecast service is temporarily rate-limiting requests. Wait a few seconds and try again."
                ) from exc
            raise ValueError(f"Upstream service returned HTTP {exc.code}.") from exc
        except URLError as exc:
            if attempt < retries - 1:
                time.sleep(1.2 * (attempt + 1))
                continue
            raise ValueError("Could not reach the upstream forecast service.") from exc
    raise ValueError("Could not fetch data from the upstream forecast service.")


def _chunked(items: list[tuple[float, float]], size: int) -> list[list[tuple[float, float]]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


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


def available_pollutants(ds: xr.Dataset) -> list[str]:
    return [name for name in SUPPORTED_POLLUTANTS if name in ds.data_vars]


def validate_dataset(ds: xr.Dataset) -> xr.Dataset:
    ds = _normalise_dataset(ds)
    required_dims = {"time", "lat", "lon"}
    missing_dims = required_dims.difference(ds.dims)
    if missing_dims:
        raise ValueError(f"Dataset missing expected dimensions: {sorted(missing_dims)}")
    if not available_pollutants(ds):
        raise ValueError("Dataset does not expose any supported pollutant variables.")
    return ds


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

    records: list[dict] = []
    for batch in _chunked(points, MAX_POINTS_PER_REQUEST):
        query = urlencode(
            {
                "latitude": ",".join(f"{lat:.4f}" for lat, _ in batch),
                "longitude": ",".join(f"{lon:.4f}" for _, lon in batch),
                "hourly": ",".join(pollutant_list),
                "forecast_hours": config.forecast_hours,
                "timezone": config.timezone,
                "domains": config.domains,
            }
        )
        payload = _fetch_json(f"{OPEN_METEO_AIR_QUALITY_URL}?{query}", timeout=timeout)
        batch_records = payload if isinstance(payload, list) else [payload]
        records.extend(batch_records)

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
            "title": "AtmosLens air-quality forecast",
            "source": "Open-Meteo Air Quality API",
            "forecast_mode": "live_grid",
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


def fetch_open_meteo_point(
    *,
    lat: float,
    lon: float,
    forecast_hours: int = 48,
    timezone: str = "auto",
    domains: str = "auto",
    pollutants: Iterable[str] = SUPPORTED_POLLUTANTS,
    timeout: int = 90,
) -> xr.Dataset:
    pollutant_list = [pollutant for pollutant in pollutants if pollutant in SUPPORTED_POLLUTANTS]
    if not pollutant_list:
        raise ValueError("At least one supported pollutant must be requested.")

    query = urlencode(
        {
            "latitude": f"{lat:.4f}",
            "longitude": f"{lon:.4f}",
            "hourly": ",".join(pollutant_list),
            "forecast_hours": forecast_hours,
            "timezone": timezone,
            "domains": domains,
        }
    )
    payload = _fetch_json(f"{OPEN_METEO_AIR_QUALITY_URL}?{query}", timeout=timeout)
    if not isinstance(payload, dict) or "hourly" not in payload:
        raise ValueError("The upstream point forecast response was incomplete.")

    hourly = payload["hourly"]
    times = pd.to_datetime(hourly["time"])
    hourly_units = payload.get("hourly_units", {})
    data_vars: dict[str, tuple[tuple[str, ...], np.ndarray, dict[str, str]]] = {}
    for pollutant in pollutant_list:
        values = hourly.get(pollutant)
        if values is None:
            raise ValueError(f"Open-Meteo response missing hourly field for '{pollutant}'.")
        data_vars[pollutant] = (
            ("time",),
            np.asarray([np.nan if value is None else float(value) for value in values], dtype=float),
            {"units": hourly_units.get(pollutant, "")},
        )

    return xr.Dataset(
        data_vars=data_vars,
        coords={"time": times},
        attrs={
            "title": "AtmosLens air-quality point forecast",
            "source": "Open-Meteo Air Quality API",
            "forecast_mode": "live_point",
            "timezone": timezone,
            "domains": domains,
            "center_lat": f"{lat:.4f}",
            "center_lon": f"{lon:.4f}",
        },
    )


def _realistic_spatial_pattern(n_lat: int, n_lon: int, seed: int = 0) -> np.ndarray:
    """Generate a spatially-correlated pattern that resembles real pollution fields.

    Uses a sum of low-frequency sinusoids with random phases to mimic the smooth,
    non-uniform spatial gradients seen in real gridded forecast data — avoiding the
    obviously-fake diagonal gradient of a simple ``lat - lon`` formula.
    """
    rng = np.random.RandomState(seed)
    lat_grid = np.linspace(-1.0, 1.0, n_lat)[:, None]
    lon_grid = np.linspace(-1.0, 1.0, n_lon)[None, :]
    pattern = np.zeros((n_lat, n_lon), dtype=float)
    # Sum several LOW-frequency spatial harmonics so even coarse grids look smooth
    for _ in range(5):
        freq_lat = rng.uniform(0.3, 1.2)
        freq_lon = rng.uniform(0.3, 1.2)
        phase = rng.uniform(0, 2 * np.pi)
        amplitude = rng.uniform(0.15, 0.35)
        pattern += amplitude * np.sin(freq_lat * np.pi * lat_grid + freq_lon * np.pi * lon_grid + phase)
    # Add a slight urban-centre hotspot (center of grid is often the city core)
    dist_from_center = np.sqrt(lat_grid**2 + lon_grid**2)
    pattern += 0.3 * np.exp(-1.5 * dist_from_center**2)
    # Normalize to roughly [-1, 1]
    ptp = pattern.max() - pattern.min()
    if ptp > 0:
        pattern = 2.0 * (pattern - pattern.min()) / ptp - 1.0
    return pattern[None, :, :]  # (1, lat, lon) for broadcasting with time


def expand_point_forecast_to_grid(
    point_ds: xr.Dataset,
    config: RegionConfig,
    *,
    output_path: str | Path | None = None,
) -> xr.Dataset:
    latitudes, longitudes, _ = build_grid(config)
    spatial_pattern = _realistic_spatial_pattern(len(latitudes), len(longitudes), seed=42)

    data_vars: dict[str, tuple[tuple[str, ...], np.ndarray, dict[str, str]]] = {}
    for pollutant in available_pollutants(point_ds):
        series = np.asarray(point_ds[pollutant].values, dtype=float)
        spread = max(float(np.nanstd(series)) * 0.45, float(np.nanmean(series)) * 0.05, 0.8)
        cube = np.clip(series[:, None, None] + spatial_pattern * spread, a_min=0.0, a_max=None)
        data_vars[pollutant] = (
            ("time", "lat", "lon"),
            cube,
            {"units": str(point_ds[pollutant].attrs.get("units", ""))},
        )

    ds = xr.Dataset(
        data_vars=data_vars,
        coords={"time": pd.to_datetime(point_ds.time.values), "lat": latitudes, "lon": longitudes},
        attrs={
            "title": "AtmosLens approximate local fallback forecast",
            "source": "Open-Meteo point forecast expanded to a local grid",
            "forecast_mode": "point_fallback",
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


def build_template_fallback_grid(
    config: RegionConfig,
    *,
    output_path: str | Path | None = None,
) -> xr.Dataset:
    template = validate_dataset(xr.load_dataset(DEFAULT_DATA_PATH))
    latitudes, longitudes, _ = build_grid(config)
    spatial_pattern = _realistic_spatial_pattern(len(latitudes), len(longitudes), seed=17)

    data_vars: dict[str, tuple[tuple[str, ...], np.ndarray, dict[str, str]]] = {}
    for pollutant in available_pollutants(template):
        source = np.asarray(template[pollutant].values, dtype=float)
        base = source.mean(axis=(1, 2), keepdims=True)
        spread = max(float(np.nanstd(base[:, 0, 0])) * 0.55, float(np.nanmean(base[:, 0, 0])) * 0.06, 1.0)
        cube = np.clip(base + spatial_pattern * spread, a_min=0.0, a_max=None)
        data_vars[pollutant] = (
            ("time", "lat", "lon"),
            cube,
            {"units": str(template[pollutant].attrs.get("units", ""))},
        )

    ds = xr.Dataset(
        data_vars=data_vars,
        coords={"time": pd.to_datetime(template.time.values), "lat": latitudes, "lon": longitudes},
        attrs={
            "title": "AtmosLens bundled fallback forecast",
            "source": "Bundled AtmosLens fallback cube",
            "forecast_mode": "template_fallback",
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


def fetch_resilient_forecast(
    config: RegionConfig = DEFAULT_REGION,
    *,
    output_path: str | Path | None = None,
    pollutants: Iterable[str] = SUPPORTED_POLLUTANTS,
    timeout: int = 90,
) -> tuple[xr.Dataset, str]:
    try:
        ds = fetch_open_meteo_grid(config=config, output_path=output_path, pollutants=pollutants, timeout=timeout)
        return ds, (
            f"Loaded live forecast cube for {config.name} centred on "
            f"{config.lat_min + (config.lat_max - config.lat_min) / 2:.2f}, "
            f"{config.lon_min + (config.lon_max - config.lon_min) / 2:.2f}."
        )
    except ValueError as exc:
        primary_error = str(exc)
        retryable = "rate-limiting" in primary_error or "Could not reach" in primary_error
        if not retryable:
            raise

    try:
        point_ds = fetch_open_meteo_point(
            lat=(config.lat_min + config.lat_max) / 2.0,
            lon=(config.lon_min + config.lon_max) / 2.0,
            forecast_hours=config.forecast_hours,
            timezone=config.timezone,
            domains=config.domains,
            pollutants=pollutants,
            timeout=timeout,
        )
        ds = expand_point_forecast_to_grid(point_ds, config, output_path=output_path)
        return ds, (
            f"The upstream gridded forecast service was temporarily busy, so AtmosLens loaded an approximate local fallback cube for "
            f"{config.name}. Retry `Refresh Forecast Cube` later for a full-resolution live map."
        )
    except Exception:
        ds = build_template_fallback_grid(config, output_path=output_path)
        return ds, (
            f"The upstream forecast service was unavailable, so AtmosLens loaded a bundled fallback cube centred on {config.name}. "
            f"Retry `Refresh Forecast Cube` later for live data."
        )


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
    raise FileNotFoundError(f"Could not find {dataset_path}. Run `atmoslens-fetch --output {dataset_path}` first.")


def dataset_bounds(ds: xr.Dataset) -> dict[str, float]:
    valid = validate_dataset(ds)
    return {
        "lat_min": float(valid.lat.min()),
        "lat_max": float(valid.lat.max()),
        "lon_min": float(valid.lon.min()),
        "lon_max": float(valid.lon.max()),
    }


def coordinates_in_bounds(ds: xr.Dataset, lat: float, lon: float) -> bool:
    bounds = dataset_bounds(ds)
    return bounds["lat_min"] <= lat <= bounds["lat_max"] and bounds["lon_min"] <= lon <= bounds["lon_max"]


def assert_coordinates_in_bounds(ds: xr.Dataset, lat: float, lon: float, *, label: str) -> None:
    if coordinates_in_bounds(ds, lat, lon):
        return
    bounds = dataset_bounds(ds)
    raise ValueError(
        f"{label} ({lat:.3f}, {lon:.3f}) is outside the current forecast cube. "
        f"Current bounds are lat {bounds['lat_min']:.3f} to {bounds['lat_max']:.3f}, "
        f"lon {bounds['lon_min']:.3f} to {bounds['lon_max']:.3f}. Refresh the region or edit the coordinates."
    )


def location_series(ds: xr.Dataset, lat: float, lon: float, pollutant: str) -> pd.Series:
    assert_coordinates_in_bounds(ds, lat, lon, label="Selected location")
    da = ds[pollutant].interp(lat=lat, lon=lon)
    series = pd.Series(da.values, index=pd.to_datetime(da["time"].values), name=pollutant).sort_index()
    if series.isna().all():
        raise ValueError("No forecast values were available for the selected location.")
    return series


def map_frame(ds: xr.Dataset, pollutant: str, timestamp: pd.Timestamp) -> xr.DataArray:
    return ds[pollutant].sel(time=timestamp, method="nearest")


def location_presets_for_region(region_name: str) -> list[str]:
    return [name for name, location in LOCATION_PRESETS.items() if location.region_name == region_name]


def route_presets_for_region(region_name: str) -> list[str]:
    return [name for name, route in ROUTE_PRESETS.items() if route.region_name == region_name]


def build_route_from_endpoints(
    name: str,
    start_label: str,
    start: tuple[float, float],
    end_label: str,
    end: tuple[float, float],
    *,
    duration_minutes: int,
    region_name: str = "",
) -> RouteDefinition:
    description = f"Custom route from {start_label} to {end_label} generated from user-provided endpoints."
    return _line_route(
        name,
        start_label,
        start,
        end_label,
        end,
        duration_minutes=duration_minutes,
        region_name=region_name,
        description=description,
    )


def _search_display_name(result: dict[str, object]) -> str:
    parts = [str(result.get("name", "")).strip()]
    admin1 = str(result.get("admin1", "")).strip()
    country = str(result.get("country", "")).strip()
    for part in (admin1, country):
        if part and part not in parts:
            parts.append(part)
    return ", ".join(part for part in parts if part)


def _search_description(result: dict[str, object]) -> str:
    parts = []
    feature = str(result.get("feature_code", "")).strip()
    timezone = str(result.get("timezone", "")).strip()
    country_code = str(result.get("country_code", "")).strip()
    if feature:
        parts.append(feature)
    if country_code:
        parts.append(country_code)
    if timezone:
        parts.append(timezone)
    return " | ".join(parts)


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat_scale = 111.0
    lon_scale = 111.0 * math.cos(math.radians((lat1 + lat2) / 2.0))
    return math.hypot((lat2 - lat1) * lat_scale, (lon2 - lon1) * lon_scale)


def _query_variants(query: str, *, context: str | None = None) -> list[str]:
    clean_query = " ".join(query.split())
    tokens = [token for token in re.split(r"[\s,]+", clean_query) if token]
    variants = [clean_query]
    clean_context = " ".join((context or "").split())
    if len(tokens) >= 2:
        variants.extend(
            [
                f"{tokens[0]}, {' '.join(tokens[1:])}",
                f"{' '.join(tokens[1:])} {tokens[0]}",
                f"{tokens[-1]}, {' '.join(tokens[:-1])}",
                " ".join(tokens[:2]),
                " ".join(tokens[-2:]),
                tokens[0],
                tokens[-1],
            ]
        )
    elif tokens:
        variants.append(tokens[0])

    if clean_context:
        variants.extend(
            [
                f"{clean_query} {clean_context}",
                f"{clean_query}, {clean_context}",
            ]
        )

    deduped: list[str] = []
    seen: set[str] = set()
    for variant in variants:
        candidate = " ".join(variant.split()).strip(" ,")
        normalised = candidate.casefold()
        if len(candidate) < 2 or normalised in seen:
            continue
        seen.add(normalised)
        deduped.append(candidate)
    return deduped


def _result_search_text(result: dict[str, object]) -> str:
    return " ".join(
        str(result.get(field, "")).strip()
        for field in ("name", "admin1", "admin2", "country")
    ).casefold()


def _nominatim_result_to_search_result(result: dict[str, object]) -> dict[str, object]:
    address = result.get("address", {}) if isinstance(result.get("address", {}), dict) else {}
    display_name = str(result.get("display_name", "")).strip()
    parts = [part.strip() for part in display_name.split(",") if part.strip()]
    name = parts[0] if parts else str(result.get("name", "")).strip() or display_name
    admin1 = (
        str(address.get("state") or address.get("province") or address.get("county") or "").strip()
        or (parts[1] if len(parts) > 2 else "")
    )
    admin2 = str(address.get("city") or address.get("town") or address.get("municipality") or "").strip()
    country = str(address.get("country") or (parts[-1] if parts else "")).strip()
    return {
        "name": name,
        "admin1": admin1,
        "admin2": admin2,
        "country": country,
        "country_code": str(address.get("country_code") or "").upper(),
        "feature_code": str(result.get("type") or "").upper(),
        "latitude": float(result["lat"]),
        "longitude": float(result["lon"]),
        "timezone": "auto",
        "population": 0,
    }


def _search_places_nominatim(query: str, *, count: int, language: str, timeout: int) -> list[dict[str, object]]:
    params = urlencode(
        {
            "q": query,
            "format": "jsonv2",
            "limit": count,
            "addressdetails": 1,
            "accept-language": language,
        }
    )
    payload = _fetch_json(f"{NOMINATIM_SEARCH_URL}?{params}", timeout=timeout)
    if not isinstance(payload, list):
        return []
    allowed_address_types = {
        "administrative",
        "borough",
        "city",
        "country",
        "county",
        "district",
        "hamlet",
        "island",
        "municipality",
        "neighbourhood",
        "postcode",
        "province",
        "quarter",
        "state",
        "suburb",
        "town",
        "village",
    }
    return [
        _nominatim_result_to_search_result(result)
        for result in payload
        if "lat" in result
        and "lon" in result
        and str(result.get("addresstype") or "").strip().casefold() in allowed_address_types
    ]


def _search_score(
    result: dict[str, object],
    query_tokens: list[str],
    variant_index: int,
    *,
    reference: tuple[float, float] | None = None,
    country_bias: str | None = None,
) -> tuple[float, int, int, int, int]:
    search_text = _result_search_text(result)
    overlap = sum(token in search_text for token in query_tokens)
    exact_name = int(str(result.get("name", "")).strip().casefold() == " ".join(query_tokens))
    population = int(float(result.get("population") or 0))
    feature = str(result.get("feature_code", "")).strip().upper()
    feature_bonus = {
        "PPLC": 5,
        "PPLA": 4,
        "PPLA2": 3,
        "PPLA3": 2,
        "PPLA4": 1,
        "PPL": 2,
        "ADM1": 1,
        "ADM2": 1,
        "ADM3": 1,
        "ADM4": 1,
    }.get(feature, 0)
    country_text = " ".join(
        str(result.get(field, "")).strip().casefold()
        for field in ("country", "country_code", "admin1", "admin2")
    )
    country_match = int(bool(country_bias) and country_bias.casefold() in country_text)
    distance_bucket = 0
    proximity_bonus = 0.0
    if reference is not None:
        distance_km = _distance_km(reference[0], reference[1], float(result["latitude"]), float(result["longitude"]))
        distance_bucket = -int(distance_km // 25.0)
        proximity_bonus = max(0.0, 2.5 - min(distance_km, 2000.0) / 800.0)
    text_score = overlap * 2.5 + exact_name * 3.0 + feature_bonus / 5.0 + country_match * 1.4 + proximity_bonus
    return (text_score, country_match, overlap, -variant_index, max(population, exact_name * 10_000_000) + distance_bucket)


def search_places(
    query: str,
    *,
    count: int = 6,
    language: str = "en",
    timeout: int = 30,
    reference: tuple[float, float] | None = None,
    country_bias: str | None = None,
    context: str | None = None,
) -> list[LocationDefinition]:
    clean_query = " ".join(query.split())
    if len(clean_query) < 2:
        raise ValueError("Type at least two characters before searching for a place.")

    query_tokens = [token.casefold() for token in re.split(r"[\s,]+", clean_query) if token]
    aggregated: list[tuple[tuple[float, int, int, int], dict[str, object]]] = []
    seen_results: set[tuple[str, float, float]] = set()

    for variant_index, variant in enumerate(_query_variants(clean_query, context=context)):
        params = urlencode({"name": variant, "count": max(count, 8), "language": language, "format": "json"})
        payload = _fetch_json(f"{OPEN_METEO_GEOCODING_URL}?{params}", timeout=timeout)
        results = payload.get("results", []) if isinstance(payload, dict) else []
        for result in results:
            lat = round(float(result["latitude"]), 5)
            lon = round(float(result["longitude"]), 5)
            key = (str(result.get("name", "")).strip().casefold(), lat, lon)
            if key in seen_results:
                continue
            seen_results.add(key)
            aggregated.append(
                (
                    _search_score(
                        result,
                        query_tokens,
                        variant_index,
                        reference=reference,
                        country_bias=country_bias,
                    ),
                    result,
                )
            )
        if len(aggregated) >= count * 3:
            break

    fallback_variants = [clean_query]
    if context:
        fallback_variants.append(f"{clean_query} {context}")
    if country_bias:
        fallback_variants.append(f"{clean_query} {country_bias}")
    fallback_variants = list(dict.fromkeys(fallback_variants))
    if len(aggregated) < count * 2 or context:
        base_variant_count = len(_query_variants(clean_query, context=context))
        for extra_index, variant in enumerate(fallback_variants):
            for result in _search_places_nominatim(variant, count=max(4, count), language=language, timeout=timeout):
                lat = round(float(result["latitude"]), 5)
                lon = round(float(result["longitude"]), 5)
                key = (str(result.get("name", "")).strip().casefold(), lat, lon)
                if key in seen_results:
                    continue
                seen_results.add(key)
                aggregated.append(
                    (
                        _search_score(
                            result,
                            query_tokens,
                            base_variant_count + extra_index,
                            reference=reference,
                            country_bias=country_bias,
                        ),
                        result,
                    )
                )

    if not aggregated:
        raise ValueError(f"No places matched '{clean_query}'. Try a city, district, or postcode.")

    locations = []
    for _, result in sorted(aggregated, key=lambda item: item[0], reverse=True)[:count]:
        lat = float(result["latitude"])
        lon = float(result["longitude"])
        timezone = str(result.get("timezone") or "auto")
        locations.append(
            LocationDefinition(
                name=_search_display_name(result),
                lat=lat,
                lon=lon,
                timezone=timezone,
                region_name=str(result.get("country", "")).strip(),
                description=_search_description(result),
            )
        )
    return locations


def location_label(location: LocationDefinition) -> str:
    suffix = f" | {location.description}" if location.description else ""
    return f"{location.name} | {location.lat:.3f}, {location.lon:.3f}{suffix}"


def dataset_summary(ds: xr.Dataset) -> dict[str, object]:
    valid = validate_dataset(ds)
    summary = {
        "dims": {name: int(length) for name, length in valid.sizes.items()},
        "pollutants": available_pollutants(valid),
        "time_start": pd.Timestamp(valid.time.min().item()).isoformat(),
        "time_end": pd.Timestamp(valid.time.max().item()).isoformat(),
        "region_name": valid.attrs.get("region_name", ""),
        "source": valid.attrs.get("source", ""),
        "forecast_mode": valid.attrs.get("forecast_mode", "unknown"),
        "timezone": valid.attrs.get("timezone", ""),
    }
    summary.update(dataset_bounds(valid))
    return summary


def build_region_from_args(args: argparse.Namespace) -> RegionConfig:
    if args.center_lat is not None and args.center_lon is not None:
        return region_from_center(
            args.name,
            args.center_lat,
            args.center_lon,
            lat_span=args.lat_span,
            lon_span=args.lon_span,
            n_lat=args.n_lat if args.n_lat is not None else DEFAULT_REGION.n_lat,
            n_lon=args.n_lon if args.n_lon is not None else DEFAULT_REGION.n_lon,
            forecast_hours=args.forecast_hours,
            timezone=args.timezone,
            domains=args.domains,
        )
    return replace(
        DEFAULT_REGION,
        name=args.name,
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
    parser.add_argument("--name", default=DEFAULT_REGION.name)
    parser.add_argument("--forecast-hours", type=int, default=DEFAULT_REGION.forecast_hours)
    parser.add_argument("--timezone", default=DEFAULT_REGION.timezone)
    parser.add_argument("--domains", default=DEFAULT_REGION.domains)
    parser.add_argument("--lat-min", type=float)
    parser.add_argument("--lat-max", type=float)
    parser.add_argument("--lon-min", type=float)
    parser.add_argument("--lon-max", type=float)
    parser.add_argument("--center-lat", type=float)
    parser.add_argument("--center-lon", type=float)
    parser.add_argument("--lat-span", type=float, default=DEFAULT_REGION.lat_max - DEFAULT_REGION.lat_min)
    parser.add_argument("--lon-span", type=float, default=DEFAULT_REGION.lon_max - DEFAULT_REGION.lon_min)
    parser.add_argument("--n-lat", type=int)
    parser.add_argument("--n-lon", type=int)
    args = parser.parse_args(argv)

    config = build_region_from_args(args)
    ds = fetch_open_meteo_grid(config=config, output_path=args.output)
    print(json.dumps(dataset_summary(ds), indent=2))
    print(f"Wrote forecast cube to {Path(args.output).expanduser().resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
