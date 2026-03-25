from __future__ import annotations

import math

import numpy as np
import pandas as pd
import xarray as xr

from atmoslens.models import RouteDefinition
from atmoslens.scoring import classify_verdict, score_value


def _segment_distance_km(start: tuple[float, float], end: tuple[float, float]) -> float:
    lat1, lon1 = start
    lat2, lon2 = end
    lat_scale = 111.0
    lon_scale = 111.0 * math.cos(math.radians((lat1 + lat2) / 2))
    return math.hypot((lat2 - lat1) * lat_scale, (lon2 - lon1) * lon_scale)


def interpolate_route(route: RouteDefinition, samples: int = 32) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if len(route.points) < 2:
        raise ValueError("A route needs at least two points.")

    cumulative = [0.0]
    for start, end in zip(route.points[:-1], route.points[1:]):
        cumulative.append(cumulative[-1] + _segment_distance_km(start, end))

    total_distance = cumulative[-1] or 1.0
    targets = np.linspace(0.0, total_distance, samples)
    latitudes = np.empty(samples)
    longitudes = np.empty(samples)

    for index, target in enumerate(targets):
        for segment_index in range(len(cumulative) - 1):
            start_distance = cumulative[segment_index]
            end_distance = cumulative[segment_index + 1]
            if target <= end_distance or segment_index == len(cumulative) - 2:
                start = route.points[segment_index]
                end = route.points[segment_index + 1]
                span = max(end_distance - start_distance, 1e-9)
                weight = (target - start_distance) / span
                latitudes[index] = start[0] + (end[0] - start[0]) * weight
                longitudes[index] = start[1] + (end[1] - start[1]) * weight
                break

    fractions = targets / total_distance
    return latitudes, longitudes, fractions


def route_exposure_profile(
    ds: xr.Dataset,
    route: RouteDefinition,
    pollutant: str,
    departure_time: pd.Timestamp,
    *,
    samples: int = 32,
) -> pd.DataFrame:
    latitudes, longitudes, fractions = interpolate_route(route, samples=samples)
    timestamps = departure_time + pd.to_timedelta(fractions * route.duration_minutes, unit="minute")

    values = ds[pollutant].interp(
        time=xr.DataArray(timestamps.to_numpy(), dims="sample"),
        lat=xr.DataArray(latitudes, dims="sample"),
        lon=xr.DataArray(longitudes, dims="sample"),
    )
    frame = pd.DataFrame(
        {
            "sample": np.arange(samples),
            "fraction": fractions,
            "time": timestamps,
            "lat": latitudes,
            "lon": longitudes,
            "concentration": values.values.astype(float),
        }
    )
    return frame


def candidate_departures(
    ds: xr.Dataset,
    route: RouteDefinition,
    *,
    horizon_hours: int = 24,
) -> list[pd.Timestamp]:
    times = pd.to_datetime(ds.time.values)
    latest_departure = times.max() - pd.Timedelta(minutes=route.duration_minutes)
    limited = [time for time in times if time <= latest_departure]
    horizon_end = limited[0] + pd.Timedelta(hours=horizon_hours)
    return [time for time in limited if time < horizon_end]


def rank_route_departures(
    ds: xr.Dataset,
    route: RouteDefinition,
    pollutant: str,
    profile_name: str,
    activity_name: str,
    *,
    horizon_hours: int = 24,
) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for departure in candidate_departures(ds, route, horizon_hours=horizon_hours):
        profile = route_exposure_profile(ds, route, pollutant, departure)
        mean_value = float(profile["concentration"].mean())
        peak_value = float(profile["concentration"].max())
        blended_value = 0.6 * mean_value + 0.4 * peak_value
        score = score_value(blended_value, pollutant, profile_name, activity_name)
        records.append(
            {
                "departure": departure,
                "arrival": departure + pd.Timedelta(minutes=route.duration_minutes),
                "mean_value": mean_value,
                "peak_value": peak_value,
                "blended_value": blended_value,
                "score": score,
                "verdict": classify_verdict(score),
            }
        )

    return pd.DataFrame.from_records(records)

