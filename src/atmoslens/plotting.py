from __future__ import annotations

import os
from pathlib import Path

MPL_CACHE = Path(__file__).resolve().parents[2] / ".mpl-cache"
MPL_CACHE.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPL_CACHE))

import cartopy.crs as ccrs
import colorcet as cc
import geoviews as gv
import holoviews as hv
import hvplot.pandas  # noqa: F401
import hvplot.xarray  # noqa: F401
import numpy as np
import pandas as pd
from holoviews.operation.datashader import quadmesh_rasterize

from atmoslens.models import AnalysisResult, LocationDefinition, RouteDefinition
from atmoslens.profiles import adjusted_thresholds, pollutant_meta

gv.extension("bokeh")
hv.extension("bokeh")


def pollutant_cmap(pollutant: str):
    return {
        "pm2_5": cc.fire,
        "nitrogen_dioxide": cc.kbc,
        "ozone": cc.bgy,
        "european_aqi": cc.rainbow4,
    }[pollutant]


def _color_limits(frame) -> tuple[float, float]:
    values = np.asarray(frame.values, dtype=float)
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return (0.0, 1.0)
    low = float(np.quantile(finite, 0.05))
    high = float(np.quantile(finite, 0.95))
    if high <= low:
        high = low + 1.0
    return (low, high)


def build_pollution_map(
    frame,
    pollutant: str,
    timestamp: pd.Timestamp,
    *,
    location: LocationDefinition,
    route: RouteDefinition | None = None,
):
    meta = pollutant_meta(pollutant)
    clim = _color_limits(frame)
    quadmesh = gv.QuadMesh(frame, crs=ccrs.PlateCarree())
    raster = quadmesh_rasterize(quadmesh, aggregator="mean", dynamic=False).opts(
        width=820,
        height=500,
        alpha=0.86,
        cmap=pollutant_cmap(pollutant),
        clim=clim,
        colorbar=True,
        title=f"{meta['label']} map for {timestamp:%a %d %b %H:%M}",
        clabel=f"{meta['label']} ({meta['unit']})",
        tools=["hover"],
    )
    tiles = gv.tile_sources.CartoLight.opts(alpha=0.7)

    active_location = pd.DataFrame(
        [{"name": location.name, "lat": location.lat, "lon": location.lon, "kind": "Decision point"}]
    )
    points = gv.Points(
        active_location,
        kdims=["lon", "lat"],
        vdims=["name", "kind"],
        crs=ccrs.PlateCarree(),
    ).opts(
        size=16,
        line_color="#0f172a",
        fill_color="#d97706",
        line_width=2,
        tools=["hover"],
        marker="diamond",
    )

    overlays = tiles * raster * points
    if route is not None:
        route_points = pd.DataFrame(
            [
                {"name": route.start_label or "Start", "lat": route.points[0][0], "lon": route.points[0][1], "kind": "Route start"},
                {"name": route.end_label or "End", "lat": route.points[-1][0], "lon": route.points[-1][1], "kind": "Route end"},
            ]
        )
        route_markers = gv.Points(
            route_points,
            kdims=["lon", "lat"],
            vdims=["name", "kind"],
            crs=ccrs.PlateCarree(),
        ).opts(
            size=12,
            line_color="#0f172a",
            fill_color="#f8fafc",
            line_width=2,
            tools=["hover"],
        )
        route_path = gv.Path(
            [[(lon, lat) for lat, lon in route.points]],
            crs=ccrs.PlateCarree(),
        ).opts(color="#0f172a", line_width=4, alpha=0.8)
        overlays = overlays * route_path * route_markers

    return overlays.opts(toolbar="right", active_tools=["wheel_zoom"], show_legend=False)


def build_timeline_plot(
    result: AnalysisResult,
    pollutant: str,
    profile_name: str,
    activity_name: str,
):
    meta = pollutant_meta(pollutant)
    thresholds = adjusted_thresholds(pollutant, profile_name, activity_name)
    timeline = pd.DataFrame(result.timeline_records)
    windows = pd.DataFrame(result.window_records)
    best = windows.sort_values("score", ascending=True).iloc[0]

    curve = timeline.hvplot.line(
        x="time",
        y="value",
        line_width=3,
        color="#0f766e",
        width=820,
        height=330,
        ylabel=f"{meta['label']} ({meta['unit']})",
        xlabel="Forecast time",
        title="Forecast and decision window",
    )
    markers = timeline.hvplot.scatter(x="time", y="value", color="#0f766e", size=50, alpha=0.7)
    good_band = hv.HSpan(0, thresholds["good"]).opts(fill_color="#d7f4df", fill_alpha=0.55)
    caution_band = hv.HSpan(thresholds["good"], thresholds["caution"]).opts(
        fill_color="#fde7b2", fill_alpha=0.35
    )
    top_value = max(float(timeline["value"].max()) * 1.1, float(thresholds["caution"]) * 1.2)
    avoid_band = hv.HSpan(thresholds["caution"], top_value).opts(fill_color="#f9c8c2", fill_alpha=0.25)
    best_window = hv.VSpan(best["start"], best["end"]).opts(fill_color="#d97706", fill_alpha=0.12)
    return (good_band * caution_band * avoid_band * best_window * curve * markers).opts(legend_position="top_left")


def build_route_plot(
    result: AnalysisResult,
    pollutant: str,
    profile_name: str,
    activity_name: str,
):
    meta = pollutant_meta(pollutant)
    thresholds = adjusted_thresholds(pollutant, profile_name, activity_name)
    route_df = pd.DataFrame(result.route_records)
    best = route_df.sort_values("score", ascending=True).iloc[0]

    line = route_df.hvplot.step(
        x="departure",
        y="mean_value",
        where="mid",
        line_width=3,
        color="#0f172a",
        width=820,
        height=330,
        ylabel=f"Mean {meta['label']} ({meta['unit']})",
        xlabel="Departure time",
        title="Departure-time route exposure",
    )
    points = route_df.hvplot.scatter(x="departure", y="mean_value", color="#0f172a", size=55, alpha=0.75)
    good_band = hv.HSpan(0, thresholds["good"]).opts(fill_color="#d7f4df", fill_alpha=0.55)
    caution_band = hv.HSpan(thresholds["good"], thresholds["caution"]).opts(
        fill_color="#fde7b2", fill_alpha=0.35
    )
    upper = max(float(route_df["mean_value"].max()) * 1.15, float(thresholds["caution"]) * 1.2)
    avoid_band = hv.HSpan(thresholds["caution"], upper).opts(fill_color="#f9c8c2", fill_alpha=0.25)
    best_window = hv.VSpan(best["departure"], best["arrival"]).opts(fill_color="#0f766e", fill_alpha=0.12)
    return (good_band * caution_band * avoid_band * best_window * line * points).opts(legend_position="top_left")


def build_scenario_matrix_plot(matrix: pd.DataFrame):
    score_map = hv.HeatMap(
        matrix,
        kdims=["activity", "profile"],
        vdims=["score", "verdict", "best_window", "headline"],
    ).opts(
        width=620,
        height=280,
        cmap=["#0f766e", "#f59e0b", "#dc2626"],
        clim=(0, 100),
        colorbar=True,
        colorbar_position="right",
        title="Profile and activity decision matrix",
        tools=["hover"],
        xrotation=15,
        line_color="#e2e8f0",
        toolbar="right",
    )
    labels = hv.Labels(matrix, kdims=["activity", "profile"], vdims=["verdict"]).opts(
        text_color="white",
        text_font_size="10pt",
    )
    return (score_map * labels).opts(show_legend=False)
