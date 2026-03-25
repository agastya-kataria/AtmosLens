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

from atmoslens.models import AnalysisResult, LocationDefinition, RouteDefinition
from atmoslens.profiles import adjusted_thresholds, pollutant_meta

gv.extension("bokeh")
hv.extension("bokeh")


def pollutant_cmap(pollutant: str):
    return {
        "pm2_5": ["#15803d", "#65a30d", "#facc15", "#f97316", "#dc2626", "#7f1d1d"],
        "nitrogen_dioxide": ["#166534", "#4d7c0f", "#fde047", "#fb923c", "#dc2626", "#7f1d1d"],
        "ozone": ["#166534", "#65a30d", "#facc15", "#fb923c", "#ef4444", "#7f1d1d"],
        "european_aqi": ["#15803d", "#84cc16", "#facc15", "#fb923c", "#dc2626", "#7f1d1d"],
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
    timezone_label: str,
    route: RouteDefinition | None = None,
):
    meta = pollutant_meta(pollutant)
    clim = _color_limits(frame)
    lons = np.asarray(frame["lon"].values, dtype=float)
    lats = np.asarray(frame["lat"].values, dtype=float)
    mesh = gv.QuadMesh(
        (lons, lats, np.asarray(frame.values, dtype=float)),
        kdims=["Longitude", "Latitude"],
        vdims=[meta["label"]],
        crs=ccrs.PlateCarree(),
    ).opts(
        width=860,
        height=520,
        responsive=False,
        alpha=0.72,
        cmap=pollutant_cmap(pollutant),
        clim=clim,
        colorbar=True,
        title=f"{meta['label']} — {timestamp:%a %d %b %H:%M} ({timezone_label})",
        clabel=f"{meta['label']} ({meta['unit']})",
        line_alpha=0,
        projection=ccrs.GOOGLE_MERCATOR,
        tools=["hover"],
    )
    lon_pad = max(0.05, (float(lons.max()) - float(lons.min())) * 0.15)
    lat_pad = max(0.05, (float(lats.max()) - float(lats.min())) * 0.15)
    tiles = gv.tile_sources.CartoLight.opts(
        alpha=0.88,
        xlim=(float(lons.min()) - lon_pad, float(lons.max()) + lon_pad),
        ylim=(float(lats.min()) - lat_pad, float(lats.max()) + lat_pad),
    )

    active_location = pd.DataFrame(
        [{"name": location.name, "lat": location.lat, "lon": location.lon, "kind": "Decision point"}]
    )
    point_glow = gv.Points(
        active_location,
        kdims=["lon", "lat"],
        vdims=["name", "kind"],
        crs=ccrs.PlateCarree(),
    ).opts(size=36, fill_alpha=0.15, fill_color="#d97706", line_alpha=0)
    points = gv.Points(
        active_location,
        kdims=["lon", "lat"],
        vdims=["name", "kind"],
        crs=ccrs.PlateCarree(),
    ).opts(
        size=16,
        line_color="#0f172a",
        fill_color="#d97706",
        line_width=2.5,
        tools=["hover"],
        marker="diamond",
    )

    overlays = tiles * mesh * point_glow * points
    if route is not None:
        start_marker = gv.Points(
            pd.DataFrame(
                [{"name": route.start_label or "Start", "lat": route.points[0][0], "lon": route.points[0][1], "kind": "Route start"}]
            ),
            kdims=["lon", "lat"],
            vdims=["name", "kind"],
            crs=ccrs.PlateCarree(),
        ).opts(size=13, line_color="#0f172a", fill_color="#2563eb", line_width=2, tools=["hover"])
        end_marker = gv.Points(
            pd.DataFrame(
                [{"name": route.end_label or "End", "lat": route.points[-1][0], "lon": route.points[-1][1], "kind": "Route end"}]
            ),
            kdims=["lon", "lat"],
            vdims=["name", "kind"],
            crs=ccrs.PlateCarree(),
        ).opts(size=13, line_color="#0f172a", fill_color="#f97316", line_width=2, tools=["hover"])
        route_glow = gv.Path(
            [[(lon, lat) for lat, lon in route.points]],
            crs=ccrs.PlateCarree(),
        ).opts(color="#f59e0b", line_width=12, alpha=0.14)
        route_path = gv.Path(
            [[(lon, lat) for lat, lon in route.points]],
            crs=ccrs.PlateCarree(),
        ).opts(color="#0f172a", line_width=4, alpha=0.7)
        route_core = gv.Path(
            [[(lon, lat) for lat, lon in route.points]],
            crs=ccrs.PlateCarree(),
        ).opts(color="#f8fafc", line_width=2, alpha=0.95, line_dash="dashed")
        overlays = overlays * route_glow * route_path * route_core * start_marker * end_marker

    return overlays.opts(
        toolbar="right",
        active_tools=["wheel_zoom"],
        show_legend=False,
    )


def build_timeline_plot(
    result: AnalysisResult,
    pollutant: str,
    profile_name: str,
    activity_name: str,
    timezone_label: str,
):
    meta = pollutant_meta(pollutant)
    thresholds = adjusted_thresholds(pollutant, profile_name, activity_name)
    timeline = pd.DataFrame(result.timeline_records).sort_values("time").reset_index(drop=True)
    windows = pd.DataFrame(result.window_records)
    best = windows.sort_values("score", ascending=True).iloc[0]
    timeline["step"] = np.arange(len(timeline))
    tick_stride = max(1, len(timeline) // 8)
    xticks = [(int(row.step), pd.Timestamp(row.time).strftime("%a %H:%M")) for row in timeline.iloc[::tick_stride].itertuples()]
    last_tick = (int(timeline.iloc[-1]["step"]), pd.Timestamp(timeline.iloc[-1]["time"]).strftime("%a %H:%M"))
    if last_tick not in xticks:
        xticks.append(last_tick)
    time_to_step = {pd.Timestamp(row.time): int(row.step) for row in timeline.itertuples()}
    start_step = time_to_step[pd.Timestamp(best["start"])]
    duration_hours = max(1, int(round((pd.Timestamp(best["end"]) - pd.Timestamp(best["start"])).total_seconds() / 3600.0)))
    end_step = start_step + duration_hours - 1

    curve = timeline.hvplot.line(
        x="step",
        y="value",
        line_width=3,
        color="#0f766e",
        width=860,
        height=340,
        responsive=False,
        ylabel=f"{meta['label']} ({meta['unit']})",
        xlabel=f"Forecast hour ({timezone_label})",
        title=f"{meta['label']} forecast — best {activity_name.lower()} window highlighted",
    )
    markers = timeline.hvplot.scatter(x="step", y="value", color="#0f766e", size=48, alpha=0.72)
    good_band = hv.HSpan(0, thresholds["good"]).opts(fill_color="#d7f4df", fill_alpha=0.55)
    caution_band = hv.HSpan(thresholds["good"], thresholds["caution"]).opts(
        fill_color="#fde7b2", fill_alpha=0.35
    )
    top_value = max(float(timeline["value"].max()) * 1.1, float(thresholds["caution"]) * 1.2)
    avoid_band = hv.HSpan(thresholds["caution"], top_value).opts(fill_color="#f9c8c2", fill_alpha=0.25)
    best_window = hv.VSpan(start_step - 0.45, end_step + 0.45).opts(fill_color="#d97706", fill_alpha=0.14)

    good_label = hv.Text(0.5, float(thresholds["good"]) * 0.5, "Good", fontsize=9).opts(text_color="#15803d", text_alpha=0.6)
    caution_label = hv.Text(0.5, (float(thresholds["good"]) + float(thresholds["caution"])) * 0.5, "Caution", fontsize=9).opts(text_color="#d97706", text_alpha=0.6)

    return (good_band * caution_band * avoid_band * best_window * good_label * caution_label * curve * markers).opts(
        legend_position="top_left",
        xticks=xticks,
    )


def build_route_plot(
    result: AnalysisResult,
    pollutant: str,
    profile_name: str,
    activity_name: str,
    timezone_label: str,
):
    meta = pollutant_meta(pollutant)
    thresholds = adjusted_thresholds(pollutant, profile_name, activity_name)
    route_df = pd.DataFrame(result.route_records).sort_values("departure").reset_index(drop=True)
    best_index = int(route_df["score"].astype(float).idxmin())
    best = route_df.iloc[best_index]
    route_df["step"] = np.arange(len(route_df))
    tick_stride = max(1, len(route_df) // 8)
    xticks = [
        (int(row.step), pd.Timestamp(row.departure).strftime("%a %H:%M"))
        for row in route_df.iloc[::tick_stride].itertuples()
    ]
    last_tick = (int(route_df.iloc[-1]["step"]), pd.Timestamp(route_df.iloc[-1]["departure"]).strftime("%a %H:%M"))
    if last_tick not in xticks:
        xticks.append(last_tick)

    line = route_df.hvplot.step(
        x="step",
        y="mean_value",
        where="mid",
        line_width=3,
        color="#0f172a",
        width=860,
        height=340,
        responsive=False,
        ylabel=f"Mean {meta['label']} ({meta['unit']})",
        xlabel=f"Departure hour ({timezone_label})",
        title=f"Route exposure by departure time — best departure highlighted",
    )
    points = route_df.hvplot.scatter(x="step", y="mean_value", color="#0f172a", size=54, alpha=0.78)
    good_band = hv.HSpan(0, thresholds["good"]).opts(fill_color="#d7f4df", fill_alpha=0.55)
    caution_band = hv.HSpan(thresholds["good"], thresholds["caution"]).opts(
        fill_color="#fde7b2", fill_alpha=0.35
    )
    upper = max(float(route_df["mean_value"].max()) * 1.15, float(thresholds["caution"]) * 1.2)
    avoid_band = hv.HSpan(thresholds["caution"], upper).opts(fill_color="#f9c8c2", fill_alpha=0.25)
    best_window = hv.VSpan(best_index - 0.45, best_index + 0.45).opts(fill_color="#0f766e", fill_alpha=0.14)
    return (good_band * caution_band * avoid_band * best_window * line * points).opts(
        legend_position="top_left",
        xticks=xticks,
    )


def build_scenario_matrix_plot(matrix: pd.DataFrame):
    score_map = hv.HeatMap(
        matrix,
        kdims=["activity", "profile"],
        vdims=["score", "verdict", "best_window", "headline"],
    ).opts(
        width=640,
        height=320,
        responsive=False,
        cmap=cc.CET_L17,
        clim=(0, 100),
        colorbar=True,
        colorbar_position="right",
        title="Profile × activity decision matrix",
        tools=["hover"],
        xrotation=25,
        line_color="#e2e8f0",
        toolbar="right",
    )
    labels = hv.Labels(matrix, kdims=["activity", "profile"], vdims=["verdict"]).opts(
        text_color="white",
        text_font_size="10pt",
    )
    return (score_map * labels).opts(show_legend=False)
