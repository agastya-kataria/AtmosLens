from __future__ import annotations

import pandas as pd

from atmoslens.datasets import DEFAULT_ROUTES, location_series
from atmoslens.exposure import rank_route_departures
from atmoslens.models import AnalysisRequest, AnalysisResult, Recommendation, RouteDefinition, TransformStep
from atmoslens.profiles import adjusted_thresholds, get_activity, pollutant_meta
from atmoslens.scoring import current_conditions, evaluate_windows, improvement_phrase


def activity_pipeline_steps(request: AnalysisRequest) -> tuple[TransformStep, ...]:
    return (
        TransformStep(
            operation="select_location",
            parameters={
                "location_name": request.location_name,
                "lat": round(request.location_lat, 4),
                "lon": round(request.location_lon, 4),
            },
            target_variable=request.pollutant,
        ),
        TransformStep(
            operation="select_time_range",
            parameters={"advisor_mode": request.advisor_mode, "horizon_hours": request.time_horizon_hours},
            target_variable=request.pollutant,
        ),
        TransformStep(
            operation="aggregate_hourly_windows",
            parameters={"activity_name": request.activity_name},
            target_variable=request.pollutant,
        ),
        TransformStep(
            operation="score_exposure",
            parameters={"profile_name": request.profile_name},
            target_variable=request.pollutant,
        ),
        TransformStep(
            operation="recommend_activity",
            parameters={"activity_name": request.activity_name},
            target_variable=request.pollutant,
        ),
    )


def route_pipeline_steps(request: AnalysisRequest) -> tuple[TransformStep, ...]:
    return (
        TransformStep(
            operation="select_route",
            parameters={
                "route_name": request.route_name or "",
                "duration_minutes": request.route_duration_minutes,
            },
            target_variable=request.pollutant,
        ),
        TransformStep(
            operation="sample_route",
            parameters={"samples": 32, "points": [list(point) for point in request.route_points]},
            target_variable=request.pollutant,
        ),
        TransformStep(
            operation="score_exposure",
            parameters={"profile_name": request.profile_name, "activity_name": request.activity_name},
            target_variable=request.pollutant,
        ),
        TransformStep(
            operation="recommend_departure_window",
            parameters={"horizon_hours": request.time_horizon_hours},
            target_variable=request.pollutant,
        ),
    )


def build_activity_result(ds, request: AnalysisRequest) -> AnalysisResult:
    series = location_series(ds, request.location_lat, request.location_lon, request.pollutant)
    windows = evaluate_windows(
        series,
        request.pollutant,
        request.profile_name,
        request.activity_name,
        request.advisor_mode,
        horizon_hours=request.time_horizon_hours,
    )
    current = current_conditions(series, request.pollutant, request.profile_name, request.activity_name)
    best = windows.sort_values("score", ascending=True).iloc[0]
    thresholds = adjusted_thresholds(request.pollutant, request.profile_name, request.activity_name)
    pollutant = pollutant_meta(request.pollutant)
    activity = get_activity(request.activity_name)
    explanation = (
        f"{request.profile_name} thresholds rate the current {pollutant['label']} forecast at "
        f"{current['value']:.1f} {pollutant['unit']} at {request.location_name}, while the cleanest {activity.label.lower()} window "
        f"falls at {best['label']} with a lower blended exposure score. "
        f"{improvement_phrase(float(current['score']), float(best['score']))}"
    )
    recommendation = Recommendation(
        verdict=str(best["verdict"]),
        headline=f"Best time for {activity.label.lower()}: {best['label']}",
        explanation=explanation,
        best_window_label=str(best["label"]),
        score=float(best["score"]),
        current_value=float(current["value"]),
        unit=str(pollutant["unit"]),
    )
    timeline = [
        {"time": timestamp, "value": float(value)}
        for timestamp, value in series.items()
        if timestamp < series.index.min() + pd.Timedelta(hours=request.time_horizon_hours)
    ]
    return AnalysisResult(
        request=request,
        recommendation=recommendation,
        pipeline_steps=activity_pipeline_steps(request),
        timeline_records=timeline,
        window_records=windows.to_dict(orient="records"),
    )


def build_route_result(ds, request: AnalysisRequest) -> AnalysisResult:
    if len(request.route_points) < 2:
        raise ValueError("A route needs at least a start and end point.")
    preset_route = DEFAULT_ROUTES.get(request.route_name or "")
    if preset_route and tuple((round(lat, 4), round(lon, 4)) for lat, lon in preset_route.points) == request.route_points:
        route = RouteDefinition(
            name=preset_route.name,
            points=request.route_points,
            duration_minutes=request.route_duration_minutes,
            description=preset_route.description,
            start_label=preset_route.start_label,
            end_label=preset_route.end_label,
            region_name=request.dataset_region_name,
        )
    else:
        route = RouteDefinition(
            name=request.route_name or "Custom route",
            points=request.route_points,
            duration_minutes=request.route_duration_minutes,
            description=f"Custom route from {request.route_points[0]} to {request.route_points[-1]} sampled against the active forecast cube.",
            start_label=request.route_name.split(" to ")[0] if request.route_name and " to " in request.route_name else "Start",
            end_label=request.route_name.split(" to ")[-1] if request.route_name and " to " in request.route_name else "End",
            region_name=request.dataset_region_name,
        )
    departures = rank_route_departures(
        ds,
        route,
        request.pollutant,
        request.profile_name,
        request.activity_name,
        horizon_hours=request.time_horizon_hours,
    )
    best = departures.sort_values("score", ascending=True).iloc[0]
    pollutant = pollutant_meta(request.pollutant)
    recommendation = Recommendation(
        verdict=str(best["verdict"]),
        headline=f"Best departure: {best['departure']:%H:%M}",
        explanation=(
            f"{route.description} AtmosLens samples the route geometry against the gridded forecast and "
            f"finds the lowest blended {pollutant['label']} exposure for departures near {best['departure']:%H:%M}."
        ),
        best_window_label=f"{best['departure']:%H:%M}–{best['arrival']:%H:%M}",
        score=float(best["score"]),
        current_value=float(best["mean_value"]),
        unit=str(pollutant["unit"]),
    )
    return AnalysisResult(
        request=request,
        recommendation=recommendation,
        pipeline_steps=route_pipeline_steps(request),
        timeline_records=[],
        window_records=[],
        route_records=departures.to_dict(orient="records"),
    )
