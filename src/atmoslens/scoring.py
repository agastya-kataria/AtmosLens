from __future__ import annotations

import math

import pandas as pd

from atmoslens.profiles import adjusted_thresholds, get_activity

DAYPARTS: dict[str, tuple[int, int]] = {
    "Morning": (5, 11),
    "Afternoon": (11, 16),
    "Evening": (16, 22),
    "Overnight": (22, 5),
}


def format_window_label(start: pd.Timestamp, end: pd.Timestamp) -> str:
    return f"{start:%H:%M}–{end:%H:%M}"


def apply_mode_filter(series: pd.Series, mode: str, horizon_hours: int = 24) -> pd.Series:
    scoped = series.sort_index()
    horizon_end = scoped.index.min() + pd.Timedelta(hours=horizon_hours)
    scoped = scoped[scoped.index < horizon_end]
    if mode in {"Next 24 hours", "Any hour in horizon"}:
        return scoped

    start_hour, end_hour = DAYPARTS[mode]
    if start_hour < end_hour:
        mask = (scoped.index.hour >= start_hour) & (scoped.index.hour < end_hour)
    else:
        mask = (scoped.index.hour >= start_hour) | (scoped.index.hour < end_hour)
    filtered = scoped[mask]
    return filtered if not filtered.empty else scoped


def score_value(value: float, pollutant: str, profile_name: str, activity_name: str) -> float:
    thresholds = adjusted_thresholds(pollutant, profile_name, activity_name)
    good = float(thresholds["good"])
    caution = float(thresholds["caution"])

    if value <= good:
        return round((value / good) * 35.0, 2) if good else 0.0
    if value <= caution:
        return round(35.0 + ((value - good) / (caution - good)) * 35.0, 2)

    span = max(caution * 0.8, 1.0)
    return round(min(100.0, 70.0 + ((value - caution) / span) * 30.0), 2)


def classify_verdict(score: float) -> str:
    if score <= 35:
        return "Good"
    if score <= 70:
        return "Caution"
    return "Avoid"


def evaluate_windows(
    series: pd.Series,
    pollutant: str,
    profile_name: str,
    activity_name: str,
    mode: str,
    *,
    horizon_hours: int = 24,
) -> pd.DataFrame:
    scoped = apply_mode_filter(series, mode, horizon_hours=horizon_hours)
    activity = get_activity(activity_name)
    window_size = max(1, min(activity.window_hours, len(scoped)))

    records: list[dict[str, object]] = []
    for start_index in range(0, len(scoped) - window_size + 1):
        window = scoped.iloc[start_index : start_index + window_size]
        mean_value = float(window.mean())
        peak_value = float(window.max())
        blended_value = 0.7 * mean_value + 0.3 * peak_value
        score = score_value(blended_value, pollutant, profile_name, activity_name)
        records.append(
            {
                "start": window.index[0],
                "end": window.index[-1] + pd.Timedelta(hours=1),
                "mean_value": mean_value,
                "peak_value": peak_value,
                "blended_value": blended_value,
                "score": score,
                "verdict": classify_verdict(score),
                "label": format_window_label(window.index[0], window.index[-1] + pd.Timedelta(hours=1)),
            }
        )

    return pd.DataFrame.from_records(records)


def current_conditions(
    series: pd.Series,
    pollutant: str,
    profile_name: str,
    activity_name: str,
) -> dict[str, object]:
    current_value = float(series.iloc[0])
    score = score_value(current_value, pollutant, profile_name, activity_name)
    return {
        "timestamp": series.index[0],
        "value": current_value,
        "score": score,
        "verdict": classify_verdict(score),
    }


def improvement_phrase(current_score: float, best_score: float) -> str:
    delta = max(0.0, current_score - best_score)
    if math.isclose(delta, 0.0, abs_tol=1.0):
        return "Conditions stay fairly flat across the next available windows."
    if delta < 10:
        return "There is a modest improvement if you wait for the cleaner slot."
    if delta < 25:
        return "Waiting materially cuts predicted exposure."
    return "The cleaner window is meaningfully better than going now."


def score_interpretation(score: float) -> str:
    """Short human-readable label for a decision score."""
    if score <= 15:
        return "Excellent"
    if score <= 35:
        return "Good"
    if score <= 55:
        return "Moderate"
    if score <= 70:
        return "Unhealthy for sensitive groups"
    if score <= 85:
        return "Unhealthy"
    return "Hazardous"


def who_guideline_note(pollutant: str) -> str:
    """Return a short WHO air quality guideline reference for context."""
    notes = {
        "pm2_5": "WHO guideline: 15 µg/m³ (24-hour mean). Levels above this raise long-term health risk.",
        "nitrogen_dioxide": "WHO guideline: 25 µg/m³ (24-hour mean). Traffic corridors often exceed this.",
        "ozone": "WHO guideline: 100 µg/m³ (8-hour mean). Peak afternoon levels frequently surpass this in warm seasons.",
        "european_aqi": "European AQI: 0–20 Good, 20–40 Fair, 40–60 Moderate, 60–80 Poor, 80–100 Very Poor, >100 Extremely Poor.",
    }
    return notes.get(pollutant, "")
