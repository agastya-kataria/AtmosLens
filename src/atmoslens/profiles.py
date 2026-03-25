from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HealthProfile:
    name: str
    label: str
    threshold_multiplier: float
    rationale: str


@dataclass(frozen=True)
class ActivityType:
    name: str
    label: str
    threshold_multiplier: float
    window_hours: int
    description: str


HEALTH_PROFILES: dict[str, HealthProfile] = {
    "General": HealthProfile(
        name="General",
        label="General",
        threshold_multiplier=1.0,
        rationale="Balanced thresholds for everyday outdoor decisions.",
    ),
    "Sensitive": HealthProfile(
        name="Sensitive",
        label="Sensitive",
        threshold_multiplier=0.85,
        rationale="More conservative thresholds for people who react earlier to pollution spikes.",
    ),
    "Asthma": HealthProfile(
        name="Asthma",
        label="Asthma",
        threshold_multiplier=0.75,
        rationale="Treats moderate concentrations as meaningful earlier warnings.",
    ),
    "Outdoor Worker": HealthProfile(
        name="Outdoor Worker",
        label="Outdoor Worker",
        threshold_multiplier=0.9,
        rationale="Assumes longer exposure duration across a shift.",
    ),
}

ACTIVITIES: dict[str, ActivityType] = {
    "Run": ActivityType(
        name="Run",
        label="Run",
        threshold_multiplier=0.8,
        window_hours=2,
        description="High-exertion outdoor activity.",
    ),
    "Walk": ActivityType(
        name="Walk",
        label="Walk",
        threshold_multiplier=0.95,
        window_hours=1,
        description="Low-to-moderate outdoor exposure.",
    ),
    "Ventilate": ActivityType(
        name="Ventilate",
        label="Ventilate",
        threshold_multiplier=1.1,
        window_hours=2,
        description="Short indoor-outdoor exchange where slightly higher values are still workable.",
    ),
    "Cycle Commute": ActivityType(
        name="Cycle Commute",
        label="Cycle Commute",
        threshold_multiplier=0.82,
        window_hours=1,
        description="Route-based, high-breathing-rate travel through traffic corridors.",
    ),
}

POLLUTANT_META: dict[str, dict[str, float | str]] = {
    "pm2_5": {"label": "PM2.5", "unit": "µg/m³", "good": 10.0, "caution": 25.0},
    "nitrogen_dioxide": {"label": "NO₂", "unit": "µg/m³", "good": 60.0, "caution": 100.0},
    "ozone": {"label": "Ozone", "unit": "µg/m³", "good": 80.0, "caution": 120.0},
    "european_aqi": {"label": "European AQI", "unit": "index", "good": 40.0, "caution": 60.0},
}


def get_profile(name: str) -> HealthProfile:
    return HEALTH_PROFILES[name]


def get_activity(name: str) -> ActivityType:
    return ACTIVITIES[name]


def pollutant_meta(pollutant: str) -> dict[str, float | str]:
    return POLLUTANT_META[pollutant]


def adjusted_thresholds(pollutant: str, profile_name: str, activity_name: str) -> dict[str, float | str]:
    meta = pollutant_meta(pollutant)
    profile = get_profile(profile_name)
    activity = get_activity(activity_name)
    multiplier = profile.threshold_multiplier * activity.threshold_multiplier
    return {
        "label": meta["label"],
        "unit": meta["unit"],
        "good": float(meta["good"]) * multiplier,
        "caution": float(meta["caution"]) * multiplier,
    }

