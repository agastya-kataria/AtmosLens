from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class LocationDefinition:
    name: str
    lat: float
    lon: float
    timezone: str = "auto"
    region_name: str = ""
    description: str = ""


@dataclass(frozen=True)
class RouteDefinition:
    name: str
    points: tuple[tuple[float, float], ...]
    duration_minutes: int = 45
    description: str = ""
    start_label: str = ""
    end_label: str = ""
    region_name: str = ""


@dataclass(frozen=True)
class TransformStep:
    operation: str
    parameters: dict[str, Any]
    target_variable: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "operation": self.operation,
            "parameters": dict(self.parameters),
            "target_variable": self.target_variable,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class AnalysisRequest:
    location_name: str
    location_lat: float
    location_lon: float
    profile_name: str
    activity_name: str
    pollutant: str
    advisor_mode: str
    time_horizon_hours: int = 24
    route_name: str | None = None
    route_points: tuple[tuple[float, float], ...] = ()
    route_duration_minutes: int = 45
    dataset_region_name: str = ""


@dataclass(frozen=True)
class Recommendation:
    verdict: str
    headline: str
    explanation: str
    best_window_label: str
    score: float
    current_value: float
    unit: str


@dataclass(frozen=True)
class AnalysisResult:
    request: AnalysisRequest
    recommendation: Recommendation
    pipeline_steps: tuple[TransformStep, ...]
    timeline_records: list[dict[str, Any]]
    window_records: list[dict[str, Any]]
    route_records: list[dict[str, Any]] = field(default_factory=list)
