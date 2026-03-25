from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pandas as pd
import param

from atmoslens.datasets import (
    DEFAULT_DATA_PATH,
    DEFAULT_LOCATIONS,
    DEFAULT_ROUTES,
    available_pollutants,
    dataset_summary,
    load_dataset,
    map_frame,
)
from atmoslens.lumen_bridge import XarrayPipelineBridge
from atmoslens.models import AnalysisRequest
from atmoslens.recommendations import build_activity_result, build_route_result


class AtmosLensState(param.Parameterized):
    location = param.ObjectSelector(default="Docklands", objects=list(DEFAULT_LOCATIONS))
    route = param.ObjectSelector(default="Sandyford to Docklands", objects=list(DEFAULT_ROUTES))
    profile = param.ObjectSelector(default="Sensitive", objects=["General", "Sensitive", "Asthma", "Outdoor Worker"])
    activity = param.ObjectSelector(default="Run", objects=["Run", "Walk", "Ventilate", "Cycle Commute"])
    pollutant = param.ObjectSelector(default="ozone", objects=["ozone"])
    advisor_mode = param.ObjectSelector(
        default="Next 24 hours",
        objects=["Next 24 hours", "Morning", "Afternoon", "Evening", "Overnight"],
    )
    map_hour_index = param.Integer(default=0, bounds=(0, 0))

    def __init__(self, dataset=None, dataset_path: str | Path | None = None, **params):
        super().__init__(**params)
        resolved_path = Path(dataset_path or DEFAULT_DATA_PATH)
        self.dataset = dataset if dataset is not None else load_dataset(
            resolved_path,
            allow_download=not resolved_path.exists(),
        )
        pollutant_options = available_pollutants(self.dataset)
        self.param.pollutant.objects = pollutant_options
        if self.pollutant not in pollutant_options:
            self.pollutant = pollutant_options[0]
        self.param.map_hour_index.bounds = (0, len(self.available_times) - 1)

    @property
    def available_times(self) -> pd.DatetimeIndex:
        return pd.DatetimeIndex(pd.to_datetime(self.dataset.time.values))

    def current_timestamp(self) -> pd.Timestamp:
        return self.available_times[self.map_hour_index]

    def current_map_frame(self):
        return map_frame(self.dataset, self.pollutant, self.current_timestamp())

    def activity_request(self) -> AnalysisRequest:
        return AnalysisRequest(
            location_name=self.location,
            profile_name=self.profile,
            activity_name=self.activity,
            pollutant=self.pollutant,
            advisor_mode=self.advisor_mode,
            time_horizon_hours=24,
        )

    def route_request(self) -> AnalysisRequest:
        return AnalysisRequest(
            location_name=self.location,
            profile_name=self.profile,
            activity_name="Cycle Commute",
            pollutant=self.pollutant,
            advisor_mode=self.advisor_mode,
            time_horizon_hours=24,
            route_name=self.route,
        )

    @lru_cache(maxsize=128)
    def _activity_result_cached(
        self,
        location: str,
        profile: str,
        activity: str,
        pollutant: str,
        advisor_mode: str,
    ):
        request = AnalysisRequest(
            location_name=location,
            profile_name=profile,
            activity_name=activity,
            pollutant=pollutant,
            advisor_mode=advisor_mode,
            time_horizon_hours=24,
        )
        return build_activity_result(self.dataset, request)

    @lru_cache(maxsize=128)
    def _route_result_cached(
        self,
        route: str,
        profile: str,
        activity: str,
        pollutant: str,
    ):
        request = AnalysisRequest(
            location_name=self.location,
            profile_name=profile,
            activity_name=activity,
            pollutant=pollutant,
            advisor_mode=self.advisor_mode,
            time_horizon_hours=24,
            route_name=route,
        )
        return build_route_result(self.dataset, request)

    def activity_result(self):
        return self._activity_result_cached(
            self.location,
            self.profile,
            self.activity,
            self.pollutant,
            self.advisor_mode,
        )

    def route_result(self):
        return self._route_result_cached(self.route, self.profile, "Cycle Commute", self.pollutant)

    def bridge_schema(self) -> dict[str, object]:
        bridge = XarrayPipelineBridge(self.dataset)
        return bridge.schema()

    def bridge_query_spec(self) -> dict[str, object]:
        bridge = XarrayPipelineBridge(self.dataset)
        activity_result = self.activity_result()
        return bridge.build_query_spec(activity_result.request, activity_result.pipeline_steps)

    def summary(self) -> dict[str, object]:
        return dataset_summary(self.dataset)
