from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pandas as pd
import param

from atmoslens.datasets import (
    DEFAULT_DATA_PATH,
    LIVE_DATA_PATH,
    LOCATION_PRESETS,
    REGION_PRESETS,
    ROUTE_PRESETS,
    available_pollutants,
    build_route_from_endpoints,
    dataset_summary,
    load_dataset,
    location_label,
    location_presets_for_region,
    map_frame,
    region_from_center,
    route_presets_for_region,
    search_places,
    fetch_open_meteo_grid,
)
from atmoslens.lumen_bridge import XarrayPipelineBridge
from atmoslens.models import AnalysisRequest, LocationDefinition
from atmoslens.recommendations import build_activity_result, build_route_result


def _region_center(config):
    return (
        round((config.lat_min + config.lat_max) / 2.0, 4),
        round((config.lon_min + config.lon_max) / 2.0, 4),
        round(config.lat_max - config.lat_min, 4),
        round(config.lon_max - config.lon_min, 4),
    )


class AtmosLensState(param.Parameterized):
    region_preset = param.ObjectSelector(default="Dublin Metro", objects=list(REGION_PRESETS))
    region_name = param.String(default="Dublin Metro")
    region_center_lat = param.Number(default=53.3498, bounds=(-89.5, 89.5))
    region_center_lon = param.Number(default=-6.2603, bounds=(-179.5, 179.5))
    region_lat_span = param.Number(default=0.42, bounds=(0.15, 20.0))
    region_lon_span = param.Number(default=0.55, bounds=(0.15, 20.0))
    forecast_grid_lat = param.Integer(default=15, bounds=(7, 31))
    forecast_grid_lon = param.Integer(default=17, bounds=(7, 31))
    forecast_domain = param.ObjectSelector(default="cams_europe", objects=["auto", "cams_europe", "cams_global"])
    forecast_timezone = param.String(default="Europe/Dublin")

    location_preset = param.ObjectSelector(default="Dublin Docklands", objects=list(LOCATION_PRESETS))
    location_name = param.String(default="Docklands")
    location_lat = param.Number(default=53.3478, bounds=(-90.0, 90.0))
    location_lon = param.Number(default=-6.2374, bounds=(-180.0, 180.0))

    route_preset = param.ObjectSelector(default="Sandyford to Docklands", objects=list(ROUTE_PRESETS))
    route_name = param.String(default="Sandyford to Docklands")
    route_start_name = param.String(default="Sandyford")
    route_start_lat = param.Number(default=53.2749, bounds=(-90.0, 90.0))
    route_start_lon = param.Number(default=-6.2256, bounds=(-180.0, 180.0))
    route_end_name = param.String(default="Docklands")
    route_end_lat = param.Number(default=53.3478, bounds=(-90.0, 90.0))
    route_end_lon = param.Number(default=-6.2374, bounds=(-180.0, 180.0))
    route_duration_minutes = param.Integer(default=40, bounds=(10, 180))

    profile = param.ObjectSelector(default="Sensitive", objects=["General", "Sensitive", "Asthma", "Outdoor Worker"])
    activity = param.ObjectSelector(default="Run", objects=["Run", "Walk", "Ventilate", "Cycle Commute"])
    pollutant = param.ObjectSelector(default="ozone", objects=["ozone"])
    advisor_mode = param.ObjectSelector(
        default="Any hour in horizon",
        objects=["Any hour in horizon", "Morning", "Afternoon", "Evening", "Overnight"],
    )
    horizon_hours = param.ObjectSelector(default=24, objects=[12, 24, 36, 48])
    map_hour_index = param.Integer(default=0, bounds=(0, 0))

    dataset_revision = param.Integer(default=0)
    status_message = param.String(default="Loaded bundled Dublin sample forecast.")

    def __init__(self, dataset=None, dataset_path: str | Path | None = None, **params):
        super().__init__(**params)
        self.dataset_path = Path(dataset_path or DEFAULT_DATA_PATH)
        self.live_dataset_path = LIVE_DATA_PATH
        self._location_search_results: list[LocationDefinition] = []
        self._route_start_search_results: list[LocationDefinition] = []
        self._route_end_search_results: list[LocationDefinition] = []
        self._wire_watchers()
        self._sync_region_controls_from_preset()
        self._sync_location_from_preset()
        self._sync_route_from_preset()
        self.dataset = dataset if dataset is not None else load_dataset(
            self.dataset_path,
            allow_download=not self.dataset_path.exists(),
        )
        self._after_dataset_update("Loaded bundled Dublin sample forecast.")

    def _wire_watchers(self) -> None:
        self.param.watch(self._on_region_preset_changed, "region_preset")
        self.param.watch(self._on_location_preset_changed, "location_preset")
        self.param.watch(self._on_route_preset_changed, "route_preset")

    def _after_dataset_update(self, message: str) -> None:
        pollutant_options = available_pollutants(self.dataset)
        self.param.pollutant.objects = pollutant_options
        if self.pollutant not in pollutant_options:
            self.pollutant = "ozone" if "ozone" in pollutant_options else pollutant_options[0]
        self.param.map_hour_index.bounds = (0, len(self.available_times) - 1)
        self.map_hour_index = min(self.map_hour_index, len(self.available_times) - 1)
        self.dataset_revision += 1
        self.status_message = message
        self._activity_result_cached.cache_clear()
        self._route_result_cached.cache_clear()

    def _sync_region_controls_from_preset(self) -> None:
        config = REGION_PRESETS[self.region_preset]
        center_lat, center_lon, lat_span, lon_span = _region_center(config)
        self.region_name = config.name
        self.region_center_lat = center_lat
        self.region_center_lon = center_lon
        self.region_lat_span = lat_span
        self.region_lon_span = lon_span
        self.forecast_grid_lat = config.n_lat
        self.forecast_grid_lon = config.n_lon
        self.forecast_domain = config.domains
        self.forecast_timezone = config.timezone

    def _sync_location_from_preset(self) -> None:
        preset = LOCATION_PRESETS[self.location_preset]
        self.location_name = preset.name
        self.location_lat = preset.lat
        self.location_lon = preset.lon

    def _sync_route_from_preset(self) -> None:
        preset = ROUTE_PRESETS[self.route_preset]
        self.route_name = preset.name
        self.route_start_name = preset.start_label or "Start"
        self.route_end_name = preset.end_label or "End"
        self.route_start_lat = preset.points[0][0]
        self.route_start_lon = preset.points[0][1]
        self.route_end_lat = preset.points[-1][0]
        self.route_end_lon = preset.points[-1][1]
        self.route_duration_minutes = preset.duration_minutes

    def _on_region_preset_changed(self, event) -> None:
        self._sync_region_controls_from_preset()
        region_locations = location_presets_for_region(self.region_preset)
        if region_locations:
            self.location_preset = region_locations[0]
        region_routes = route_presets_for_region(self.region_preset)
        if region_routes:
            self.route_preset = region_routes[0]

    def _on_location_preset_changed(self, event) -> None:
        self._sync_location_from_preset()

    def _on_route_preset_changed(self, event) -> None:
        self._sync_route_from_preset()

    @property
    def available_times(self) -> pd.DatetimeIndex:
        return pd.DatetimeIndex(pd.to_datetime(self.dataset.time.values))

    def current_timestamp(self) -> pd.Timestamp:
        return self.available_times[self.map_hour_index]

    def current_map_frame(self):
        return map_frame(self.dataset, self.pollutant, self.current_timestamp())

    def current_location(self) -> LocationDefinition:
        return LocationDefinition(
            self.location_name,
            round(self.location_lat, 4),
            round(self.location_lon, 4),
            self.forecast_timezone,
            self.region_name,
        )

    def current_route(self):
        preset = ROUTE_PRESETS.get(self.route_preset)
        preset_matches = (
            preset is not None
            and self.route_name == preset.name
            and round(self.route_start_lat, 4) == round(preset.points[0][0], 4)
            and round(self.route_start_lon, 4) == round(preset.points[0][1], 4)
            and round(self.route_end_lat, 4) == round(preset.points[-1][0], 4)
            and round(self.route_end_lon, 4) == round(preset.points[-1][1], 4)
            and self.route_duration_minutes == preset.duration_minutes
        )
        if preset_matches:
            return preset
        return build_route_from_endpoints(
            self.route_name,
            self.route_start_name,
            (round(self.route_start_lat, 4), round(self.route_start_lon, 4)),
            self.route_end_name,
            (round(self.route_end_lat, 4), round(self.route_end_lon, 4)),
            duration_minutes=self.route_duration_minutes,
            region_name=self.region_name,
        )

    def region_config(self):
        return region_from_center(
            self.region_name or self.region_preset,
            self.region_center_lat,
            self.region_center_lon,
            lat_span=self.region_lat_span,
            lon_span=self.region_lon_span,
            n_lat=self.forecast_grid_lat,
            n_lon=self.forecast_grid_lon,
            forecast_hours=max(self.horizon_hours, 48),
            timezone=self.forecast_timezone or "auto",
            domains=self.forecast_domain,
        )

    def refresh_dataset(self) -> None:
        config = self.region_config()
        ds = fetch_open_meteo_grid(config=config, output_path=self.live_dataset_path)
        self.dataset = ds
        self._after_dataset_update(
            f"Loaded live forecast cube for {config.name} centred on {config.lat_min + (config.lat_max - config.lat_min) / 2:.2f}, "
            f"{config.lon_min + (config.lon_max - config.lon_min) / 2:.2f}."
        )

    def fit_region_to_points(
        self,
        points: list[tuple[float, float]],
        *,
        label: str,
        min_lat_span: float = 0.35,
        min_lon_span: float = 0.5,
    ) -> None:
        if not points:
            return
        latitudes = [lat for lat, _ in points]
        longitudes = [lon for _, lon in points]
        lat_min, lat_max = min(latitudes), max(latitudes)
        lon_min, lon_max = min(longitudes), max(longitudes)
        lat_span = max(min_lat_span, (lat_max - lat_min) * 1.8 or min_lat_span)
        lon_span = max(min_lon_span, (lon_max - lon_min) * 1.8 or min_lon_span)
        self.region_name = label
        self.region_center_lat = round((lat_min + lat_max) / 2.0, 4)
        self.region_center_lon = round((lon_min + lon_max) / 2.0, 4)
        self.region_lat_span = round(lat_span, 4)
        self.region_lon_span = round(lon_span, 4)

    def location_search_labels(self) -> list[str]:
        return [location_label(location) for location in self._location_search_results]

    def route_start_search_labels(self) -> list[str]:
        return [location_label(location) for location in self._route_start_search_results]

    def route_end_search_labels(self) -> list[str]:
        return [location_label(location) for location in self._route_end_search_results]

    def search_location(self, query: str) -> list[str]:
        self._location_search_results = search_places(query)
        self.apply_location_search_result(0)
        return self.location_search_labels()

    def search_route_start(self, query: str) -> list[str]:
        self._route_start_search_results = search_places(query)
        self.apply_route_start_search_result(0)
        return self.route_start_search_labels()

    def search_route_end(self, query: str) -> list[str]:
        self._route_end_search_results = search_places(query)
        self.apply_route_end_search_result(0)
        return self.route_end_search_labels()

    def apply_location_search_result(self, index: int) -> None:
        location = self._location_search_results[index]
        self.location_name = location.name
        self.location_lat = location.lat
        self.location_lon = location.lon
        self.forecast_domain = "auto"
        if location.timezone and location.timezone != "auto":
            self.forecast_timezone = location.timezone
        self.fit_region_to_points([(location.lat, location.lon)], label=f"{location.name} search region")
        self._seed_route_from_location()
        self.status_message = (
            f"Resolved {location.name}. Region recentered around the searched place and the commute route was reset to a local corridor."
        )

    def apply_route_start_search_result(self, index: int) -> None:
        location = self._route_start_search_results[index]
        self.route_start_name = location.name
        self.route_start_lat = location.lat
        self.route_start_lon = location.lon
        self._refresh_route_name()
        self._autofit_region_from_route(focus="start", timezone_hint=location.timezone)
        self.status_message = (
            f"Resolved route start as {location.name}. Region was focused on the start side of the current route."
        )

    def apply_route_end_search_result(self, index: int) -> None:
        location = self._route_end_search_results[index]
        self.route_end_name = location.name
        self.route_end_lat = location.lat
        self.route_end_lon = location.lon
        self._refresh_route_name()
        self._autofit_region_from_route(focus="end", timezone_hint=location.timezone)
        synced_location = self._sync_location_to_route_end_if_remote()
        location_suffix = f" Decision point moved to {self.route_end_name} for consistency." if synced_location else ""
        self.status_message = (
            f"Resolved route end as {location.name}. Region was auto-fit to the current route corridor.{location_suffix}"
        )

    def _refresh_route_name(self) -> None:
        self.route_name = f"{self.route_start_name} to {self.route_end_name}"

    def _seed_route_from_location(self) -> None:
        lat_offset = max(0.045, min(0.11, self.region_lat_span * 0.18))
        lon_offset = max(0.06, min(0.14, self.region_lon_span * 0.18))
        start_lat = self.location_lat - lat_offset if self.location_lat - lat_offset >= -89.5 else self.location_lat + lat_offset
        start_lon = self.location_lon - lon_offset if self.location_lon - lon_offset >= -179.5 else self.location_lon + lon_offset
        self.route_start_name = "Approach"
        self.route_start_lat = round(start_lat, 4)
        self.route_start_lon = round(start_lon, 4)
        self.route_end_name = self.location_name
        self.route_end_lat = self.location_lat
        self.route_end_lon = self.location_lon
        self.route_name = f"Approach to {self.location_name}"
        self.route_duration_minutes = 35

    def _route_points_are_local(self, *, max_lat_gap: float = 8.0, max_lon_gap: float = 8.0) -> bool:
        return (
            abs(self.route_start_lat - self.route_end_lat) <= max_lat_gap
            and abs(self.route_start_lon - self.route_end_lon) <= max_lon_gap
        )

    def _sync_location_to_route_end_if_remote(self) -> bool:
        if not self._route_points_are_local():
            return False
        latitudes = [self.route_start_lat, self.route_end_lat]
        longitudes = [self.route_start_lon, self.route_end_lon]
        lat_pad = max(0.25, abs(latitudes[0] - latitudes[1]) * 1.25)
        lon_pad = max(0.35, abs(longitudes[0] - longitudes[1]) * 1.25)
        if (
            min(latitudes) - lat_pad <= self.location_lat <= max(latitudes) + lat_pad
            and min(longitudes) - lon_pad <= self.location_lon <= max(longitudes) + lon_pad
        ):
            return False
        self.location_name = self.route_end_name
        self.location_lat = self.route_end_lat
        self.location_lon = self.route_end_lon
        return True

    def _autofit_region_from_route(self, *, focus: str, timezone_hint: str | None = None) -> None:
        route_points = [(self.route_start_lat, self.route_start_lon), (self.route_end_lat, self.route_end_lon)]
        if self._route_points_are_local():
            points = route_points
        else:
            points = [route_points[0] if focus == "start" else route_points[-1]]
        self.fit_region_to_points(points, label=f"{self.route_name or self.route_start_name} corridor")
        self.forecast_domain = "auto"
        if timezone_hint and timezone_hint != "auto":
            self.forecast_timezone = timezone_hint

    def activity_request(self) -> AnalysisRequest:
        location = self.current_location()
        return AnalysisRequest(
            location_name=location.name,
            location_lat=location.lat,
            location_lon=location.lon,
            profile_name=self.profile,
            activity_name=self.activity,
            pollutant=self.pollutant,
            advisor_mode=self.advisor_mode,
            time_horizon_hours=self.horizon_hours,
            dataset_region_name=self.summary()["region_name"],
        )

    def route_request(self) -> AnalysisRequest:
        location = self.current_location()
        route = self.current_route()
        return AnalysisRequest(
            location_name=location.name,
            location_lat=location.lat,
            location_lon=location.lon,
            profile_name=self.profile,
            activity_name="Cycle Commute",
            pollutant=self.pollutant,
            advisor_mode=self.advisor_mode,
            time_horizon_hours=self.horizon_hours,
            route_name=route.name,
            route_points=tuple((round(lat, 4), round(lon, 4)) for lat, lon in route.points),
            route_duration_minutes=route.duration_minutes,
            dataset_region_name=self.summary()["region_name"],
        )

    @lru_cache(maxsize=256)
    def _activity_result_cached(
        self,
        location_name: str,
        location_lat: float,
        location_lon: float,
        profile: str,
        activity: str,
        pollutant: str,
        advisor_mode: str,
        horizon_hours: int,
        dataset_revision: int,
    ):
        request = AnalysisRequest(
            location_name=location_name,
            location_lat=location_lat,
            location_lon=location_lon,
            profile_name=profile,
            activity_name=activity,
            pollutant=pollutant,
            advisor_mode=advisor_mode,
            time_horizon_hours=horizon_hours,
            dataset_region_name=self.summary()["region_name"],
        )
        return build_activity_result(self.dataset, request)

    @lru_cache(maxsize=256)
    def _route_result_cached(
        self,
        route_name: str,
        route_points: tuple[tuple[float, float], ...],
        route_duration_minutes: int,
        profile: str,
        pollutant: str,
        horizon_hours: int,
        dataset_revision: int,
    ):
        location = self.current_location()
        request = AnalysisRequest(
            location_name=location.name,
            location_lat=location.lat,
            location_lon=location.lon,
            profile_name=profile,
            activity_name="Cycle Commute",
            pollutant=pollutant,
            advisor_mode=self.advisor_mode,
            time_horizon_hours=horizon_hours,
            route_name=route_name,
            route_points=route_points,
            route_duration_minutes=route_duration_minutes,
            dataset_region_name=self.summary()["region_name"],
        )
        return build_route_result(self.dataset, request)

    def activity_result(self):
        location = self.current_location()
        return self._activity_result_cached(
            location.name,
            location.lat,
            location.lon,
            self.profile,
            self.activity,
            self.pollutant,
            self.advisor_mode,
            int(self.horizon_hours),
            int(self.dataset_revision),
        )

    def route_result(self):
        route = self.current_route()
        route_points = tuple((round(lat, 4), round(lon, 4)) for lat, lon in route.points)
        return self._route_result_cached(
            route.name,
            route_points,
            route.duration_minutes,
            self.profile,
            self.pollutant,
            int(self.horizon_hours),
            int(self.dataset_revision),
        )

    def bridge_schema(self) -> dict[str, object]:
        bridge = XarrayPipelineBridge(self.dataset)
        return bridge.schema()

    def bridge_query_spec(self) -> dict[str, object]:
        bridge = XarrayPipelineBridge(self.dataset)
        activity_result = self.activity_result()
        return bridge.build_query_spec(activity_result.request, activity_result.pipeline_steps)

    def summary(self) -> dict[str, object]:
        return dataset_summary(self.dataset)
