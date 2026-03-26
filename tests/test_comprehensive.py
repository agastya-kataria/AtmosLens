"""
Comprehensive test suite for AtmosLens — 100 tests covering global location
switching, cross-continent commute routing, scoring correctness, map frame
validation, timezone handling, profile/activity/pollutant combinations,
state-machine guard integrity, and edge cases.

Every consecutive location test deliberately picks a city far from the
previous one to stress the cross-continent code paths.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from atmoslens.config import MAX_COMMUTE_DISTANCE_KM
from atmoslens.datasets import coordinates_in_bounds, location_series, map_frame
from atmoslens.exposure import interpolate_route, rank_route_departures
from atmoslens.models import AnalysisRequest, LocationDefinition, RouteDefinition
from atmoslens.profiles import ACTIVITIES, HEALTH_PROFILES, POLLUTANT_META, adjusted_thresholds, health_guidance
from atmoslens.recommendations import build_activity_result, build_route_result
from atmoslens.scoring import (
    classify_verdict,
    current_conditions,
    evaluate_windows,
    improvement_phrase,
    score_interpretation,
    score_value,
    who_guideline_note,
)
from atmoslens.state import AtmosLensState


# ─── Helpers ──────────────────────────────────────────────────────────


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Reference haversine distance — independent of AtmosLens."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _build_ds_for(center_lat: float, center_lon: float, lat_span: float = 0.5, lon_span: float = 0.6) -> xr.Dataset:
    """Build a small synthetic forecast cube centered on the given point."""
    times = pd.date_range("2026-03-25 00:00", periods=24, freq="h")
    lats = np.linspace(center_lat - lat_span / 2, center_lat + lat_span / 2, 5)
    lons = np.linspace(center_lon - lon_span / 2, center_lon + lon_span / 2, 5)
    rng = np.random.RandomState(42)

    base = rng.uniform(5, 35, (24, 5, 5))
    return xr.Dataset(
        data_vars={
            "pm2_5": (("time", "lat", "lon"), base),
            "nitrogen_dioxide": (("time", "lat", "lon"), base * 1.8 + 20),
            "ozone": (("time", "lat", "lon"), 60 + base * 0.9),
            "european_aqi": (("time", "lat", "lon"), np.clip(base * 2.2, 0, None)),
        },
        coords={"time": times, "lat": lats, "lon": lons},
        attrs={"region_name": f"Synthetic ({center_lat:.2f}, {center_lon:.2f})"},
    )


# ─── World cities for global hopping (each far from the previous) ────

WORLD_CITIES = [
    # (name, lat, lon, timezone, country)
    ("Dublin, Ireland", 53.3498, -6.2603, "Europe/Dublin", "Ireland"),
    ("Sydney, Australia", -33.8688, 151.2093, "Australia/Sydney", "Australia"),
    ("São Paulo, Brazil", -23.5505, -46.6333, "America/Sao_Paulo", "Brazil"),
    ("Tokyo, Japan", 35.6762, 139.6503, "Asia/Tokyo", "Japan"),
    ("Nairobi, Kenya", -1.2921, 36.8219, "Africa/Nairobi", "Kenya"),
    ("New York, USA", 40.7128, -74.0060, "America/New_York", "United States"),
    ("Mumbai, India", 19.0760, 72.8777, "Asia/Kolkata", "India"),
    ("Reykjavik, Iceland", 64.1466, -21.9426, "Atlantic/Reykjavik", "Iceland"),
    ("Buenos Aires, Argentina", -34.6037, -58.3816, "America/Argentina/Buenos_Aires", "Argentina"),
    ("Beijing, China", 39.9042, 116.4074, "Asia/Shanghai", "China"),
    ("Cape Town, South Africa", -33.9249, 18.4241, "Africa/Johannesburg", "South Africa"),
    ("London, UK", 51.5074, -0.1278, "Europe/London", "United Kingdom"),
    ("Singapore", 1.3521, 103.8198, "Asia/Singapore", "Singapore"),
    ("Mexico City, Mexico", 19.4326, -99.1332, "America/Mexico_City", "Mexico"),
    ("Moscow, Russia", 55.7558, 37.6173, "Europe/Moscow", "Russia"),
    ("Auckland, New Zealand", -36.8485, 174.7633, "Pacific/Auckland", "New Zealand"),
    ("Cairo, Egypt", 30.0444, 31.2357, "Africa/Cairo", "Egypt"),
    ("Los Angeles, USA", 34.0522, -118.2437, "America/Los_Angeles", "United States"),
    ("Bangkok, Thailand", 13.7563, 100.5018, "Asia/Bangkok", "Thailand"),
    ("Lima, Peru", -12.0464, -77.0428, "America/Lima", "Peru"),
    ("Berlin, Germany", 52.5200, 13.4050, "Europe/Berlin", "Germany"),
    ("Jakarta, Indonesia", -6.2088, 106.8456, "Asia/Jakarta", "Indonesia"),
    ("Toronto, Canada", 43.6532, -79.3832, "America/Toronto", "Canada"),
    ("Delhi, India", 28.6139, 77.2090, "Asia/Kolkata", "India"),
    ("Santiago, Chile", -33.4489, -70.6693, "America/Santiago", "Chile"),
    ("Seoul, South Korea", 37.5665, 126.9780, "Asia/Seoul", "South Korea"),
    ("Lagos, Nigeria", 6.5244, 3.3792, "Africa/Lagos", "Nigeria"),
    ("Dubai, UAE", 25.2048, 55.2708, "Asia/Dubai", "United Arab Emirates"),
    ("Anchorage, USA", 61.2181, -149.9003, "America/Anchorage", "United States"),
    ("Rome, Italy", 41.9028, 12.4964, "Europe/Rome", "Italy"),
]


# ═════════════════════════════════════════════════════════════════════
# GROUP 1: Global location switching — each city recenters the state
#           and the route seed is always local (30 tests)
# ═════════════════════════════════════════════════════════════════════


class TestGlobalLocationSwitching:
    """Switch to 30 cities around the world. Each test verifies that the
    state stays internally consistent after apply_location_search_result."""

    @pytest.fixture()
    def state(self, sample_dataset):
        return AtmosLensState(dataset=sample_dataset)

    @pytest.mark.parametrize(
        "name,lat,lon,tz,country",
        WORLD_CITIES,
        ids=[c[0].split(",")[0] for c in WORLD_CITIES],
    )
    def test_location_switch(self, state, name, lat, lon, tz, country):
        loc = LocationDefinition(name, lat, lon, tz, country)
        state._location_search_results = [loc]
        state.apply_location_search_result(0)

        # Location updated
        assert state.location_name == name
        assert abs(state.location_lat - lat) < 0.01
        assert abs(state.location_lon - lon) < 0.01

        # Route end is near location (seeded locally)
        assert abs(state.route_end_lat - lat) < 0.5
        assert abs(state.route_end_lon - lon) < 0.5

        # Route is short (seeded as local approach)
        route_km = state.route_distance_km()
        assert route_km < 30, f"Seeded route should be local, got {route_km:.1f} km"

        # Region is recentered
        assert abs(state.region_center_lat - lat) < 0.5
        assert abs(state.region_center_lon - lon) < 0.5

        # Timezone was set
        assert state.forecast_timezone == tz


# ═════════════════════════════════════════════════════════════════════
# GROUP 2: Cross-continent route — start far from end, auto-reset
#           verified (10 tests)
# ═════════════════════════════════════════════════════════════════════


CROSS_CONTINENT_ROUTES = [
    # (start_city, start_lat, start_lon, end_city, end_lat, end_lon)
    ("Dublin", 53.3498, -6.2603, "Delhi", 28.6139, 77.2090),
    ("Tokyo", 35.6762, 139.6503, "New York", 40.7128, -74.0060),
    ("Sydney", -33.8688, 151.2093, "London", 51.5074, -0.1278),
    ("Beijing", 39.9042, 116.4074, "Buenos Aires", -34.6037, -58.3816),
    ("Nairobi", -1.2921, 36.8219, "Auckland", -36.8485, 174.7633),
    ("Moscow", 55.7558, 37.6173, "São Paulo", -23.5505, -46.6333),
    ("Cairo", 30.0444, 31.2357, "Los Angeles", 34.0522, -118.2437),
    ("Singapore", 1.3521, 103.8198, "Toronto", 43.6532, -79.3832),
    ("Santiago", -33.4489, -70.6693, "Seoul", 37.5665, 126.9780),
    ("Anchorage", 61.2181, -149.9003, "Cape Town", -33.9249, 18.4241),
]


class TestCrossContinentRouteStart:
    """When a route start is set and the existing end is on another continent,
    the end should auto-reset to a local seed."""

    @pytest.fixture()
    def state(self, sample_dataset):
        return AtmosLensState(dataset=sample_dataset)

    @pytest.mark.parametrize(
        "start_name,start_lat,start_lon,end_name,end_lat,end_lon",
        CROSS_CONTINENT_ROUTES,
        ids=[f"{r[0]}_to_{r[3]}" for r in CROSS_CONTINENT_ROUTES],
    )
    def test_auto_reset_far_route_end(self, state, start_name, start_lat, start_lon, end_name, end_lat, end_lon):
        # Set end to the far city
        state._ignore_route_sync_watch = True
        state.route_end_name = end_name
        state.route_end_lat = end_lat
        state.route_end_lon = end_lon
        state._ignore_route_sync_watch = False

        # Apply route start
        state._route_start_search_results = [
            LocationDefinition(start_name, start_lat, start_lon, "auto", "")
        ]
        state.apply_route_start_search_result(0)

        # Route start updated
        assert state.route_start_name == start_name

        # End was auto-reset close to start (not still the far city)
        assert state.route_end_name != end_name
        reset_dist = _haversine_km(state.route_start_lat, state.route_start_lon, state.route_end_lat, state.route_end_lon)
        assert reset_dist < 30, f"Reset end should be near start, got {reset_dist:.0f} km"
        assert state.route_commute_ready() is True


# ═════════════════════════════════════════════════════════════════════
# GROUP 3: Local commute routes — both endpoints within city,
#           verifying route_result works end-to-end (6 tests)
# ═════════════════════════════════════════════════════════════════════


LOCAL_COMMUTE_SCENARIOS = [
    # (city, lat, lon, offset_lat, offset_lon, tz)
    ("Dublin: Sandyford → Docklands", 53.2749, -6.2256, 0.07, 0.01, "Europe/Dublin"),
    ("Tokyo: Shibuya → Shinjuku", 35.6580, 139.7016, 0.02, -0.03, "Asia/Tokyo"),
    ("New York: Brooklyn → Manhattan", 40.6782, -73.9442, 0.04, 0.02, "America/New_York"),
    ("Sydney: Bondi → CBD", -33.8914, 151.2744, 0.03, -0.04, "Australia/Sydney"),
    ("Mumbai: Andheri → Bandra", 19.1136, 72.8697, 0.02, 0.01, "Asia/Kolkata"),
    ("Berlin: Kreuzberg → Mitte", 52.4894, 13.4024, 0.03, 0.02, "Europe/Berlin"),
]


class TestLocalCommuteRoutes:
    """Short commute corridors — end-to-end route result computed correctly."""

    @pytest.mark.parametrize(
        "label,lat,lon,dlat,dlon,tz",
        LOCAL_COMMUTE_SCENARIOS,
        ids=[s[0].split(":")[0] for s in LOCAL_COMMUTE_SCENARIOS],
    )
    def test_local_route_result(self, label, lat, lon, dlat, dlon, tz):
        ds = _build_ds_for(lat + dlat / 2, lon + dlon / 2, lat_span=0.4, lon_span=0.5)
        state = AtmosLensState(dataset=ds)
        state._ignore_location_sync_watch = True
        state._ignore_route_sync_watch = True
        state._ignore_region_geometry_watch = True
        try:
            state.location_name = label
            state.location_lat = lat + dlat
            state.location_lon = lon + dlon
            state.route_start_name = "Start"
            state.route_start_lat = lat
            state.route_start_lon = lon
            state.route_end_name = "End"
            state.route_end_lat = lat + dlat
            state.route_end_lon = lon + dlon
            state.route_name = f"Start to End"
            state.forecast_timezone = tz
        finally:
            state._ignore_location_sync_watch = False
            state._ignore_route_sync_watch = False
            state._ignore_region_geometry_watch = False

        assert state.route_commute_ready() is True
        result = state.route_result()
        rec = result.recommendation
        assert rec.verdict in {"Good", "Caution", "Avoid"}
        assert 0 <= rec.score <= 100
        assert rec.best_window_label  # not empty
        assert rec.unit  # not empty
        assert len(result.route_records) > 0


# ═════════════════════════════════════════════════════════════════════
# GROUP 4: Scoring correctness — verify score_value against manual
#           threshold math for every profile × activity × pollutant (16 tests)
# ═════════════════════════════════════════════════════════════════════


class TestScoringCorrectness:
    """Independently compute expected scores and compare to score_value."""

    @pytest.mark.parametrize("pollutant", list(POLLUTANT_META.keys()))
    @pytest.mark.parametrize("profile", list(HEALTH_PROFILES.keys()))
    def test_zero_value_gives_zero_score(self, pollutant, profile):
        score = score_value(0.0, pollutant, profile, "Walk")
        assert score == 0.0

    @pytest.mark.parametrize("pollutant", list(POLLUTANT_META.keys()))
    @pytest.mark.parametrize("profile", list(HEALTH_PROFILES.keys()))
    def test_good_threshold_gives_score_35(self, pollutant, profile):
        thresholds = adjusted_thresholds(pollutant, profile, "Walk")
        good = float(thresholds["good"])
        score = score_value(good, pollutant, profile, "Walk")
        assert abs(score - 35.0) < 0.1, f"At good threshold score should be ~35, got {score}"

    @pytest.mark.parametrize("pollutant", list(POLLUTANT_META.keys()))
    @pytest.mark.parametrize("profile", list(HEALTH_PROFILES.keys()))
    def test_caution_threshold_gives_score_70(self, pollutant, profile):
        thresholds = adjusted_thresholds(pollutant, profile, "Walk")
        caution = float(thresholds["caution"])
        score = score_value(caution, pollutant, profile, "Walk")
        assert abs(score - 70.0) < 0.1, f"At caution threshold score should be ~70, got {score}"

    @pytest.mark.parametrize("pollutant", list(POLLUTANT_META.keys()))
    def test_score_monotonically_increases(self, pollutant):
        """Higher pollution values should never produce a lower score."""
        values = [0, 5, 10, 20, 40, 80, 120, 200]
        scores = [score_value(v, pollutant, "General", "Run") for v in values]
        for i in range(len(scores) - 1):
            assert scores[i] <= scores[i + 1] + 0.01, (
                f"Score decreased: {scores[i]} > {scores[i+1]} at values {values[i]}, {values[i+1]}"
            )


# ═════════════════════════════════════════════════════════════════════
# GROUP 5: Verdict classification correctness (3 tests)
# ═════════════════════════════════════════════════════════════════════


class TestVerdictClassification:
    def test_good_verdict_range(self):
        for score in [0, 5, 10, 20, 30, 35]:
            assert classify_verdict(score) == "Good"

    def test_caution_verdict_range(self):
        for score in [36, 40, 50, 60, 70]:
            assert classify_verdict(score) == "Caution"

    def test_avoid_verdict_range(self):
        for score in [71, 80, 90, 100]:
            assert classify_verdict(score) == "Avoid"


# ═════════════════════════════════════════════════════════════════════
# GROUP 6: Score interpretation labels (6 tests)
# ═════════════════════════════════════════════════════════════════════


class TestScoreInterpretation:
    def test_excellent(self):
        assert score_interpretation(10) == "Excellent"

    def test_good(self):
        assert score_interpretation(25) == "Good"

    def test_moderate(self):
        assert score_interpretation(45) == "Moderate"

    def test_unhealthy_for_sensitive(self):
        assert score_interpretation(65) == "Unhealthy for sensitive groups"

    def test_unhealthy(self):
        assert score_interpretation(80) == "Unhealthy"

    def test_hazardous(self):
        assert score_interpretation(90) == "Hazardous"


# ═════════════════════════════════════════════════════════════════════
# GROUP 7: WHO guideline notes — all pollutants have references (4 tests)
# ═════════════════════════════════════════════════════════════════════


class TestWHOGuidelines:
    @pytest.mark.parametrize("pollutant", list(POLLUTANT_META.keys()))
    def test_who_note_not_empty(self, pollutant):
        note = who_guideline_note(pollutant)
        assert len(note) > 10

    def test_pm25_who_value(self):
        assert "15" in who_guideline_note("pm2_5")

    def test_no2_who_value(self):
        assert "25" in who_guideline_note("nitrogen_dioxide")

    def test_ozone_who_value(self):
        assert "100" in who_guideline_note("ozone")


# ═════════════════════════════════════════════════════════════════════
# GROUP 8: Health guidance — all score tiers produce non-empty guidance (6 tests)
# ═════════════════════════════════════════════════════════════════════


class TestHealthGuidance:
    @pytest.mark.parametrize("score,expected_fragment", [
        (10, "excellent"),
        (30, "good"),
        (45, "moderate"),
        (65, "elevated"),
        (80, "unhealthy"),
        (95, "hazardous"),
    ])
    def test_guidance_tier(self, score, expected_fragment):
        text = health_guidance(score, "Run", pollutant_label="PM2.5")
        assert expected_fragment in text.lower()
        assert len(text) > 20


# ═════════════════════════════════════════════════════════════════════
# GROUP 9: Improvement phrase correctness (4 tests)
# ═════════════════════════════════════════════════════════════════════


class TestImprovementPhrase:
    def test_flat_conditions(self):
        assert "flat" in improvement_phrase(50, 50).lower()

    def test_modest_improvement(self):
        phrase = improvement_phrase(50, 43)
        assert "modest" in phrase.lower()

    def test_material_improvement(self):
        phrase = improvement_phrase(60, 40)
        assert "material" in phrase.lower()

    def test_meaningful_improvement(self):
        phrase = improvement_phrase(90, 40)
        assert "meaningful" in phrase.lower() or "better" in phrase.lower()


# ═════════════════════════════════════════════════════════════════════
# GROUP 10: Adjusted thresholds — profile multipliers applied correctly (4 tests)
# ═════════════════════════════════════════════════════════════════════


class TestAdjustedThresholds:
    def test_asthma_more_conservative_than_general(self):
        general = adjusted_thresholds("ozone", "General", "Run")
        asthma = adjusted_thresholds("ozone", "Asthma", "Run")
        assert float(asthma["good"]) < float(general["good"])
        assert float(asthma["caution"]) < float(general["caution"])

    def test_ventilate_more_lenient_than_run(self):
        run = adjusted_thresholds("pm2_5", "General", "Run")
        vent = adjusted_thresholds("pm2_5", "General", "Ventilate")
        assert float(vent["good"]) > float(run["good"])

    def test_children_most_conservative_activity(self):
        children = adjusted_thresholds("pm2_5", "General", "Children's Play")
        walk = adjusted_thresholds("pm2_5", "General", "Walk")
        assert float(children["good"]) < float(walk["good"])

    def test_threshold_product_rule(self):
        """good_threshold = base * profile_multiplier * activity_multiplier."""
        for pollutant in POLLUTANT_META:
            for profile_name, profile in HEALTH_PROFILES.items():
                for activity_name, activity in ACTIVITIES.items():
                    base_good = float(POLLUTANT_META[pollutant]["good"])
                    expected = base_good * profile.threshold_multiplier * activity.threshold_multiplier
                    actual = float(adjusted_thresholds(pollutant, profile_name, activity_name)["good"])
                    assert abs(actual - expected) < 0.01, (
                        f"Threshold mismatch for {pollutant}/{profile_name}/{activity_name}: expected {expected}, got {actual}"
                    )


# ═════════════════════════════════════════════════════════════════════
# GROUP 11: Map frame validation (5 tests)
# ═════════════════════════════════════════════════════════════════════


class TestMapFrame:
    def test_map_frame_shape_matches_grid(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        frame = state.current_map_frame()
        assert frame.shape == (3, 3)  # 3 lats × 3 lons in the fixture

    def test_map_frame_no_nan(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        frame = state.current_map_frame()
        assert not np.isnan(frame.values).any()

    def test_map_frame_coordinates_match_dataset(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        frame = state.current_map_frame()
        np.testing.assert_array_almost_equal(frame.lat.values, sample_dataset.lat.values)
        np.testing.assert_array_almost_equal(frame.lon.values, sample_dataset.lon.values)

    def test_map_frame_values_positive(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        for pollutant in ["pm2_5", "ozone", "nitrogen_dioxide", "european_aqi"]:
            state.pollutant = pollutant
            frame = state.current_map_frame()
            # Ozone and NO2 should always be positive; PM2.5 and AQI might have near-zero but not large negative
            assert float(frame.min()) >= -5.0, f"{pollutant} has large negative: {float(frame.min())}"

    def test_map_frame_changes_with_hour(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        frame0 = state.current_map_frame()
        state.map_hour_index = min(5, len(state.available_times) - 1)
        frame5 = state.current_map_frame()
        # Different hours should generally produce different frames (unless data is contrived)
        assert not np.array_equal(frame0.values, frame5.values), "Different hours should give different map frames"


# ═════════════════════════════════════════════════════════════════════
# GROUP 12: Timeline / window evaluation correctness (3 tests)
# ═════════════════════════════════════════════════════════════════════


class TestTimelineEvaluation:
    def test_windows_cover_horizon(self, sample_dataset):
        series = location_series(sample_dataset, 53.35, -6.25, "ozone")
        windows = evaluate_windows(series, "ozone", "General", "Walk", "Any hour in horizon", horizon_hours=12)
        assert len(windows) > 0
        assert "score" in windows.columns
        assert "verdict" in windows.columns

    def test_best_window_has_lowest_score(self, sample_dataset):
        series = location_series(sample_dataset, 53.35, -6.25, "pm2_5")
        windows = evaluate_windows(series, "pm2_5", "Sensitive", "Run", "Any hour in horizon", horizon_hours=12)
        best = windows.sort_values("score").iloc[0]
        assert float(best["score"]) == float(windows["score"].min())

    def test_all_windows_have_valid_verdicts(self, sample_dataset):
        series = location_series(sample_dataset, 53.35, -6.25, "ozone")
        windows = evaluate_windows(series, "ozone", "General", "Walk", "Any hour in horizon")
        assert set(windows["verdict"]).issubset({"Good", "Caution", "Avoid"})


# ═════════════════════════════════════════════════════════════════════
# GROUP 13: Activity result end-to-end — every activity type (7 tests)
# ═════════════════════════════════════════════════════════════════════


class TestActivityResultAllActivities:
    @pytest.mark.parametrize("activity", list(ACTIVITIES.keys()))
    def test_activity_result(self, sample_dataset, activity):
        request = AnalysisRequest(
            location_name="Test Point",
            location_lat=53.35,
            location_lon=-6.25,
            profile_name="General",
            activity_name=activity,
            pollutant="ozone",
            advisor_mode="Any hour in horizon",
            time_horizon_hours=12,
            dataset_region_name="Dublin commuter belt",
        )
        result = build_activity_result(sample_dataset, request)
        rec = result.recommendation
        assert rec.verdict in {"Good", "Caution", "Avoid"}
        assert 0 <= rec.score <= 100
        assert rec.unit == "µg/m³"
        assert rec.best_window_label
        assert rec.health_guidance
        assert len(result.timeline_records) > 0
        assert len(result.window_records) > 0


# ═════════════════════════════════════════════════════════════════════
# GROUP 14: Route interpolation correctness (3 tests)
# ═════════════════════════════════════════════════════════════════════


class TestRouteInterpolation:
    def test_interpolated_points_start_and_end_correct(self):
        route = RouteDefinition(
            name="Test",
            points=((53.27, -6.23), (53.35, -6.24)),
            duration_minutes=35,
        )
        lats, lons, fracs = interpolate_route(route, samples=32)
        assert abs(lats[0] - 53.27) < 0.001
        assert abs(lons[0] - (-6.23)) < 0.001
        assert abs(lats[-1] - 53.35) < 0.001
        assert abs(lons[-1] - (-6.24)) < 0.001

    def test_fractions_span_zero_to_one(self):
        route = RouteDefinition(
            name="Test",
            points=((53.27, -6.23), (53.35, -6.24)),
            duration_minutes=35,
        )
        _, _, fracs = interpolate_route(route, samples=16)
        assert abs(fracs[0]) < 1e-9
        assert abs(fracs[-1] - 1.0) < 1e-9
        assert np.all(np.diff(fracs) > 0)

    def test_correct_sample_count(self):
        route = RouteDefinition(
            name="Test",
            points=((10.0, 20.0), (10.1, 20.1)),
            duration_minutes=20,
        )
        lats, lons, fracs = interpolate_route(route, samples=64)
        assert len(lats) == 64
        assert len(lons) == 64
        assert len(fracs) == 64


# ═════════════════════════════════════════════════════════════════════
# GROUP 15: Scenario matrix completeness (3 tests)
# ═════════════════════════════════════════════════════════════════════


class TestScenarioMatrix:
    def test_matrix_row_count(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        matrix = state.scenario_matrix()
        expected = len(HEALTH_PROFILES) * (len(ACTIVITIES) - 1)  # excludes Cycle Commute
        assert len(matrix) == expected

    def test_all_scores_bounded(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        matrix = state.scenario_matrix()
        assert (matrix["score"] >= 0).all()
        assert (matrix["score"] <= 100).all()

    def test_matrix_has_expected_columns(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        matrix = state.scenario_matrix()
        for col in ["profile", "activity", "verdict", "best_window", "score", "current_value", "unit", "headline"]:
            assert col in matrix.columns


# ═════════════════════════════════════════════════════════════════════
# GROUP 16: Timezone handling — every continent (8 tests)
# ═════════════════════════════════════════════════════════════════════


TIMEZONE_CITIES = [
    ("Europe/Dublin", "Dublin", 53.35, -6.25),
    ("Asia/Kolkata", "Delhi", 28.61, 77.21),
    ("America/New_York", "New York", 40.71, -74.01),
    ("Australia/Sydney", "Sydney", -33.87, 151.21),
    ("Africa/Nairobi", "Nairobi", -1.29, 36.82),
    ("Asia/Tokyo", "Tokyo", 35.68, 139.65),
    ("America/Sao_Paulo", "São Paulo", -23.55, -46.63),
    ("Pacific/Auckland", "Auckland", -36.85, 174.76),
]


class TestTimezoneHandling:
    @pytest.mark.parametrize("tz,name,lat,lon", TIMEZONE_CITIES, ids=[t[0] for t in TIMEZONE_CITIES])
    def test_timezone_applied_correctly(self, sample_dataset, tz, name, lat, lon):
        state = AtmosLensState(dataset=sample_dataset)
        state.forecast_timezone = tz

        now = state.current_local_time()
        assert now.tzinfo is not None
        assert str(now.tzinfo) == tz

        localized = state.localize_timestamp("2026-03-25 12:00:00")
        assert localized.tzinfo is not None
        assert str(localized.tzinfo) == tz


# ═════════════════════════════════════════════════════════════════════
# GROUP 17: Timezone inference from search results (5 tests)
# ═════════════════════════════════════════════════════════════════════


class TestTimezoneInference:
    @pytest.fixture()
    def state(self, sample_dataset):
        return AtmosLensState(dataset=sample_dataset)

    def test_primary_timezone_preferred(self, state):
        results = [LocationDefinition("A", 0, 0, "Asia/Kolkata")]
        tz = state._best_timezone_from_search_results(results, results[0])
        assert tz == "Asia/Kolkata"

    def test_auto_timezone_falls_through_to_secondary(self, state):
        primary = LocationDefinition("A", 0, 0, "auto")
        secondary = LocationDefinition("B", 0, 0, "Europe/Berlin")
        results = [primary, secondary]
        tz = state._best_timezone_from_search_results(results, primary)
        assert tz == "Europe/Berlin"

    def test_all_auto_falls_to_current_timezone(self, state):
        state.forecast_timezone = "America/Chicago"
        primary = LocationDefinition("A", 0, 0, "auto")
        results = [primary]
        tz = state._best_timezone_from_search_results(results, primary)
        assert tz == "America/Chicago"

    def test_empty_timezone_treated_as_auto(self, state):
        primary = LocationDefinition("A", 53.35, -6.25, "")
        results = [primary]
        tz = state._best_timezone_from_search_results(results, primary)
        # Should fall through since empty is falsy — gets current forecast_timezone
        assert tz in {"Europe/Dublin", "UTC"}

    def test_matched_region_preset_provides_timezone(self, state):
        # Dublin is in the region preset — a point near Dublin with auto tz
        # should pick up the preset timezone
        primary = LocationDefinition("Near Dublin", 53.35, -6.25, "auto")
        results = [primary]
        tz = state._best_timezone_from_search_results(results, primary)
        assert tz == "Europe/Dublin"


# ═════════════════════════════════════════════════════════════════════
# GROUP 18: State guard flags — no cascade when guards are active (4 tests)
# ═════════════════════════════════════════════════════════════════════


class TestStateGuardFlags:
    @pytest.fixture()
    def state(self, sample_dataset):
        return AtmosLensState(dataset=sample_dataset)

    def test_location_change_with_guard_does_not_reseed_route(self, state):
        original_route_start = state.route_start_name
        state._ignore_location_sync_watch = True
        state.location_name = "Guarded Change"
        state.location_lat = 0.0
        state.location_lon = 0.0
        state._ignore_location_sync_watch = False
        # Route should be unchanged because location sync was guarded
        assert state.route_start_name == original_route_start

    def test_route_change_with_guard_does_not_update_location(self, state):
        original_location = state.location_name
        state._ignore_route_sync_watch = True
        state.route_end_name = "Guarded Route End"
        state.route_end_lat = 0.0
        state.route_end_lon = 0.0
        state._ignore_route_sync_watch = False
        assert state.location_name == original_location

    def test_region_geometry_change_with_guard_does_not_switch_preset(self, state):
        original_preset = state.region_preset
        state._ignore_region_geometry_watch = True
        state.region_lat_span = 5.0
        state._ignore_region_geometry_watch = False
        assert state.region_preset == original_preset

    def test_auto_sync_off_prevents_cascade(self, state):
        state.auto_sync_controls = False
        original_route_end = state.route_end_name
        original_region = state.region_center_lat
        state.location_name = "Manual Point"
        state.location_lat = 0.0
        state.location_lon = 0.0
        # Route and region should be untouched
        assert state.route_end_name == original_route_end
        assert state.region_center_lat == original_region


# ═════════════════════════════════════════════════════════════════════
# GROUP 19: Operational status in various scenarios (5 tests)
# ═════════════════════════════════════════════════════════════════════


class TestOperationalStatus:
    def test_default_state_is_ready(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        status = state.operational_status()
        assert status["ready"] is True
        assert status["cube_matches_target"] is True
        assert status["location_ready"] is True
        assert status["route_ready"] is True
        assert status["route_commute_ready"] is True

    def test_location_outside_cube_not_ready(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        state.auto_sync_controls = False
        state._ignore_location_sync_watch = True
        state.location_lat = -33.87
        state.location_lon = 151.21
        state._ignore_location_sync_watch = False
        status = state.operational_status()
        assert status["location_ready"] is False

    def test_enormous_route_not_commute_ready(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        state._ignore_route_sync_watch = True
        state.route_start_lat = 53.35
        state.route_start_lon = -6.25
        state.route_end_lat = -33.87
        state.route_end_lon = 151.21
        state._ignore_route_sync_watch = False
        status = state.operational_status()
        assert status["route_commute_ready"] is False
        assert status["route_distance_km"] > 10000

    def test_selection_shift_measured_correctly(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        state.auto_sync_controls = False
        state._ignore_location_sync_watch = True
        state.location_lat = 28.61
        state.location_lon = 77.21
        state._ignore_location_sync_watch = False
        status = state.operational_status()
        expected_km = _haversine_km(
            float(sample_dataset.lat.mean()), float(sample_dataset.lon.mean()),
            28.61, 77.21,
        )
        # Flat-earth approximation diverges significantly from haversine at 7000+ km
        assert abs(status["selection_shift_km"] - expected_km) / expected_km < 0.15  # within 15%

    def test_busy_state_reflected(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        state.set_busy("Loading…")
        assert state.busy is True
        assert state.busy_message == "Loading…"
        status = state.operational_status()
        assert status["busy"] is True
        state.clear_busy()
        assert state.busy is False


# ═════════════════════════════════════════════════════════════════════
# GROUP 20: Bridge schema and query spec validation (3 tests)
# ═════════════════════════════════════════════════════════════════════


class TestBridge:
    def test_bridge_schema_has_dimensions(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        schema = state.bridge_schema()
        assert "dims" in schema
        assert "time" in schema["dims"]
        assert "lat" in schema["dims"]
        assert "lon" in schema["dims"]

    def test_bridge_schema_has_variables(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        schema = state.bridge_schema()
        assert "variables" in schema
        assert "ozone" in schema["variables"]

    def test_bridge_query_spec_has_transforms(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        spec = state.bridge_query_spec()
        assert "transforms" in spec
        assert len(spec["transforms"]) > 0


# ═════════════════════════════════════════════════════════════════════
# GROUP 21: Edge cases — extreme coordinates (6 tests)
# ═════════════════════════════════════════════════════════════════════


class TestEdgeCaseCoordinates:
    @pytest.fixture()
    def state(self, sample_dataset):
        return AtmosLensState(dataset=sample_dataset)

    def test_near_north_pole(self, state):
        loc = LocationDefinition("Svalbard", 78.22, 15.64, "Arctic/Longyearbyen", "Norway")
        state._location_search_results = [loc]
        state.apply_location_search_result(0)
        assert state.location_lat == 78.22
        assert state.route_distance_km() < 30

    def test_near_south_pole(self, state):
        loc = LocationDefinition("McMurdo Station", -77.85, 166.67, "Antarctica/McMurdo", "Antarctica")
        state._location_search_results = [loc]
        state.apply_location_search_result(0)
        assert state.location_lat == -77.85
        assert state.route_distance_km() < 30

    def test_equator(self, state):
        loc = LocationDefinition("Quito, Ecuador", -0.1807, -78.4678, "America/Guayaquil", "Ecuador")
        state._location_search_results = [loc]
        state.apply_location_search_result(0)
        assert abs(state.location_lat) < 0.2
        assert state.route_distance_km() < 30

    def test_prime_meridian(self, state):
        loc = LocationDefinition("Accra, Ghana", 5.6037, -0.1870, "Africa/Accra", "Ghana")
        state._location_search_results = [loc]
        state.apply_location_search_result(0)
        assert abs(state.location_lon) < 0.2
        assert state.route_distance_km() < 30

    def test_near_antimeridian_east(self, state):
        loc = LocationDefinition("Suva, Fiji", -18.1416, 178.4419, "Pacific/Fiji", "Fiji")
        state._location_search_results = [loc]
        state.apply_location_search_result(0)
        assert state.location_lon > 178
        assert state.route_distance_km() < 30

    def test_near_antimeridian_west(self, state):
        loc = LocationDefinition("Apia, Samoa", -13.8333, -171.7500, "Pacific/Apia", "Samoa")
        state._location_search_results = [loc]
        state.apply_location_search_result(0)
        assert state.location_lon < -171
        assert state.route_distance_km() < 30


# ═════════════════════════════════════════════════════════════════════
# GROUP 22: Distance calculation correctness (4 tests)
# ═════════════════════════════════════════════════════════════════════


class TestDistanceCalculation:
    @pytest.fixture()
    def state(self, sample_dataset):
        return AtmosLensState(dataset=sample_dataset)

    def test_same_point_distance_is_zero(self, state):
        dist = state._distance_km(53.35, -6.25, 53.35, -6.25)
        assert dist < 0.01

    def test_dublin_to_delhi_rough_distance(self, state):
        dist = state._distance_km(53.35, -6.26, 28.61, 77.21)
        haversine = _haversine_km(53.35, -6.26, 28.61, 77.21)
        # Flat-earth approximation should be within 30% of haversine at these distances
        assert abs(dist - haversine) / haversine < 0.35

    def test_dublin_to_sandyford_short_distance(self, state):
        dist = state._distance_km(53.35, -6.24, 53.27, -6.23)
        assert 5 < dist < 15  # roughly 8.5 km

    def test_equator_to_equator_longitude_distance(self, state):
        """At the equator, 1 degree longitude ≈ 111 km."""
        dist = state._distance_km(0.0, 0.0, 0.0, 1.0)
        assert 100 < dist < 120


# ═════════════════════════════════════════════════════════════════════
# GROUP 23: Activity result value correctness — validate output values
#           against independently computed reference (2 tests)
# ═════════════════════════════════════════════════════════════════════


class TestActivityResultValues:
    def test_current_value_matches_series_interpolation(self, sample_dataset):
        """The recommendation's current_value should match the first value from location_series."""
        state = AtmosLensState(dataset=sample_dataset)
        result = state.activity_result()
        series = location_series(sample_dataset, state.location_lat, state.location_lon, state.pollutant)
        expected_current = round(float(series.iloc[0]), 1)
        assert abs(result.recommendation.current_value - expected_current) < 0.2

    def test_current_conditions_score_consistent_with_activity_result(self, sample_dataset):
        """current_conditions score for the first hour should be consistent."""
        state = AtmosLensState(dataset=sample_dataset)
        series = location_series(sample_dataset, state.location_lat, state.location_lon, state.pollutant)
        cc = current_conditions(series, state.pollutant, state.profile, state.activity)
        result = state.activity_result()
        # The activity result score is for the *best* window, not current conditions
        # But current conditions score should be valid
        assert 0 <= cc["score"] <= 100
        assert cc["verdict"] in {"Good", "Caution", "Avoid"}


# ═════════════════════════════════════════════════════════════════════
# GROUP 24: Recommendation fields are all populated (2 tests)
# ═════════════════════════════════════════════════════════════════════


class TestRecommendationCompleteness:
    def test_activity_recommendation_all_fields(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        rec = state.activity_result().recommendation
        assert rec.verdict
        assert rec.headline
        assert rec.explanation
        assert rec.best_window_label
        assert 0 <= rec.score <= 100
        assert rec.current_value >= 0
        assert rec.unit
        assert rec.who_guideline
        assert rec.score_label
        assert rec.health_guidance

    def test_route_recommendation_all_fields(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        rec = state.route_result().recommendation
        assert rec.verdict
        assert rec.headline
        assert rec.explanation
        assert rec.best_window_label
        assert 0 <= rec.score <= 100
        assert rec.current_value >= 0
        assert rec.unit
        assert rec.who_guideline
        assert rec.score_label
        assert rec.health_guidance


# ═════════════════════════════════════════════════════════════════════
# GROUP 25: Profile × pollutant full matrix validation (4 tests)
# ═════════════════════════════════════════════════════════════════════


class TestProfilePollutantMatrix:
    @pytest.mark.parametrize("pollutant", list(POLLUTANT_META.keys()))
    def test_all_profiles_produce_valid_results(self, sample_dataset, pollutant):
        for profile in HEALTH_PROFILES:
            request = AnalysisRequest(
                location_name="Test",
                location_lat=53.35,
                location_lon=-6.25,
                profile_name=profile,
                activity_name="Walk",
                pollutant=pollutant,
                advisor_mode="Any hour in horizon",
                time_horizon_hours=12,
                dataset_region_name="Dublin commuter belt",
            )
            result = build_activity_result(sample_dataset, request)
            assert result.recommendation.verdict in {"Good", "Caution", "Avoid"}
            assert result.recommendation.unit == str(POLLUTANT_META[pollutant]["unit"])


# ═════════════════════════════════════════════════════════════════════
# GROUP 26: Route ranking validation (2 tests)
# ═════════════════════════════════════════════════════════════════════


class TestRouteRanking:
    def test_route_departures_all_have_valid_verdicts(self, sample_dataset):
        route = RouteDefinition(
            name="Test route",
            points=((53.27, -6.23), (53.35, -6.24)),
            duration_minutes=35,
        )
        departures = rank_route_departures(sample_dataset, route, "ozone", "General", "Walk", horizon_hours=12)
        assert len(departures) > 0
        assert set(departures["verdict"]).issubset({"Good", "Caution", "Avoid"})
        assert (departures["score"] >= 0).all()
        assert (departures["score"] <= 100).all()

    def test_best_departure_is_lowest_score(self, sample_dataset):
        route = RouteDefinition(
            name="Test route",
            points=((53.27, -6.23), (53.35, -6.24)),
            duration_minutes=35,
        )
        departures = rank_route_departures(sample_dataset, route, "pm2_5", "Sensitive", "Cycle Commute", horizon_hours=12)
        best = departures.sort_values("score").iloc[0]
        assert float(best["score"]) == float(departures["score"].min())


# ═════════════════════════════════════════════════════════════════════
# GROUP 27: Data summary validation (2 tests)
# ═════════════════════════════════════════════════════════════════════


class TestDataSummary:
    def test_summary_keys(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        summary = state.summary()
        for key in ["region_name", "time_start", "time_end", "lat_min", "lat_max", "lon_min", "lon_max", "dims", "pollutants"]:
            assert key in summary, f"Missing key: {key}"

    def test_summary_values_match_dataset(self, sample_dataset):
        state = AtmosLensState(dataset=sample_dataset)
        summary = state.summary()
        assert abs(float(summary["lat_min"]) - float(sample_dataset.lat.min())) < 0.01
        assert abs(float(summary["lat_max"]) - float(sample_dataset.lat.max())) < 0.01
        assert abs(float(summary["lon_min"]) - float(sample_dataset.lon.min())) < 0.01
        assert abs(float(summary["lon_max"]) - float(sample_dataset.lon.max())) < 0.01


# ═════════════════════════════════════════════════════════════════════
# GROUP 28: Failed-fetch rollback (2 tests — location + route)
# ═════════════════════════════════════════════════════════════════════


class TestFetchRollback:
    def test_failed_location_fetch_preserves_all_state(self, sample_dataset, monkeypatch):
        state = AtmosLensState(dataset=sample_dataset)
        original = {
            "location_name": state.location_name,
            "location_lat": state.location_lat,
            "location_lon": state.location_lon,
            "region_name": state.region_name,
            "dataset_revision": state.dataset_revision,
        }
        state._location_search_results = [
            LocationDefinition("Tokyo, Japan", 35.68, 139.65, "Asia/Tokyo", "Japan")
        ]
        monkeypatch.setattr(state, "_fetch_dataset_for_config", lambda _: (_ for _ in ()).throw(ValueError("rate limited")))
        with pytest.raises(ValueError, match="rate limited"):
            state.load_location_search_result(0)
        assert state.location_name == original["location_name"]
        assert state.location_lat == original["location_lat"]
        assert state.region_name == original["region_name"]
        assert state.dataset_revision == original["dataset_revision"]

    def test_failed_route_end_fetch_preserves_all_state(self, sample_dataset, monkeypatch):
        state = AtmosLensState(dataset=sample_dataset)
        original = {
            "route_end_name": state.route_end_name,
            "route_end_lat": state.route_end_lat,
            "region_name": state.region_name,
            "dataset_revision": state.dataset_revision,
        }
        state._route_end_search_results = [
            LocationDefinition("Skerries", 53.58, -6.11, "Europe/Dublin", "Ireland")
        ]
        monkeypatch.setattr(state, "_fetch_dataset_for_config", lambda _: (_ for _ in ()).throw(ValueError("timeout")))
        with pytest.raises(ValueError, match="timeout"):
            state.load_route_end_search_result(0)
        assert state.route_end_name == original["route_end_name"]
        assert state.route_end_lat == original["route_end_lat"]
        assert state.region_name == original["region_name"]
        assert state.dataset_revision == original["dataset_revision"]


# ═════════════════════════════════════════════════════════════════════
# GROUP 29: Coordinates-in-bounds checks (3 tests)
# ═════════════════════════════════════════════════════════════════════


class TestCoordinatesInBounds:
    def test_center_is_in_bounds(self, sample_dataset):
        assert coordinates_in_bounds(sample_dataset, 53.35, -6.25) is True

    def test_far_point_is_out_of_bounds(self, sample_dataset):
        assert coordinates_in_bounds(sample_dataset, 0.0, 0.0) is False

    def test_boundary_point_is_in_bounds(self, sample_dataset):
        lat_min = float(sample_dataset.lat.min())
        lon_min = float(sample_dataset.lon.min())
        assert coordinates_in_bounds(sample_dataset, lat_min, lon_min) is True


# ═════════════════════════════════════════════════════════════════════
# GROUP 30: Rapid sequential location switches — state remains consistent (1 test)
# ═════════════════════════════════════════════════════════════════════


class TestRapidLocationSwitching:
    def test_rapid_sequential_switches_stay_consistent(self, sample_dataset):
        """Simulate a user rapidly switching between 10 cities — state should
        always end consistent no matter the order."""
        state = AtmosLensState(dataset=sample_dataset)
        cities = WORLD_CITIES[:10]
        for name, lat, lon, tz, country in cities:
            loc = LocationDefinition(name, lat, lon, tz, country)
            state._location_search_results = [loc]
            state.apply_location_search_result(0)

        # After all switches, state should reflect the last city
        last = cities[-1]
        assert state.location_name == last[0]
        assert abs(state.location_lat - last[1]) < 0.01
        assert abs(state.location_lon - last[2]) < 0.01
        assert state.forecast_timezone == last[3]
        assert state.route_commute_ready() is True
        assert state.route_distance_km() < 30
