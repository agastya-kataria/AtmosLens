from __future__ import annotations

import pytest

from atmoslens.models import LocationDefinition
from atmoslens.state import AtmosLensState


def test_scenario_matrix_covers_all_profiles_and_non_route_activities(sample_dataset):
    state = AtmosLensState(dataset=sample_dataset)

    matrix = state.scenario_matrix()

    assert len(matrix) == 12
    assert set(matrix["activity"]) == {"Run", "Walk", "Ventilate"}
    assert set(matrix["profile"]) == {"General", "Sensitive", "Asthma", "Outdoor Worker"}
    assert float(matrix["score"].min()) <= float(matrix["score"].max())


def test_manual_location_edit_keeps_seeded_route_and_region_in_sync(sample_dataset):
    state = AtmosLensState(dataset=sample_dataset)

    state.location_name = "Custom Point"
    state.location_lat = 53.31
    state.location_lon = -6.19

    assert state.route_start_name == "Approach"
    assert state.route_end_name == "Custom Point"
    assert state.route_end_lat == 53.31
    assert state.route_end_lon == -6.19
    assert state.region_center_lat == 53.31
    assert state.region_center_lon == -6.19


def test_manual_region_geometry_switches_display_to_custom(sample_dataset):
    state = AtmosLensState(dataset=sample_dataset)

    state.region_lat_span = 0.83

    assert state.region_preset == "Custom Search"


def test_far_route_is_not_treated_as_a_valid_commute(sample_dataset):
    state = AtmosLensState(dataset=sample_dataset)

    state.route_start_name = "Phuket"
    state.route_start_lat = 7.8804
    state.route_start_lon = 98.3923
    state.route_end_name = "Tao"
    state.route_end_lat = -20.55
    state.route_end_lon = 164.81

    assert state.route_commute_ready() is False
    assert state.operational_status()["route_commute_ready"] is False


def test_failed_location_refresh_does_not_switch_active_analysis(sample_dataset, monkeypatch):
    state = AtmosLensState(dataset=sample_dataset)
    original_location = state.location_name
    original_region = state.region_name
    original_bounds = state.summary()["region_name"]
    state._location_search_results = [
        LocationDefinition("Delhi, India", 28.6139, 77.2090, "Asia/Kolkata", "India")
    ]

    def _fail(_config):
        raise ValueError("The upstream forecast service is temporarily rate-limiting requests. Wait a few seconds and try again.")

    monkeypatch.setattr(state, "_fetch_dataset_for_config", _fail)

    with pytest.raises(ValueError):
        state.load_location_search_result(0)

    assert state.location_name == original_location
    assert state.region_name == original_region
    assert state.summary()["region_name"] == original_bounds


def test_failed_route_refresh_does_not_switch_active_route(sample_dataset, monkeypatch):
    state = AtmosLensState(dataset=sample_dataset)
    original_route = state.route_name
    original_end = state.route_end_name
    original_region = state.region_name
    state._route_end_search_results = [
        LocationDefinition("Skerries", 53.5826, -6.1089, "Europe/Dublin", "Ireland")
    ]

    def _fail(_config):
        raise ValueError("The upstream forecast service is temporarily rate-limiting requests. Wait a few seconds and try again.")

    monkeypatch.setattr(state, "_fetch_dataset_for_config", _fail)

    with pytest.raises(ValueError):
        state.load_route_end_search_result(0)

    assert state.route_name == original_route
    assert state.route_end_name == original_end
    assert state.region_name == original_region


def test_operational_status_separates_selection_shift_from_commute_length(sample_dataset):
    state = AtmosLensState(dataset=sample_dataset)
    state.auto_sync_controls = False
    state.location_name = "Delhi, India"
    state.location_lat = 28.6139
    state.location_lon = 77.2090

    status = state.operational_status()

    assert status["selection_shift_km"] > 6000
    assert status["route_distance_km"] < 20


def test_current_local_time_uses_selected_timezone(sample_dataset):
    state = AtmosLensState(dataset=sample_dataset)
    state.forecast_timezone = "Asia/Kolkata"

    now = state.current_local_time()
    localized = state.localize_timestamp("2026-03-25 00:00:00")

    assert now.tzinfo is not None
    assert str(now.tzinfo) == "Asia/Kolkata"
    assert localized.hour == 0
    assert str(localized.tzinfo) == "Asia/Kolkata"
