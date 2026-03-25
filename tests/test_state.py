from __future__ import annotations

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
