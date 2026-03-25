from __future__ import annotations

from atmoslens.state import AtmosLensState


def test_scenario_matrix_covers_all_profiles_and_non_route_activities(sample_dataset):
    state = AtmosLensState(dataset=sample_dataset)

    matrix = state.scenario_matrix()

    assert len(matrix) == 12
    assert set(matrix["activity"]) == {"Run", "Walk", "Ventilate"}
    assert set(matrix["profile"]) == {"General", "Sensitive", "Asthma", "Outdoor Worker"}
    assert float(matrix["score"].min()) <= float(matrix["score"].max())
