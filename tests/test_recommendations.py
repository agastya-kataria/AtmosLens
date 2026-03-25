from __future__ import annotations

from atmoslens.models import AnalysisRequest
from atmoslens.recommendations import build_activity_result


def test_activity_result_surfaces_best_window(sample_dataset):
    request = AnalysisRequest(
        location_name="Docklands",
        location_lat=53.3478,
        location_lon=-6.2374,
        profile_name="General",
        activity_name="Run",
        pollutant="pm2_5",
        advisor_mode="Next 24 hours",
    )

    result = build_activity_result(sample_dataset, request)

    assert result.recommendation.headline.startswith("Best time for run")
    assert result.recommendation.best_window_label == "03:00–05:00"
    assert result.recommendation.verdict == "Good"
    assert len(result.pipeline_steps) == 5
