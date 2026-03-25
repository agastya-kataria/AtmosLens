from __future__ import annotations

from atmoslens.models import AnalysisRequest
from atmoslens.lumen_support import build_activity_pipeline, pipeline_summary_spec
from atmoslens.recommendations import build_activity_result


def test_lumen_support_builds_pipeline_over_activity_outputs(sample_dataset):
    request = AnalysisRequest(
        location_name="Docklands",
        location_lat=53.3478,
        location_lon=-6.2374,
        profile_name="Sensitive",
        activity_name="Run",
        pollutant="pm2_5",
        advisor_mode="Next 24 hours",
    )
    result = build_activity_result(sample_dataset, request)
    pipeline = build_activity_pipeline(result)
    summary = pipeline_summary_spec(pipeline)

    assert summary["source_type"] == "InMemorySource"
    assert summary["tables"] == ["timeline", "windows"]
    assert summary["table"] == "windows"
