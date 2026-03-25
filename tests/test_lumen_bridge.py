from __future__ import annotations

from atmoslens.lumen_bridge import XarrayPipelineBridge
from atmoslens.models import AnalysisRequest
from atmoslens.recommendations import activity_pipeline_steps


def test_lumen_bridge_serializes_schema_and_transform_steps(sample_dataset):
    bridge = XarrayPipelineBridge(sample_dataset)
    request = AnalysisRequest(
        location_name="Docklands",
        profile_name="Sensitive",
        activity_name="Run",
        pollutant="pm2_5",
        advisor_mode="Morning",
    )

    schema = bridge.schema()
    query_spec = bridge.build_query_spec(request, activity_pipeline_steps(request))

    assert schema["dims"]["lat"] == 3
    assert "pm2_5" in schema["variables"]
    assert query_spec["select"]["variable"] == "pm2_5"
    assert query_spec["transforms"][0]["operation"] == "select_location"
